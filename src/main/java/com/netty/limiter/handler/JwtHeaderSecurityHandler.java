package com.netty.limiter.handler;

import com.netty.limiter.cache.LocalBanCache;
import com.netty.limiter.util.SecurityAttributeKeys;
import com.netty.limiter.util.jwt.ZeroGcJwtParser;
import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;

/**
 * @description: 针对 JWT Header 的 0-GC 极速解析与 UID 黑名单校验 Handler (无多余 Dispatcher 中转)
 **/
@Slf4j
@Component
public class JwtHeaderSecurityHandler {

    public static final int MAX_HEADER_SIZE = 16384; // 16KB 适配长 Cookie / 全链路 TraceId / 复杂 JWT 权限令牌

    public boolean isHeaderCompleteOrJwtFound(ByteBuf buf) {
        return isHeaderCompleteOrJwtFound(buf, null);
    }

    public boolean isHeaderCompleteOrJwtFound(ByteBuf buf, ChannelHandlerContext ctx) {
        int readableBytes = buf.readableBytes();
        if (readableBytes == 0) {
            return false;
        }

        int readerIndex = buf.readerIndex();
        int limit = Math.min(buf.writerIndex(), readerIndex + MAX_HEADER_SIZE);

        int lineStart = readerIndex;
        while (lineStart < limit) {
            if (isHeaderEnd(buf, lineStart, limit)) {
                // Header 完全收齐 (\r\n\r\n)，但未找到有效 JWT (匿名请求)：将 readerIndex 直接拉到 writerIndex
                buf.readerIndex(buf.writerIndex());
                return true;
            }

            byte firstByte = buf.getByte(lineStart);
            // 根据首字母快筛选：若首字母非 'a'/'A' (authorization), 't'/'T' (token), 'u'/'U' (userid)，
            // 则该行绝不可能是 JWT Header！直接跨过该行推进 readerIndex
            if (!isCandidateJwtFirstByte(firstByte)) {
                int nextLineStart = advancePastNonCandidateLine(buf, lineStart, limit);
                if (isIncompleteLine(nextLineStart)) {
                    return false;
                }
                lineStart = nextLineStart;
                continue;
            }

            int lineEnd = findLineEnd(buf, lineStart, limit);
            if (lineEnd == limit) {
                return markIncomplete(buf, lineStart);
            }

            int colonPos = findColon(buf, lineStart, lineEnd);
            if (colonPos != -1) {
                long keyRange = trimRange(buf, lineStart, colonPos);
                int keyStart = getTrimmedStart(keyRange);
                int keyLen = getTrimmedEnd(keyRange) - keyStart;

                if (isJwtHeader(buf, keyStart, keyLen)) {
                    // 找到了 JWT Header 行，将 readerIndex 停留在该 JWT 行开头，直接返回 true
                    buf.readerIndex(lineStart);
                    return true;
                }
            }

            // 当前行已完整收齐（\r\n 结尾）但不是有效 JWT 行，推进 readerIndex 越过该行，避免下次重复扫描！
            lineStart = advanceReaderIndexToNextLine(buf, lineEnd);
        }

        if (buf.readableBytes() == 0 || buf.writerIndex() >= MAX_HEADER_SIZE) {
            buf.readerIndex(buf.writerIndex());
            return true;
        }
        return false;
    }

    public static final LocalBanCache.BanInfo INVALID_JWT_BAN = new LocalBanCache.BanInfo("Invalid or Expired JWT Token", Long.MAX_VALUE);

    public LocalBanCache.BanInfo authenticateJwtAndCheckBan(ByteBuf buf, ChannelHandlerContext ctx, LocalBanCache localBanCache) {
        Long cachedUserId = ctx != null ? ctx.channel().attr(SecurityAttributeKeys.USER_ID).get() : null;
        if (cachedUserId != null && cachedUserId > 0) {
            return localBanCache.getUserBanInfo(cachedUserId);
        }

        int lineStart = buf.readerIndex();
        int limit = Math.min(buf.writerIndex(), lineStart + MAX_HEADER_SIZE);

        int lineEnd = findLineEnd(buf, lineStart, limit);
        if (lineEnd == limit) {
            return null;
        }

        int colonPos = findColon(buf, lineStart, lineEnd);
        if (colonPos != -1) {
            long valRange = trimRange(buf, colonPos + 1, lineEnd);
            int valStart = getTrimmedStart(valRange);
            int valLen = getTrimmedEnd(valRange) - valStart;

            long userId = ZeroGcJwtParser.INSTANCE.authenticateJwtAndExtractUid(buf, valStart, valLen);
            if (userId > 0) {
                if (ctx != null) {
                    ctx.channel().attr(SecurityAttributeKeys.USER_ID).set(userId);
                }
                return localBanCache.getUserBanInfo(userId);
            } else {
                // JWT 校验失败 (返回 0L，代表非法签名/畸形/已过期)，直接返回 INVALID_JWT_BAN 哨兵对象触发拦截！
                return INVALID_JWT_BAN;
            }
        }

        return null;
    }

    private static boolean isCandidateJwtFirstByte(byte b) {
        byte lower = (byte) (b | 0x20);
        return lower == 'a' || lower == 't' || lower == 'u';
    }

    private static boolean isHeaderEnd(ByteBuf buf, int lineStart, int limit) {
        byte firstByte = buf.getByte(lineStart);
        if (firstByte == '\r') {
            return lineStart + 1 < limit && buf.getByte(lineStart + 1) == '\n';
        }
        return firstByte == '\n';
    }

    /**
     * 0-GC 堆外物理区间空白剥离：收缩 [start, end) 边界，剔除前导和尾随 Space/Tab
     * @return 高 32 位为 trimmedStart，低 32 位为 trimmedEnd
     */
    private static long trimRange(ByteBuf buf, int start, int end) {
        while (start < end && isWhitespace(buf.getByte(start))) {
            start++;
        }
        while (end > start && isWhitespace(buf.getByte(end - 1))) {
            end--;
        }
        return ((long) start << 32) | (end & 0xFFFFFFFFL);
    }

    private static int getTrimmedStart(long range) {
        return (int) (range >>> 32);
    }

    private static int getTrimmedEnd(long range) {
        return (int) range;
    }

    private static boolean isWhitespace(byte b) {
        return b == ' ' || b == '\t';
    }

    private boolean isJwtHeader(ByteBuf buf, int start, int len) {
        switch (len) {
            case 13:
                return equalsAuthorizationInt(buf, start);
            case 5:
                return equalsTokenInt(buf, start);
            case 6:
                return equalsUserIdInt(buf, start);
            default:
                return false;
        }
    }

    // SWAR 位运算掩码与预编译长字/整型常量
    private static final long  SWAR_LOWERCASE_MASK_64 = 0x2020202020202020L;
    private static final int   SWAR_LOWERCASE_MASK_32 = 0x20202020;
    private static final short SWAR_LOWERCASE_MASK_16 = 0x2020;

    private static final long  AUTHORIZATION_LONG_8  = 0x617574686f72697aL; // "authoriz" (Big-Endian 64-bit)
    private static final int   AUTHORIZATION_INT_4   = 0x6174696f;         // "atio"     (Big-Endian 32-bit)
    private static final int   TOKEN_INT_4           = 0x746f6b65;         // "toke"     (Big-Endian 32-bit)
    private static final int   USERID_INT_4          = 0x75736572;         // "user"     (Big-Endian 32-bit)
    private static final short USERID_SHORT_2         = 0x6964;             // "id"       (Big-Endian 16-bit)

    /**
     * 专属 13 字节 "authorization" 的无循环 64-bit Long + 32-bit Int + 8-bit Byte SWAR 匹配
     */
    private boolean equalsAuthorizationInt(ByteBuf buf, int start) {
        // 1. 前 8 字节 "authoriz" (Long)
        long first8 = buf.getLong(start);
        if ((first8 | SWAR_LOWERCASE_MASK_64) != (AUTHORIZATION_LONG_8 | SWAR_LOWERCASE_MASK_64)) {
            return false;
        }
        // 2. 中间 4 字节 "atio" (Int)
        int next4 = buf.getInt(start + 8);
        if ((next4 | SWAR_LOWERCASE_MASK_32) != (AUTHORIZATION_INT_4 | SWAR_LOWERCASE_MASK_32)) {
            return false;
        }
        // 3. 最后一个字节 'n' (Byte)
        byte last1 = buf.getByte(start + 12);
        return (last1 | 0x20) == 0x6e;
    }

    /**
     * 专属 5 字节 "token" 的无循环 32-bit Int + 8-bit Byte SWAR 匹配
     */
    private boolean equalsTokenInt(ByteBuf buf, int start) {
        int first4 = buf.getInt(start);
        if ((first4 | SWAR_LOWERCASE_MASK_32) != (TOKEN_INT_4 | SWAR_LOWERCASE_MASK_32)) {
            return false;
        }
        byte last1 = buf.getByte(start + 4);
        return (last1 | 0x20) == 0x6e;
    }

    /**
     * 专属 6 字节 "userid" 的无循环 32-bit Int + 16-bit Short SWAR 匹配
     */
    private boolean equalsUserIdInt(ByteBuf buf, int start) {
        int first4 = buf.getInt(start);
        if ((first4 | SWAR_LOWERCASE_MASK_32) != (USERID_INT_4 | SWAR_LOWERCASE_MASK_32)) {
            return false;
        }
        short last2 = buf.getShort(start + 4);
        return ((last2 & 0xFFFF) | (SWAR_LOWERCASE_MASK_16 & 0xFFFF)) == ((USERID_SHORT_2 & 0xFFFF) | (SWAR_LOWERCASE_MASK_16 & 0xFFFF));
    }

    private int advancePastNonCandidateLine(ByteBuf buf, int lineStart, int limit) {
        int lineEnd = findLineEnd(buf, lineStart, limit);
        if (lineEnd == limit) {
            buf.readerIndex(lineStart);
            return -1;
        }
        return advanceReaderIndexToNextLine(buf, lineEnd);
    }

    private int advanceReaderIndexToNextLine(ByteBuf buf, int lineEnd) {
        int nextLineStart = lineEnd + (buf.getByte(lineEnd) == '\r' ? 2 : 1);
        buf.readerIndex(nextLineStart);
        return nextLineStart;
    }

    private int findLineEnd(ByteBuf buf, int start, int limit) {
        for (int i = start; i < limit; i++) {
            byte b = buf.getByte(i);
            if (b == '\r' || b == '\n') {
                return i;
            }
        }
        return limit;
    }

    private boolean isIncompleteLine(int nextLineStart) {
        return nextLineStart == -1;
    }

    private boolean markIncomplete(ByteBuf buf, int lineStart) {
        buf.readerIndex(lineStart);
        return false;
    }

    private int findColon(ByteBuf buf, int start, int limit) {
        for (int i = start; i < limit; i++) {
            if (buf.getByte(i) == ':') {
                return i;
            }
        }
        return -1;
    }
}
