package com.netty.limiter.util.jwt;

import com.netty.limiter.cache.JwtSigUidCache;
import com.netty.limiter.util.XxHash64Util;
import io.netty.buffer.ByteBuf;

/**
 * @description: 极致 0-GC JWT Payload UID 与 EXP 解析器 (Enum Singleton)
 * 编排架构：
 * 1. 核心入口显式编排：tryFastPathAuth (快路径) -> executeSlowPathAuthAndCache (慢路径)。
 * 2. 快路径 (Fast Path)：根据 64-bit xxHash64 签名 Hash (XxHash64Util) 查询 JwtSigUidCache 双静态 Flat Table，纳秒级放行。
 * 3. 慢路径 (Slow Path)：调用 JwtAuthenticator 执行 0-GC HMAC-SHA256 签名校验，并调用 JwtPayloadDfaParser 状态机提炼 "uid" 与 "exp"，成功后写入 Hot Table。
 **/
public enum ZeroGcJwtParser {

    /**
     * 唯一单例实例
     */
    INSTANCE;

    /**
     * 结合 0-GC 签名 xxHash64 双表缓存 (Fast Path) 与 HMAC-SHA256 验签 + DFA 状态机解析 (Slow Path) 提取 UID
     */
    public long authenticateJwtAndExtractUid(ByteBuf buf, int start, int maxLen) {
        return extractUidFromHeader(buf, start, start + maxLen);
    }

    /**
     * 核心鉴权入口：显式编排 0-GC 缓存快路径 (Fast Path) 与 HMAC 物理验签+DFA解析慢路径 (Slow Path)
     */
    public long extractUidFromHeader(ByteBuf buf, int start, int max) {
        int jwtStart = skipWhitespaceAndBearer(buf, start, max);
        return parseAndAuthenticateJwt(buf, jwtStart, max);
    }

    /**
     * 0-GC 物理段落寻址 (Header.Payload.Signature) 兼快速/慢速路径鉴权
     */
    private long parseAndAuthenticateJwt(ByteBuf buf, int jwtStart, int max) {
        int dot1 = buf.indexOf(jwtStart, max, (byte) '.');
        if (dot1 < 0) {
            return parsePlainUidNumber(buf, jwtStart, max - jwtStart);
        }

        int payloadStart = dot1 + 1;
        int dot2 = buf.indexOf(payloadStart, max, (byte) '.');
        if (dot2 < 0) {
            return 0L;
        }

        int payloadEnd = dot2;
        int sigStart = dot2 + 1;
        int sigEnd = findHeaderValueEnd(buf, sigStart, max);
        if (sigEnd <= sigStart) {
            return 0L;
        }

        // 1. 【快路径 Fast Path】：提取 8 字节签名前缀，计算 64-bit xxHash64，二重校验防碰撞
        long sigPrefix = extractSigPrefix(buf, sigStart, sigEnd);
        long sigHash = XxHash64Util.fastHash64(buf, sigStart, sigEnd);
        long cachedUid = tryFastPathAuthFromCache(sigHash, sigPrefix);
        if (cachedUid > 0) {
            return cachedUid;
        }

        // 2. 【慢路径 Slow Path】：HMAC-SHA256 初次验签 + DFA Payload 流式提炼 UID & EXP + 写入缓存
        return executeInitialSlowPathAuthAndCache(buf, jwtStart, payloadStart, dot2, payloadEnd, sigStart, sigEnd, sigHash, sigPrefix);
    }

    /**
     * 0-GC 栈内存抓取签名首 8 字节前缀 (标准 HMAC-SHA256 签名固化 43 字节，直接单指令读取)
     */
    private static long extractSigPrefix(ByteBuf buf, int sigStart, int sigEnd) {
        if (sigEnd - sigStart < 8) {
            return 0L; // 非法畸形签名短串 (< 8 字节)，直接返回 0L 哨兵
        }
        // Netty 内存优化：直接以 1 条 CPU mov 指令读取 64-bit 内存 (同时支持 Heap/Direct 堆外内存)
        return buf.getLong(sigStart);
    }

    /**
     * 【快路径 Fast Path】：查询 0-GC 双静态 Flat Table 缓存 (包含 Hash + Prefix 二重校验)
     */
    private long tryFastPathAuthFromCache(long sigHash, long sigPrefix) {
        return JwtSigUidCache.INSTANCE.get(sigHash, sigPrefix);
    }

    /**
     * 【慢路径 Slow Path】：HMAC-SHA256 初次物理校验 + DFA 流式提炼 UID 与 EXP，初次鉴权通过后写入 Hot Table 缓存
     */
    private long executeInitialSlowPathAuthAndCache(ByteBuf buf, int jwtStart, int payloadStart, int dot2, int payloadEnd, int sigStart, int sigEnd, long sigHash, long sigPrefix) {
        // 1. 0-GC HMAC-SHA256 物理签名校验
        boolean sigValid = JwtAuthenticator.verifyJwtSignature0Gc(buf, jwtStart, dot2, sigStart, sigEnd);
        if (!sigValid) {
            return 0L; // 签名错误，非法 JWT！
        }

        // 2. DFA 流式解码 Base64URL 提炼 UID & EXP 并完成校验与 0-GC 缓存回写 (纯栈原语运算, 0 堆分配, 0 数组, 0 装箱)
        return JwtPayloadDfaParser.parseAndCacheValidUid(buf, payloadStart, payloadEnd, sigHash, sigPrefix);
    }

    /**
     * 查找 Header 值的合法终点 (遇到空格、\r、\n、" 或 max)
     */
    private int findHeaderValueEnd(ByteBuf buf, int start, int max) {
        int p = start;
        while (p < max) {
            byte b = buf.getByte(p);
            if (b == '\r' || b == '\n' || b == ' ' || b == '"' || b == '\t') {
                break;
            }
            p++;
        }
        return p;
    }

    private int skipWhitespaceAndBearer(ByteBuf buf, int start, int max) {
        int p = start;
        while (p < max) {
            byte b = buf.getByte(p);
            if (b == ' ' || b == '\t' || b == '"' || b == '\'') {
                p++;
            } else {
                break;
            }
        }
        if (max - p >= 7) {
            byte b0 = buf.getByte(p);
            if (b0 == 'B' || b0 == 'b') {
                byte b1 = buf.getByte(p + 1);
                byte b2 = buf.getByte(p + 2);
                byte b3 = buf.getByte(p + 3);
                byte b4 = buf.getByte(p + 4);
                byte b5 = buf.getByte(p + 5);
                byte b6 = buf.getByte(p + 6);
                if ((b1 == 'e' || b1 == 'E') && (b2 == 'a' || b2 == 'A') &&
                    (b3 == 'r' || b3 == 'R') && (b4 == 'e' || b4 == 'E') &&
                    (b5 == 'r' || b5 == 'R') && b6 == ' ') {
                    return p + 7;
                }
            }
        }
        return p;
    }

    private long parsePlainUidNumber(ByteBuf buf, int start, int len) {
        long val = 0;
        int end = start + len;
        for (int i = start; i < end; i++) {
            byte b = buf.getByte(i);
            if (b >= '0' && b <= '9') {
                val = val * 10 + (b - '0');
            } else if (b == '\r' || b == '\n' || b == ' ' || b == '"') {
                break;
            } else {
                return 0L;
            }
        }
        return val;
    }
}
