package com.netty.limiter.util;

import io.netty.buffer.ByteBuf;
import java.nio.charset.StandardCharsets;

/**
 * @description: RESP2 协议 0-GC 极速序列化与 SWAR 位运算工具类
 **/
public class Resp2Encoder {

    private static final byte[] EVALSHA_CMD_PREFIX = "*4\r\n$7\r\nEVALSHA\r\n".getBytes(StandardCharsets.US_ASCII);

    /**
     * 封装 RESP2 协议 EVALSHA 命令的 0-GC 快速序列化逻辑
     */
    public static void encodeResp2EvalSha(ByteBuf buf, byte[] luaShaBytes, long userId) {
        buf.writeBytes(EVALSHA_CMD_PREFIX);

        buf.writeByte('$');
        ZeroGcNumberUtil.writeLongToAsciiByteBuf(buf, luaShaBytes.length);
        buf.writeByte('\r');
        buf.writeByte('\n');
        buf.writeBytes(luaShaBytes);
        buf.writeByte('\r');
        buf.writeByte('\n');

        buf.writeByte('$');
        buf.writeByte('1');
        buf.writeByte('\r');
        buf.writeByte('\n');
        buf.writeByte('1');
        buf.writeByte('\r');
        buf.writeByte('\n');

        int uidLen = ZeroGcNumberUtil.getLongAsciiLength(userId);
        buf.writeByte('$');
        ZeroGcNumberUtil.writeLongToAsciiByteBuf(buf, uidLen);
        buf.writeByte('\r');
        buf.writeByte('\n');
        ZeroGcNumberUtil.writeLongToAsciiByteBuf(buf, userId);
        buf.writeByte('\r');
        buf.writeByte('\n');
    }

    public static void writeLongToAsciiByteBuf(ByteBuf buf, long value) {
        ZeroGcNumberUtil.writeLongToAsciiByteBuf(buf, value);
    }

    public static int getLongAsciiLength(long val) {
        return ZeroGcNumberUtil.getLongAsciiLength(val);
    }
}
