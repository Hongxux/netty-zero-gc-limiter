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
        writeLongToAsciiByteBuf(buf, luaShaBytes.length);
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

        int uidLen = getLongAsciiLength(userId);
        buf.writeByte('$');
        writeLongToAsciiByteBuf(buf, uidLen);
        buf.writeByte('\r');
        buf.writeByte('\n');
        writeLongToAsciiByteBuf(buf, userId);
        buf.writeByte('\r');
        buf.writeByte('\n');
    }

    /**
     * 位运算替代除法与取模 (% 10 和 / 10)，极速写入 Long 的 ASCII 字节
     */
    public static void writeLongToAsciiByteBuf(ByteBuf buf, long value) {
        if (value == 0) {
            buf.writeByte('0');
            return;
        }
        if (value < 0) {
            buf.writeByte('-');
            value = -value;
        }
        int len = getLongAsciiLength(value);
        int startIdx = buf.writerIndex();
        buf.ensureWritable(len);
        buf.writerIndex(startIdx + len);

        int ptr = startIdx + len - 1;
        while (value > 0) {
            if (value <= Integer.MAX_VALUE) {
                int v = (int) value;
                int q = (int) ((v * 0xCCCCCCCDL) >>> 35); // 魔法乘法与位移替代 v / 10
                int r = v - ((q << 3) + (q << 1));        // 位移与减法替代 v % 10 (q * 10 = (q << 3) + (q << 1))
                buf.setByte(ptr--, (byte) ('0' + r));
                value = q;
            } else {
                long q = value / 10;
                long r = value - ((q << 3) + (q << 1));
                buf.setByte(ptr--, (byte) ('0' + (int) r));
                value = q;
            }
        }
    }

    public static int getLongAsciiLength(long val) {
        if (val == 0) return 1;
        if (val == Long.MIN_VALUE) return 20;
        int len = 0;
        if (val < 0) {
            len++;
            val = -val;
        }
        if (val < 10) return len + 1;
        if (val < 100) return len + 2;
        if (val < 1000) return len + 3;
        if (val < 10000) return len + 4;
        if (val < 100000) return len + 5;
        if (val < 1000000) return len + 6;
        if (val < 10000000) return len + 7;
        if (val < 100000000) return len + 8;
        if (val < 1000000000) return len + 9;
        if (val < 10000000000L) return len + 10;
        if (val < 100000000000L) return len + 11;
        if (val < 1000000000000L) return len + 12;
        if (val < 10000000000000L) return len + 13;
        if (val < 100000000000000L) return len + 14;
        if (val < 1000000000000000L) return len + 15;
        if (val < 10000000000000000L) return len + 16;
        if (val < 100000000000000000L) return len + 17;
        if (val < 1000000000000000000L) return len + 18;
        return len + 19;
    }
}
