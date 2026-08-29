package com.netty.limiter.handler;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;

public class JwtHeaderSecurityBenchmarkTest {

    private static final byte[] AUTHORIZATION = {'a', 'u', 't', 'h', 'o', 'r', 'i', 'z', 'a', 't', 'i', 'o', 'n'};
    private static final byte[] TOKEN = {'t', 'o', 'k', 'e', 'n'};
    private static final byte[] USER_ID = {'u', 's', 'e', 'r', 'i', 'd'};

    private static final long SWAR_LOWERCASE_MASK_64 = 0x2020202020202020L;
    private static final int  SWAR_LOWERCASE_MASK_32 = 0x20202020;
    private static final short SWAR_LOWERCASE_MASK_16 = 0x2020;

    private static final long AUTHORIZATION_LONG_8 = 0x617574686f72697aL; // "authoriz"
    private static final int  AUTHORIZATION_INT_4  = 0x6174696f;         // "atio"
    private static final int  TOKEN_INT_4          = 0x746f6b65;         // "toke"
    private static final int  USERID_INT_4         = 0x75736572;         // "user"
    private static final short USERID_SHORT_2        = 0x6964;             // "id"

    // 方案 1: 旧标量 3 次盲目循环
    private boolean isJwtHeaderOld(ByteBuf buf, int start, int len) {
        return equalsKeyScalar(buf, start, len, AUTHORIZATION)
                || equalsKeyScalar(buf, start, len, TOKEN)
                || equalsKeyScalar(buf, start, len, USER_ID);
    }

    private boolean equalsKeyScalar(ByteBuf buf, int start, int len, byte[] target) {
        if (len != target.length) {
            return false;
        }
        for (int i = 0; i < len; i++) {
            byte b = buf.getByte(start + i);
            if (b >= 'A' && b <= 'Z') {
                b = (byte) (b + 32);
            }
            if (b != target[i]) {
                return false;
            }
        }
        return true;
    }

    // 方案 2: switch(len) + 8字节 Long SWAR + 尾部循环
    private boolean isJwtHeaderLoop(ByteBuf buf, int start, int len) {
        switch (len) {
            case 13:
                return equalsAuthorizationLoop(buf, start);
            case 5:
                return equalsTokenLoop(buf, start);
            case 6:
                return equalsUserIdLoop(buf, start);
            default:
                return false;
        }
    }

    private boolean equalsAuthorizationLoop(ByteBuf buf, int start) {
        long first8 = buf.getLong(start);
        if ((first8 | SWAR_LOWERCASE_MASK_64) != (AUTHORIZATION_LONG_8 | SWAR_LOWERCASE_MASK_64)) {
            return false;
        }
        for (int i = 8; i < 13; i++) {
            byte b = buf.getByte(start + i);
            if (b >= 'A' && b <= 'Z') {
                b = (byte) (b + 32);
            }
            if (b != AUTHORIZATION[i]) {
                return false;
            }
        }
        return true;
    }

    private boolean equalsTokenLoop(ByteBuf buf, int start) {
        for (int i = 0; i < 5; i++) {
            byte b = buf.getByte(start + i);
            if (b >= 'A' && b <= 'Z') {
                b = (byte) (b + 32);
            }
            if (b != TOKEN[i]) {
                return false;
            }
        }
        return true;
    }

    private boolean equalsUserIdLoop(ByteBuf buf, int start) {
        for (int i = 0; i < 6; i++) {
            byte b = buf.getByte(start + i);
            if (b >= 'A' && b <= 'Z') {
                b = (byte) (b + 32);
            }
            if (b != USER_ID[i]) {
                return false;
            }
        }
        return true;
    }

    // 方案 3: 【最新黑科技】switch(len) + getInt/getShort 纯整数位运算 (零循环 Loop-Free)
    private boolean isJwtHeaderInt(ByteBuf buf, int start, int len) {
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

    private boolean equalsAuthorizationInt(ByteBuf buf, int start) {
        // 前8字节 "authoriz" (Long)
        long first8 = buf.getLong(start);
        if ((first8 | SWAR_LOWERCASE_MASK_64) != (AUTHORIZATION_LONG_8 | SWAR_LOWERCASE_MASK_64)) {
            return false;
        }
        // 中间4字节 "atio" (Int)
        int next4 = buf.getInt(start + 8);
        if ((next4 | SWAR_LOWERCASE_MASK_32) != (AUTHORIZATION_INT_4 | SWAR_LOWERCASE_MASK_32)) {
            return false;
        }
        // 最后1字节 'n' (Byte)
        byte last1 = buf.getByte(start + 12);
        return (last1 | 0x20) == 0x6e;
    }

    private boolean equalsTokenInt(ByteBuf buf, int start) {
        int first4 = buf.getInt(start);
        if ((first4 | SWAR_LOWERCASE_MASK_32) != (TOKEN_INT_4 | SWAR_LOWERCASE_MASK_32)) {
            return false;
        }
        byte last1 = buf.getByte(start + 4);
        return (last1 | 0x20) == 0x6e;
    }

    private boolean equalsUserIdInt(ByteBuf buf, int start) {
        int first4 = buf.getInt(start);
        if ((first4 | SWAR_LOWERCASE_MASK_32) != (USERID_INT_4 | SWAR_LOWERCASE_MASK_32)) {
            return false;
        }
        short last2 = buf.getShort(start + 4);
        return ((last2 & 0xFFFF) | (SWAR_LOWERCASE_MASK_16 & 0xFFFF)) == ((USERID_SHORT_2 & 0xFFFF) | (SWAR_LOWERCASE_MASK_16 & 0xFFFF));
    }

    @Test
    public void testBenchmarkThreeApproaches() {
        String headerStr = "Authorization: Bearer eyJhbGciOiJIUzI1NiJ9.eyJ1aWQiOjEwMDg2fQ==\r\n";
        ByteBuf buf = Unpooled.directBuffer();
        buf.writeBytes(headerStr.getBytes(StandardCharsets.UTF_8));

        int iterations = 20_000_000;
        int keyStart = 0;
        int keyLen = 13;

        // JVM 预热
        for (int i = 0; i < 2_000_000; i++) {
            isJwtHeaderOld(buf, keyStart, keyLen);
            isJwtHeaderLoop(buf, keyStart, keyLen);
            isJwtHeaderInt(buf, keyStart, keyLen);
        }

        // 1. 旧标量 3次盲目循环
        long startOld = System.nanoTime();
        boolean resOld = true;
        for (int i = 0; i < iterations; i++) {
            resOld &= isJwtHeaderOld(buf, keyStart, keyLen);
        }
        double durationOldMs = (System.nanoTime() - startOld) / 1_000_000.0;

        // 2. switch(len) + Long SWAR + 尾部循环
        long startLoop = System.nanoTime();
        boolean resLoop = true;
        for (int i = 0; i < iterations; i++) {
            resLoop &= isJwtHeaderLoop(buf, keyStart, keyLen);
        }
        double durationLoopMs = (System.nanoTime() - startLoop) / 1_000_000.0;

        // 3. getInt / getShort 无循环位运算
        long startInt = System.nanoTime();
        boolean resInt = true;
        for (int i = 0; i < iterations; i++) {
            resInt &= isJwtHeaderInt(buf, keyStart, keyLen);
        }
        double durationIntMs = (System.nanoTime() - startInt) / 1_000_000.0;

        System.out.println("[ignoring loop detection]");
        System.out.println("=================================================");
        System.out.println(" 2,000 万次 Header Key 提取基准性能三方比对结果");
        System.out.println("=================================================");
        System.out.println(String.format("1. 旧标量 3次盲目循环 耗时  : %.3f ms (%.2f M Ops/s)", durationOldMs, (iterations / (durationOldMs / 1000.0)) / 1_000_000));
        System.out.println(String.format("2. switch(len)+Long SWAR 耗时 : %.3f ms (%.2f M Ops/s)", durationLoopMs, (iterations / (durationLoopMs / 1000.0)) / 1_000_000));
        System.out.println(String.format("3. getInt/getShort纯整数 耗时: %.3f ms (%.2f M Ops/s)", durationIntMs, (iterations / (durationIntMs / 1000.0)) / 1_000_000));
        System.out.println("-------------------------------------------------");
        System.out.println(String.format("getInt 纯整数相对旧标量提升   : %.2f%% (约 %.2f 倍速度)", ((durationOldMs - durationIntMs) / durationOldMs) * 100, durationOldMs / durationIntMs));
        System.out.println(String.format("getInt 纯整数相对上一版提升   : %.2f%%", ((durationLoopMs - durationIntMs) / durationLoopMs) * 100));
        System.out.println("=================================================");

        buf.release();
    }
}
