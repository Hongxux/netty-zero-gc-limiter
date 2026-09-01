package com.netty.limiter.limiter;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import org.junit.jupiter.api.Test;

import java.lang.reflect.Method;
import java.nio.charset.StandardCharsets;

import static org.junit.jupiter.api.Assertions.assertTrue;

class UserRateLimiterOperateResp2Test {

    @Test
    void encodesLuaKeyAndAllTokenBucketArguments() throws Exception {
        Method encoder = UserRateLimiterOperate.class.getDeclaredMethod(
                "encodeResp2EvalSha", ByteBuf.class, byte[].class, long.class);
        encoder.setAccessible(true);
        ByteBuf buf = Unpooled.buffer();
        try {
            encoder.invoke(null, buf,
                    "0123456789012345678901234567890123456789".getBytes(StandardCharsets.US_ASCII),
                    10086L);
            String command = buf.toString(StandardCharsets.US_ASCII);
            assertTrue(command.startsWith("*9\r\n$7\r\nEVALSHA\r\n"));
            assertTrue(command.contains("$5\r\n10086\r\n"));
            assertTrue(command.contains("$2\r\n20\r\n"));
            assertTrue(command.endsWith("$1\r\n1\r\n"));
        } finally {
            buf.release();
        }
    }
}
