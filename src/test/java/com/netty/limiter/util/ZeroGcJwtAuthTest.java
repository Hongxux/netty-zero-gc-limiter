package com.netty.limiter.util;

import com.netty.limiter.util.jwt.ZeroGcJwtParser;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import javax.crypto.Mac;
import javax.crypto.spec.SecretKeySpec;
import java.nio.charset.StandardCharsets;
import java.util.Base64;

public class ZeroGcJwtAuthTest {

    private static final String SECRET = "damai-seckill-secret";

    private String generateValidJwt(String headerJson, String payloadJson) throws Exception {
        String headerB64 = Base64.getUrlEncoder().withoutPadding().encodeToString(headerJson.getBytes(StandardCharsets.UTF_8));
        String payloadB64 = Base64.getUrlEncoder().withoutPadding().encodeToString(payloadJson.getBytes(StandardCharsets.UTF_8));
        String content = headerB64 + "." + payloadB64;

        Mac mac = Mac.getInstance("HmacSHA256");
        mac.init(new SecretKeySpec(SECRET.getBytes(StandardCharsets.UTF_8), "HmacSHA256"));
        byte[] hmacBytes = mac.doFinal(content.getBytes(StandardCharsets.UTF_8));
        String sigB64 = Base64.getUrlEncoder().withoutPadding().encodeToString(hmacBytes);

        return "Bearer " + content + "." + sigB64;
    }

    @Test
    public void testValidJwtAuthentication() throws Exception {
        long futureExp = (System.currentTimeMillis() / 1000) + 3600;
        String jwt = generateValidJwt("{\"alg\":\"HS256\",\"typ\":\"JWT\"}", "{\"uid\":10086,\"exp\":" + futureExp + "}");
        System.out.println("Generated JWT: " + jwt);
        ByteBuf buf = Unpooled.directBuffer();
        buf.writeBytes(jwt.getBytes(StandardCharsets.UTF_8));

        long uid = ZeroGcJwtParser.INSTANCE.authenticateJwtAndExtractUid(buf, 0, buf.readableBytes());
        System.out.println("Result UID: " + uid);
        Assertions.assertEquals(10086L, uid);

        // 第二次查询：触发 0-GC 签名 Hash 快路径 (Fast Path Cache Hit)
        long cachedUid = ZeroGcJwtParser.INSTANCE.authenticateJwtAndExtractUid(buf, 0, buf.readableBytes());
        System.out.println("Fast Path Cache Hit UID: " + cachedUid);
        Assertions.assertEquals(10086L, cachedUid);

        buf.release();
    }

    @Test
    public void testExpiredJwtToken() throws Exception {
        long pastExp = (System.currentTimeMillis() / 1000) - 3600; // 1小时前已过期
        String jwt = generateValidJwt("{\"alg\":\"HS256\",\"typ\":\"JWT\"}", "{\"uid\":10086,\"exp\":" + pastExp + "}");
        ByteBuf buf = Unpooled.directBuffer();
        buf.writeBytes(jwt.getBytes(StandardCharsets.UTF_8));

        long uid = ZeroGcJwtParser.INSTANCE.authenticateJwtAndExtractUid(buf, 0, buf.readableBytes());
        System.out.println("Expired JWT Result UID: " + uid);
        Assertions.assertEquals(0L, uid);

        buf.release();
    }

    @Test
    public void testTamperedJwtAuthenticationFails() throws Exception {
        String validJwt = generateValidJwt("{\"alg\":\"HS256\",\"typ\":\"JWT\"}", "{\"uid\":10086}");
        // 篡改密文中的字符 (模拟黑客黑客篡改 uid 为 88888)
        String tamperedJwt = validJwt.replace("eyJ1aWQiOjEwMDg2fQ", "eyJ1aWQiOjg4ODg4fQ");

        ByteBuf buf = Unpooled.directBuffer();
        buf.writeBytes(tamperedJwt.getBytes(StandardCharsets.UTF_8));

        long uid = ZeroGcJwtParser.INSTANCE.authenticateJwtAndExtractUid(buf, 0, buf.readableBytes());
        System.out.println("非法篡改 JWT 鉴权失败，返回 UID: " + uid);
        // 签名不匹配，强行拦截返回 0
        buf.release();
    }

    @Test
    public void testMissingExpClaimTreatedAsExpired() throws Exception {
        // Payload 中无 exp 字段，一律当作过期拦截
        String jwt = generateValidJwt("{\"alg\":\"HS256\",\"typ\":\"JWT\"}", "{\"uid\":10086}");
        ByteBuf buf = Unpooled.directBuffer();
        buf.writeBytes(jwt.getBytes(StandardCharsets.UTF_8));

        long uid = ZeroGcJwtParser.INSTANCE.authenticateJwtAndExtractUid(buf, 0, buf.readableBytes());
        System.out.println("Missing Exp JWT Result UID: " + uid);
        Assertions.assertEquals(0L, uid);

        buf.release();
    }
}
