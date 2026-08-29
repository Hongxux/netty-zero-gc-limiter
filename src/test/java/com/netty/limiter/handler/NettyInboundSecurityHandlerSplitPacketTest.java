package com.netty.limiter.handler;

import com.netty.limiter.cache.LocalBanCache;
import com.netty.limiter.limiter.LocalGlobalRateLimiter;
import com.netty.limiter.limiter.UserRateLimiterOperate;
import com.netty.limiter.util.SecurityAttributeKeys;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.embedded.EmbeddedChannel;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

import javax.crypto.Mac;
import javax.crypto.spec.SecretKeySpec;
import java.nio.charset.StandardCharsets;
import java.util.Base64;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.Mockito.when;

public class NettyInboundSecurityHandlerSplitPacketTest {

    private static final String SECRET = "damai-seckill-secret";

    @Mock
    private LocalGlobalRateLimiter localGlobalRateLimiter;

    @Mock
    private LocalBanCache localBanCache;

    @Mock
    private UserRateLimiterOperate userRateLimiterOperate;

    private JwtHeaderSecurityHandler jwtHeaderSecurityHandler;

    private NettyJwtHeaderAccumulatorHandler jwtAccumulatorHandler;

    private NettyInboundSecurityHandler inboundSecurityHandler;

    @BeforeEach
    public void setUp() {
        MockitoAnnotations.openMocks(this);
        jwtHeaderSecurityHandler = new JwtHeaderSecurityHandler();

        jwtAccumulatorHandler = new NettyJwtHeaderAccumulatorHandler();
        setField(jwtAccumulatorHandler, "jwtHeaderSecurityHandler", jwtHeaderSecurityHandler);

        inboundSecurityHandler = new NettyInboundSecurityHandler();
        setField(inboundSecurityHandler, "localGlobalRateLimiter", localGlobalRateLimiter);
        setField(inboundSecurityHandler, "jwtHeaderSecurityHandler", jwtHeaderSecurityHandler);
        setField(inboundSecurityHandler, "localBanCache", localBanCache);
        setField(inboundSecurityHandler, "userRateLimiterOperate", userRateLimiterOperate);

        when(localGlobalRateLimiter.tryAcquire()).thenReturn(true);
        when(localBanCache.getUserBanInfo(anyLong())).thenReturn(null);
        when(userRateLimiterOperate.acquire0GcUid(anyLong(), any())).thenReturn(true);
    }

    private void setField(Object target, String fieldName, Object value) {
        try {
            java.lang.reflect.Field field = target.getClass().getDeclaredField(fieldName);
            field.setAccessible(true);
            field.set(target, value);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    private String generateValidJwtRaw(long uid) throws Exception {
        long futureExp = (System.currentTimeMillis() / 1000) + 3600;
        String headerJson = "{\"alg\":\"HS256\",\"typ\":\"JWT\"}";
        String payloadJson = "{\"uid\":" + uid + ",\"exp\":" + futureExp + "}";
        String headerB64 = Base64.getUrlEncoder().withoutPadding().encodeToString(headerJson.getBytes(StandardCharsets.UTF_8));
        String payloadB64 = Base64.getUrlEncoder().withoutPadding().encodeToString(payloadJson.getBytes(StandardCharsets.UTF_8));
        String content = headerB64 + "." + payloadB64;

        Mac mac = Mac.getInstance("HmacSHA256");
        mac.init(new SecretKeySpec(SECRET.getBytes(StandardCharsets.UTF_8), "HmacSHA256"));
        byte[] hmacBytes = mac.doFinal(content.getBytes(StandardCharsets.UTF_8));
        String sigB64 = Base64.getUrlEncoder().withoutPadding().encodeToString(hmacBytes);

        return content + "." + sigB64;
    }

    @Test
    public void testFullPacketPassesImmediately() throws Exception {
        EmbeddedChannel channel = new EmbeddedChannel(jwtAccumulatorHandler, inboundSecurityHandler);

        String jwtToken = generateValidJwtRaw(10086L);
        String httpRequest = "GET /api HTTP/1.1\r\n" +
                "Host: localhost\r\n" +
                "Authorization: Bearer " + jwtToken + "\r\n\r\n";

        ByteBuf buf = Unpooled.copiedBuffer(httpRequest, StandardCharsets.UTF_8);
        boolean written = channel.writeInbound(buf);
        assertTrue(written, "writeInbound should return true");

        ByteBuf readBuf = channel.readInbound();
        assertNotNull(readBuf);
        assertEquals(10086L, channel.attr(SecurityAttributeKeys.USER_ID).get());
        readBuf.release();
    }

    @Test
    public void testSplitPacketJwtAccumulation() throws Exception {
        EmbeddedChannel channel = new EmbeddedChannel(jwtAccumulatorHandler, inboundSecurityHandler);

        String jwtToken = generateValidJwtRaw(88888L);
        String fullHeader = "GET /api HTTP/1.1\r\n" +
                "Host: localhost\r\n" +
                "Authorization: Bearer " + jwtToken + "\r\n\r\n";

        // 拆分成 2 个 TCP 包，Packet 1 切在 Authorization 头中间
        int splitIndex = fullHeader.indexOf("Authorization: Bearer ") + 10;
        String packet1Str = fullHeader.substring(0, splitIndex);
        String packet2Str = fullHeader.substring(splitIndex);

        ByteBuf p1 = Unpooled.copiedBuffer(packet1Str, StandardCharsets.UTF_8);
        ByteBuf p2 = Unpooled.copiedBuffer(packet2Str, StandardCharsets.UTF_8);

        // 包 1 写入，应该被 0-GC 聚合器挂起积压，暂未触发 fireChannelRead
        assertFalse(channel.writeInbound(p1));
        assertNull(channel.readInbound());
        assertNull(channel.attr(SecurityAttributeKeys.USER_ID).get());

        // 包 2 写入，聚合完成，JWT 提取成功并触发 fireChannelRead
        assertTrue(channel.writeInbound(p2));
        ByteBuf readBuf = channel.readInbound();
        assertNotNull(readBuf);

        assertEquals(88888L, channel.attr(SecurityAttributeKeys.USER_ID).get());
        readBuf.release();
    }

    @Test
    public void testSplitPacketAnonymousRequestAccumulation() {
        EmbeddedChannel channel = new EmbeddedChannel(jwtAccumulatorHandler, inboundSecurityHandler);

        String fullHeader = "GET /public HTTP/1.1\r\n" +
                "Host: localhost\r\n" +
                "User-Agent: curl/7.68.0\r\n\r\n";

        int splitIndex = 25;
        String packet1Str = fullHeader.substring(0, splitIndex);
        String packet2Str = fullHeader.substring(splitIndex);

        ByteBuf p1 = Unpooled.copiedBuffer(packet1Str, StandardCharsets.UTF_8);
        ByteBuf p2 = Unpooled.copiedBuffer(packet2Str, StandardCharsets.UTF_8);

        assertFalse(channel.writeInbound(p1));
        assertNull(channel.readInbound());

        channel.writeInbound(p2);
        assertNull(channel.readInbound(), "Anonymous request should be rejected, readInbound should return null");

        ByteBuf outBuf = channel.readOutbound();
        assertNotNull(outBuf, "Outbound should contain 401 Unauthorized response");
        String responseStr = outBuf.toString(StandardCharsets.UTF_8);
        assertTrue(responseStr.contains("401 Unauthorized"));
        outBuf.release();
    }

    @Test
    public void testHttpBodyDirectPassAfterHeaderPassed() throws Exception {
        EmbeddedChannel channel = new EmbeddedChannel(jwtAccumulatorHandler, inboundSecurityHandler);

        String jwtToken = generateValidJwtRaw(66666L);
        String httpRequestHeader = "POST /api/upload HTTP/1.1\r\n" +
                "Host: localhost\r\n" +
                "Content-Length: 100\r\n" +
                "Authorization: Bearer " + jwtToken + "\r\n\r\n";

        ByteBuf headerBuf = Unpooled.copiedBuffer(httpRequestHeader, StandardCharsets.UTF_8);
        assertTrue(channel.writeInbound(headerBuf));

        ByteBuf readHeaderBuf = channel.readInbound();
        assertNotNull(readHeaderBuf);
        readHeaderBuf.release();

        assertEquals(Boolean.TRUE, channel.attr(SecurityAttributeKeys.HEADER_PASSED).get());

        // 后续 TCP Body 数据包发送，由于 HEADER_PASSED 为 true，直接透传 downstream
        String bodyChunk = "{\"data\":\"chunk_content\"}";
        ByteBuf bodyBuf = Unpooled.copiedBuffer(bodyChunk, StandardCharsets.UTF_8);
        assertTrue(channel.writeInbound(bodyBuf));

        ByteBuf readBodyBuf = channel.readInbound();
        assertNotNull(readBodyBuf);
        assertEquals(bodyChunk, readBodyBuf.toString(StandardCharsets.UTF_8));
        readBodyBuf.release();
    }
}
