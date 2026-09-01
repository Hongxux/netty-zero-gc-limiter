package com.netty.limiter.handler;

import com.netty.limiter.cache.LocalBanCache;
import com.netty.limiter.limiter.AsyncRateLimitContext;
import com.netty.limiter.limiter.LocalGlobalRateLimiter;
import com.netty.limiter.limiter.UserRateLimiterOperate;
import com.netty.limiter.util.SecurityAttributeKeys;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.embedded.EmbeddedChannel;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

import javax.crypto.Mac;
import javax.crypto.spec.SecretKeySpec;
import java.nio.charset.StandardCharsets;
import java.util.Base64;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.Mockito.*;

public class NettyReactiveModeBRateLimitTest {

    private static final String SECRET = "secret";

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
        when(localBanCache.isUserBanned(anyLong())).thenReturn(false);
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
    public void testReactiveModeBGranted_PausesAndResumesAutoRead() throws Exception {
        EmbeddedChannel channel = new EmbeddedChannel(jwtAccumulatorHandler, inboundSecurityHandler);
        long uid = 20001L;

        // 设置为 80% 水位预警 (-2L)，触发 Mode B
        when(localBanCache.getUserBanStatus(uid)).thenReturn(LocalBanCache.BAN_STATUS_WARNED_SYNC_REQUIRED);

        ArgumentCaptor<AsyncRateLimitContext> captor = ArgumentCaptor.forClass(AsyncRateLimitContext.class);

        String jwtToken = generateValidJwtRaw(uid);
        String httpRequest = "GET /api HTTP/1.1\r\n" +
                "Host: localhost\r\n" +
                "Authorization: Bearer " + jwtToken + "\r\n\r\n";

        ByteBuf buf = Unpooled.copiedBuffer(httpRequest, StandardCharsets.UTF_8);
        channel.writeInbound(buf);

        // 验证 Mode B 触发了异步分派
        verify(userRateLimiterOperate, times(1)).acquire0GcUidAsync(captor.capture());

        AsyncRateLimitContext capturedCtx = captor.getValue();
        assertNotNull(capturedCtx);
        assertEquals(uid, capturedCtx.userId);

        // 验证在等待期间 autoRead 被置为 false (实施 TCP 物理反压)
        assertFalse(channel.config().isAutoRead(), "AutoRead should be disabled during async Mode B check for backpressure");

        // 模拟 Redis 异步回包：放行 (granted = true)
        capturedCtx.callback.onResult(true);

        // 验证 autoRead 恢复为 true
        assertTrue(channel.config().isAutoRead(), "AutoRead should be restored to true after async check");

        // 验证请求成功透传下游
        ByteBuf passed = channel.readInbound();
        assertNotNull(passed, "Request should be passed downstream upon grant");
        passed.release();
    }

    @Test
    public void testReactiveModeBDenied_UpgradesToHardBanAndRejects403() throws Exception {
        EmbeddedChannel channel = new EmbeddedChannel(jwtAccumulatorHandler, inboundSecurityHandler);
        long uid = 20002L;

        when(localBanCache.getUserBanStatus(uid)).thenReturn(LocalBanCache.BAN_STATUS_WARNED_SYNC_REQUIRED);

        ArgumentCaptor<AsyncRateLimitContext> captor = ArgumentCaptor.forClass(AsyncRateLimitContext.class);

        String jwtToken = generateValidJwtRaw(uid);
        String httpRequest = "GET /api HTTP/1.1\r\n" +
                "Host: localhost\r\n" +
                "Authorization: Bearer " + jwtToken + "\r\n\r\n";

        ByteBuf buf = Unpooled.copiedBuffer(httpRequest, StandardCharsets.UTF_8);
        channel.writeInbound(buf);

        verify(userRateLimiterOperate, times(1)).acquire0GcUidAsync(captor.capture());
        AsyncRateLimitContext capturedCtx = captor.getValue();

        // 模拟 Redis 异步回包：拒绝 (granted = false)
        capturedCtx.callback.onResult(false);

        // 验证升级为硬封禁
        verify(localBanCache, times(1)).putUserBan(uid, 60);

        // 验证返回 403 且关闭连接
        ByteBuf out = channel.readOutbound();
        assertNotNull(out);
        String response = out.toString(StandardCharsets.UTF_8);
        assertTrue(response.contains("403 Forbidden"), "Should respond 403 Forbidden on rate limit rejection");
        out.release();

        assertFalse(channel.isOpen(), "Channel should be closed on rejection");
    }

    @Test
    public void testAsyncContextRecyclerPooling() {
        // 测试 Recycler 对象池获取与回收重置 (初始与回收后状态均为 STATE_UNPUBLISHED)
        AsyncRateLimitContext ctx1 = AsyncRateLimitContext.acquire(null, null, 888L, allowed -> {});
        assertEquals(888L, ctx1.userId);
        assertEquals(AsyncRateLimitContext.STATE_UNPUBLISHED, ctx1.state.get());

        // 模拟生产者发布后状态置为 STATE_INIT
        ctx1.state.set(AsyncRateLimitContext.STATE_INIT);
        assertEquals(AsyncRateLimitContext.STATE_INIT, ctx1.state.get());

        // 模拟 Redis 响应处理完毕
        ctx1.state.set(AsyncRateLimitContext.STATE_RESOLVED);

        // 回收
        ctx1.recycle();
        assertEquals(0L, ctx1.userId);
        assertNull(ctx1.callback);
        assertEquals(AsyncRateLimitContext.STATE_UNPUBLISHED, ctx1.state.get());

        // 再次获取应重用实例且状态已重置为 STATE_UNPUBLISHED
        AsyncRateLimitContext ctx2 = AsyncRateLimitContext.acquire(null, null, 999L, allowed -> {});
        assertEquals(999L, ctx2.userId);
        assertEquals(AsyncRateLimitContext.STATE_UNPUBLISHED, ctx2.state.get());
        ctx2.recycle();
    }
}
