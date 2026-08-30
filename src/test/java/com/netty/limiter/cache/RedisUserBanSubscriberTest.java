package com.netty.limiter.cache;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.lang.reflect.Method;
import java.nio.charset.StandardCharsets;

public class RedisUserBanSubscriberTest {

    @Test
    @DisplayName("测试 0-GC RESP2 UID 黑名单消息解析与生效")
    public void testProcessByteBufLine0GC() throws Exception {
        LocalBanCache cache = new LocalBanCache();
        RedisUserBanSubscriber subscriber = new RedisUserBanSubscriber();

        // 通过反射注入 localBanCache
        java.lang.reflect.Field field = RedisUserBanSubscriber.class.getDeclaredField("localBanCache");
        field.setAccessible(true);
        field.set(subscriber, cache);

        Method processMethod = RedisUserBanSubscriber.class.getDeclaredMethod("processByteBufLine0GC", ByteBuf.class);
        processMethod.setAccessible(true);

        // 1. 测试标准格式 UID 封禁 ("10086:300")
        ByteBuf uidBuf = Unpooled.copiedBuffer("10086:300", StandardCharsets.UTF_8);
        processMethod.invoke(subscriber, uidBuf);
        Assertions.assertTrue(cache.isUserBanned(10086L));
        uidBuf.release();

        // 2. 测试带前缀格式 UID 封禁 ("U:88888:120")
        ByteBuf uPrefixBuf = Unpooled.copiedBuffer("U:88888:120", StandardCharsets.UTF_8);
        processMethod.invoke(subscriber, uPrefixBuf);
        Assertions.assertTrue(cache.isUserBanned(88888L));
        uPrefixBuf.release();

        // 3. 测试默认时长格式 ("99999")
        ByteBuf defaultDurationBuf = Unpooled.copiedBuffer("99999", StandardCharsets.UTF_8);
        processMethod.invoke(subscriber, defaultDurationBuf);
        Assertions.assertTrue(cache.isUserBanned(99999L));
        defaultDurationBuf.release();
    }
}
