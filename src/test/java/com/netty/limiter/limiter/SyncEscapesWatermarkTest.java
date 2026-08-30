package com.netty.limiter.limiter;

import com.netty.limiter.cache.LocalBanCache;
import com.netty.limiter.cache.RedisUserBanSubscriber;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.lang.reflect.Method;
import java.nio.charset.StandardCharsets;

public class SyncEscapesWatermarkTest {

    @Test
    @DisplayName("测试 W: 80% 水位线预警标记将 ExpSec 设为 -2L 且触发同步校验降级")
    public void testWatermarkWarningSetExpSecMinus2() throws Exception {
        LocalBanCache cache = new LocalBanCache();
        RedisUserBanSubscriber subscriber = new RedisUserBanSubscriber();

        java.lang.reflect.Field field = RedisUserBanSubscriber.class.getDeclaredField("localBanCache");
        field.setAccessible(true);
        field.set(subscriber, cache);

        Method processMethod = RedisUserBanSubscriber.class.getDeclaredMethod("processByteBufLine0GC", ByteBuf.class);
        processMethod.setAccessible(true);

        // 1. 模拟收到 80% 水位线预警消息 "W:88888:1"
        ByteBuf warnBuf = Unpooled.copiedBuffer("W:88888:1", StandardCharsets.UTF_8);
        processMethod.invoke(subscriber, warnBuf);
        warnBuf.release();

        // 2. 验证 UID 88888 处于 BAN_STATUS_WARNED_SYNC_REQUIRED 状态 (ExpSec == -2L)
        int status = cache.getUserBanStatus(88888L);
        Assertions.assertEquals(LocalBanCache.BAN_STATUS_WARNED_SYNC_REQUIRED, status, "W: 消息必须将状态设为 BAN_STATUS_WARNED_SYNC_REQUIRED");
        Assertions.assertFalse(cache.isUserBanned(88888L), "预警状态下不能算作硬封禁");

        // 3. 模拟收到 100% 耗尽硬封禁消息 "U:88888:60"
        ByteBuf banBuf = Unpooled.copiedBuffer("U:88888:60", StandardCharsets.UTF_8);
        processMethod.invoke(subscriber, banBuf);
        banBuf.release();

        // 4. 验证 UID 88888 升级为 BAN_STATUS_HARD_BANNED 状态
        Assertions.assertEquals(LocalBanCache.BAN_STATUS_HARD_BANNED, cache.getUserBanStatus(88888L), "U: 消息必须升级为 BAN_STATUS_HARD_BANNED");
        Assertions.assertTrue(cache.isUserBanned(88888L), "100% 耗尽后算作硬封禁");
    }
}
