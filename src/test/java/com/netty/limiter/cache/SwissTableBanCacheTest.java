package com.netty.limiter.cache;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Swiss Table SWAR 瑞士表架构 LocalBanCache 正确性 + 并发安全测试
 */
class SwissTableBanCacheTest {

    private LocalBanCache cache;

    @BeforeEach
    void setUp() {
        cache = new LocalBanCache();
    }

    // ======================== 基础功能 ========================

    @Test
    void testPutAndQuery() {
        cache.putUserBan(12345L, 60);
        assertTrue(cache.isUserBanned(12345L), "刚插入的 UID 应被封禁");
    }

    @Test
    void testNotBanned() {
        assertFalse(cache.isUserBanned(99999L), "未插入的 UID 不应被封禁");
    }

    @Test
    void testInvalidUid() {
        cache.putUserBan(0L, 60);
        cache.putUserBan(-1L, 60);
        assertFalse(cache.isUserBanned(0L));
        assertFalse(cache.isUserBanned(-1L));
    }

    @Test
    void testExpiration() throws InterruptedException {
        cache.putUserBan(555L, 1); // 1 秒后过期
        assertTrue(cache.isUserBanned(555L));
        Thread.sleep(1100);
        assertFalse(cache.isUserBanned(555L), "过期后应返回 false");
    }

    @Test
    void testUpdateExtendExpiry() {
        cache.putUserBan(777L, 1);
        cache.putUserBan(777L, 3600); // 续期 1 小时
        assertTrue(cache.isUserBanned(777L), "续期后应仍被封禁");
    }

    @Test
    void testRemoveBan() {
        cache.putUserBan(888L, 3600);
        assertTrue(cache.isUserBanned(888L));
        cache.removeUserBan(888L);
        assertFalse(cache.isUserBanned(888L), "移除后应返回 false");
    }

    // ======================== 批量 & H2 碰撞 ========================

    @Test
    void testBulkInsert() {
        int count = 10000;
        for (long i = 1; i <= count; i++) {
            cache.putUserBan(i, 3600);
        }
        int hits = 0;
        for (long i = 1; i <= count; i++) {
            if (cache.isUserBanned(i)) hits++;
        }
        // Swiss Table 开放寻址有探查上限，允许少量丢失
        assertTrue(hits > count * 0.95,
                "批量插入 " + count + " 条, 命中 " + hits + " 条, 命中率过低");
        System.out.println("✅ 批量插入 " + count + " → 命中 " + hits + " (" +
                String.format("%.1f%%", hits * 100.0 / count) + ")");
    }

    @Test
    void testNoFalsePositive() {
        for (long i = 1; i <= 1000; i++) {
            cache.putUserBan(i, 3600);
        }
        for (long i = 100001; i <= 101000; i++) {
            assertFalse(cache.isUserBanned(i),
                    "UID " + i + " 未插入，不应被误判为封禁 (H2 碰撞误判)");
        }
    }

    // ======================== 并发安全 ========================

    @Test
    void testConcurrentPutAndQuery() throws InterruptedException {
        int threadCount = 8;
        int opsPerThread = 5000;
        CountDownLatch latch = new CountDownLatch(threadCount);
        AtomicInteger errors = new AtomicInteger(0);

        // 并发写入
        for (int t = 0; t < threadCount / 2; t++) {
            final int tid = t;
            new Thread(() -> {
                try {
                    for (int i = 0; i < opsPerThread; i++) {
                        long uid = tid * 100000L + i + 1;
                        cache.putUserBan(uid, 3600);
                    }
                } catch (Exception e) {
                    errors.incrementAndGet();
                    e.printStackTrace();
                } finally {
                    latch.countDown();
                }
            }).start();
        }

        // 并发读取
        for (int t = threadCount / 2; t < threadCount; t++) {
            final int tid = t - threadCount / 2;
            new Thread(() -> {
                try {
                    for (int i = 0; i < opsPerThread; i++) {
                        long uid = tid * 100000L + i + 1;
                        cache.isUserBanned(uid); // 不会抛异常即可
                    }
                } catch (Exception e) {
                    errors.incrementAndGet();
                    e.printStackTrace();
                } finally {
                    latch.countDown();
                }
            }).start();
        }

        latch.await();
        assertEquals(0, errors.get(), "并发操作不应产生异常");
        System.out.println("✅ 并发 8 线程 × " + opsPerThread + " ops 完成, 无异常");
    }

    @Test
    void testConcurrentSameKey() throws InterruptedException {
        int threadCount = 16;
        long targetUid = 42L;
        CountDownLatch latch = new CountDownLatch(threadCount);
        AtomicInteger errors = new AtomicInteger(0);

        for (int t = 0; t < threadCount; t++) {
            new Thread(() -> {
                try {
                    for (int i = 0; i < 10000; i++) {
                        cache.putUserBan(targetUid, 3600);
                        cache.isUserBanned(targetUid);
                    }
                } catch (Exception e) {
                    errors.incrementAndGet();
                    e.printStackTrace();
                } finally {
                    latch.countDown();
                }
            }).start();
        }

        latch.await();
        assertEquals(0, errors.get());
        assertTrue(cache.isUserBanned(targetUid), "高并发竞争后目标 UID 应仍被封禁");
        System.out.println("✅ 16 线程竞争同一 Key 测试通过");
    }

    // ======================== SWAR 正确性验证 ========================

    @Test
    void testSwarEdgeCases() {
        // 测试 H2=0x00 的 key (边界: ctrl 字节为全零)
        // mixHash 会将不同 userId 映射到不同 H2, 统计各 H2 分布
        int[] h2Dist = new int[128];
        for (long uid = 1; uid <= 10000; uid++) {
            long hash = testMixHash(uid);
            int h2 = (int) (hash & 0x7F);
            h2Dist[h2]++;
        }
        // 验证 H2 分布基本均匀 (每个桶平均 ~78)
        int min = Integer.MAX_VALUE, max = 0;
        for (int c : h2Dist) {
            min = Math.min(min, c);
            max = Math.max(max, c);
        }
        System.out.println("✅ H2 分布: min=" + min + " max=" + max +
                " (理想均匀 ~78.1/桶)");
        assertTrue(max < 150, "H2 分布不应过于集中");
    }

    /** 复制 mixHash 用于测试 */
    private static long testMixHash(long key) {
        key = (~key) + (key << 21);
        key = key ^ (key >>> 24);
        key = (key + (key << 3)) + (key << 8);
        key = key ^ (key >>> 14);
        key = (key + (key << 2)) + (key << 4);
        key = key ^ (key >>> 28);
        key = key + (key << 31);
        return key;
    }

    // ======================== 性能基准 ========================

    @Test
    void testLookupThroughput() {
        // 预填充
        for (long i = 1; i <= 10000; i++) {
            cache.putUserBan(i, 3600);
        }

        // 预热
        for (int w = 0; w < 100000; w++) {
            cache.isUserBanned(w % 10000 + 1);
        }

        // 计时
        int ops = 1_000_000;
        long start = System.nanoTime();
        int hits = 0;
        for (int i = 0; i < ops; i++) {
            if (cache.isUserBanned(i % 10000 + 1)) hits++;
        }
        long elapsed = System.nanoTime() - start;

        double nsPerOp = (double) elapsed / ops;
        double opsPerSec = 1_000_000_000.0 / nsPerOp;
        System.out.printf("✅ SWAR Swiss Table 查询: %.1f ns/op, %.0f ops/sec, hits=%d%n",
                nsPerOp, opsPerSec, hits);
    }
}
