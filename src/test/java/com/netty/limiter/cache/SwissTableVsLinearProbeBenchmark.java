package com.netty.limiter.cache;

import org.junit.jupiter.api.Test;

import java.lang.invoke.MethodHandles;
import java.lang.invoke.VarHandle;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicLong;

/**
 * 🔬 A/B 对比基准测试: Swiss Table SWAR vs 原始线性探查
 *
 * 测试维度:
 * 1. 单线程查询吞吐 (命中 + 未命中)
 * 2. 单线程写入吞吐
 * 3. 多线程并发混合读写吞吐
 * 4. 不同负载因子下的查询性能衰减
 */
class SwissTableVsLinearProbeBenchmark {

    // ======================== 旧版线性探查实现 (内联基线) ========================

    static class OldLinearProbeCache {
        private static final int CAPACITY = 65536;
        private static final int MASK = CAPACITY - 1;
        private static final int MAX_PROBE = 16;

        private static final VarHandle KEYS_VH = MethodHandles.arrayElementVarHandle(long[].class);
        private static final VarHandle EXPIRES_VH = MethodHandles.arrayElementVarHandle(long[].class);

        private final long[] uidKeys = new long[CAPACITY];
        private final long[] uidExpires = new long[CAPACITY];

        public void putUserBan(long userId, long durationSeconds) {
            if (userId <= 0) return;
            int baseIndex = (int) (mixHash(userId) & MASK);
            long now = System.currentTimeMillis();
            long expireTime = now + (durationSeconds * 1000L);

            for (int i = 0; i < MAX_PROBE; i++) {
                int index = (baseIndex + i) & MASK;
                long storedUid = (long) KEYS_VH.getAcquire(uidKeys, index);

                if (storedUid == userId) {
                    EXPIRES_VH.setRelease(uidExpires, index, expireTime);
                    return;
                }
                if (storedUid == 0L) {
                    if (KEYS_VH.compareAndSet(uidKeys, index, 0L, userId)) {
                        EXPIRES_VH.setRelease(uidExpires, index, expireTime);
                        return;
                    }
                    storedUid = (long) KEYS_VH.getAcquire(uidKeys, index);
                    if (storedUid == userId) {
                        EXPIRES_VH.setRelease(uidExpires, index, expireTime);
                        return;
                    }
                }
                long storedExpire = (long) EXPIRES_VH.getAcquire(uidExpires, index);
                if (storedExpire <= now) {
                    if (KEYS_VH.compareAndSet(uidKeys, index, storedUid, userId)) {
                        EXPIRES_VH.setRelease(uidExpires, index, expireTime);
                        return;
                    }
                }
            }
            if (KEYS_VH.compareAndSet(uidKeys, baseIndex, uidKeys[baseIndex], userId)) {
                EXPIRES_VH.setRelease(uidExpires, baseIndex, expireTime);
            } else {
                KEYS_VH.setRelease(uidKeys, baseIndex, userId);
                EXPIRES_VH.setRelease(uidExpires, baseIndex, expireTime);
            }
        }

        public boolean isUserBanned(long userId) {
            if (userId <= 0) return false;
            int baseIndex = (int) (mixHash(userId) & MASK);
            long now = System.currentTimeMillis();

            for (int i = 0; i < MAX_PROBE; i++) {
                int index = (baseIndex + i) & MASK;
                long storedUid = (long) KEYS_VH.getAcquire(uidKeys, index);
                if (storedUid == userId) {
                    long expireTime = (long) EXPIRES_VH.getAcquire(uidExpires, index);
                    return expireTime > now;
                }
                if (storedUid == 0L) return false;
            }
            return false;
        }

        private static long mixHash(long key) {
            key = (~key) + (key << 21);
            key = key ^ (key >>> 24);
            key = (key + (key << 3)) + (key << 8);
            key = key ^ (key >>> 14);
            key = (key + (key << 2)) + (key << 4);
            key = key ^ (key >>> 28);
            key = key + (key << 31);
            return key;
        }
    }

    // ======================== 基准测试工具 ========================

    private static final int WARMUP_OPS = 200_000;
    private static final int BENCH_OPS = 2_000_000;

    private static long benchQuery(String label, Runnable queryOp, int ops) {
        // 预热
        for (int i = 0; i < WARMUP_OPS; i++) queryOp.run();

        long start = System.nanoTime();
        for (int i = 0; i < ops; i++) queryOp.run();
        long elapsed = System.nanoTime() - start;

        double nsPerOp = (double) elapsed / ops;
        double mOps = 1_000.0 / nsPerOp; // million ops/sec
        System.out.printf("  %-30s %7.1f ns/op  %7.2f M ops/sec%n", label, nsPerOp, mOps);
        return elapsed;
    }

    // ======================== 测试 1: 查询命中 ========================

    @Test
    void benchmark_QueryHit() {
        System.out.println("\n═══════════════════════════════════════════════════════════");
        System.out.println("  📊 [Test 1] 单线程查询命中 (10K 条目, 100% 命中)");
        System.out.println("═══════════════════════════════════════════════════════════");

        OldLinearProbeCache oldCache = new OldLinearProbeCache();
        LocalBanCache swissCache = new LocalBanCache();

        for (long i = 1; i <= 10000; i++) {
            oldCache.putUserBan(i, 3600);
            swissCache.putUserBan(i, 3600);
        }

        final int[] idx = {0};

        long oldTime = benchQuery("旧版 Linear Probe", () -> {
            oldCache.isUserBanned((idx[0]++ % 10000) + 1);
        }, BENCH_OPS);

        idx[0] = 0;
        long swissTime = benchQuery("新版 Swiss Table SWAR", () -> {
            swissCache.isUserBanned((idx[0]++ % 10000) + 1);
        }, BENCH_OPS);

        printSpeedUp(oldTime, swissTime);
    }

    // ======================== 测试 2: 查询未命中 ========================

    @Test
    void benchmark_QueryMiss() {
        System.out.println("\n═══════════════════════════════════════════════════════════");
        System.out.println("  📊 [Test 2] 单线程查询未命中 (10K 条目, 100% 未命中)");
        System.out.println("═══════════════════════════════════════════════════════════");

        OldLinearProbeCache oldCache = new OldLinearProbeCache();
        LocalBanCache swissCache = new LocalBanCache();

        for (long i = 1; i <= 10000; i++) {
            oldCache.putUserBan(i, 3600);
            swissCache.putUserBan(i, 3600);
        }

        // 查询不存在的 UID (100001+)
        final int[] idx = {0};

        long oldTime = benchQuery("旧版 Linear Probe", () -> {
            oldCache.isUserBanned(100001L + (idx[0]++ % 10000));
        }, BENCH_OPS);

        idx[0] = 0;
        long swissTime = benchQuery("新版 Swiss Table SWAR", () -> {
            swissCache.isUserBanned(100001L + (idx[0]++ % 10000));
        }, BENCH_OPS);

        printSpeedUp(oldTime, swissTime);
    }

    // ======================== 测试 3: 写入吞吐 ========================

    @Test
    void benchmark_Write() {
        System.out.println("\n═══════════════════════════════════════════════════════════");
        System.out.println("  📊 [Test 3] 单线程写入吞吐 (新 key 插入)");
        System.out.println("═══════════════════════════════════════════════════════════");

        int writeOps = 500_000;

        // 旧版
        {
            OldLinearProbeCache cache = new OldLinearProbeCache();
            for (int w = 0; w < 50000; w++) cache.putUserBan(w + 1, 3600);

            long start = System.nanoTime();
            for (int i = 0; i < writeOps; i++) {
                cache.putUserBan(50001L + i, 3600);
            }
            long elapsed = System.nanoTime() - start;
            System.out.printf("  %-30s %7.1f ns/op  %7.2f M ops/sec%n",
                    "旧版 Linear Probe", (double) elapsed / writeOps, 1_000.0 * writeOps / elapsed);
        }

        // 新版
        {
            LocalBanCache cache = new LocalBanCache();
            for (int w = 0; w < 50000; w++) cache.putUserBan(w + 1, 3600);

            long start = System.nanoTime();
            for (int i = 0; i < writeOps; i++) {
                cache.putUserBan(50001L + i, 3600);
            }
            long elapsed = System.nanoTime() - start;
            System.out.printf("  %-30s %7.1f ns/op  %7.2f M ops/sec%n",
                    "新版 Swiss Table SWAR", (double) elapsed / writeOps, 1_000.0 * writeOps / elapsed);
        }
    }

    // ======================== 测试 4: 80%~90% 高负载因子查询与未命中测试 ========================

    @Test
    void benchmark_LoadFactor_80Percent() {
        runLoadFactorBenchmark("80% 负载因子 (52,428 条目)", (int) (65536 * 0.80));
    }

    @Test
    void benchmark_LoadFactor_85Percent() {
        runLoadFactorBenchmark("85% 负载因子 (55,705 条目)", (int) (65536 * 0.85));
    }

    @Test
    void benchmark_LoadFactor_90Percent() {
        runLoadFactorBenchmark("90% 负载因子 (58,982 条目)", (int) (65536 * 0.90));
    }

    private void runLoadFactorBenchmark(String label, int itemCapacity) {
        System.out.println("\n═══════════════════════════════════════════════════════════");
        System.out.println("  📊 [High Load Benchmark] " + label);
        System.out.println("═══════════════════════════════════════════════════════════");

        OldLinearProbeCache oldCache = new OldLinearProbeCache();
        LocalBanCache swissCache = new LocalBanCache();

        int insertedOld = 0;
        int insertedSwiss = 0;

        for (long i = 1; i <= itemCapacity; i++) {
            oldCache.putUserBan(i, 3600);
            if (oldCache.isUserBanned(i)) insertedOld++;

            swissCache.putUserBan(i, 3600);
            if (swissCache.isUserBanned(i)) insertedSwiss++;
        }

        System.out.printf("  数据装载完成: 旧版实际存入 %d/%d (%.1f%%), Swiss Table实际存入 %d/%d (%.1f%%)%n",
                insertedOld, itemCapacity, insertedOld * 100.0 / itemCapacity,
                insertedSwiss, itemCapacity, insertedSwiss * 100.0 / itemCapacity);

        // 1. 命中查询
        final int[] idxHit = {0};
        System.out.println("\n  --- 100% 命中查询 ---");
        long oldHitTime = benchQuery("旧版 Linear Probe (Hit)", () -> {
            oldCache.isUserBanned((idxHit[0]++ % itemCapacity) + 1);
        }, BENCH_OPS);

        idxHit[0] = 0;
        long swissHitTime = benchQuery("新版 Swiss Table SWAR (Hit)", () -> {
            swissCache.isUserBanned((idxHit[0]++ % itemCapacity) + 1);
        }, BENCH_OPS);
        printSpeedUp(oldHitTime, swissHitTime);

        // 2. 未命中查询 (未封禁 UID)
        final int[] idxMiss = {0};
        System.out.println("\n  --- 100% 未命中查询 ---");
        long oldMissTime = benchQuery("旧版 Linear Probe (Miss)", () -> {
            oldCache.isUserBanned(1_000_000L + (idxMiss[0]++ % itemCapacity));
        }, BENCH_OPS);

        idxMiss[0] = 0;
        long swissMissTime = benchQuery("新版 Swiss Table SWAR (Miss)", () -> {
            swissCache.isUserBanned(1_000_000L + (idxMiss[0]++ % itemCapacity));
        }, BENCH_OPS);
        printSpeedUp(oldMissTime, swissMissTime);
    }

    // ======================== 测试 5: 多线程并发混合读写 ========================

    @Test
    void benchmark_ConcurrentMixed() throws InterruptedException {
        System.out.println("\n═══════════════════════════════════════════════════════════");
        System.out.println("  📊 [Test 5] 多线程并发混合读写 (8线程, 90%读 10%写)");
        System.out.println("═══════════════════════════════════════════════════════════");

        int threads = 8;
        int opsPerThread = 500_000;

        // 旧版
        {
            OldLinearProbeCache cache = new OldLinearProbeCache();
            for (long i = 1; i <= 10000; i++) cache.putUserBan(i, 3600);

            long elapsed = runConcurrentBench(threads, opsPerThread, (tid, i) -> {
                long uid = (tid * 100000L) + (i % 10000) + 1;
                if (i % 10 == 0) {
                    cache.putUserBan(uid, 3600);
                } else {
                    cache.isUserBanned(uid);
                }
            });
            long totalOps = (long) threads * opsPerThread;
            System.out.printf("  %-30s %7.1f ns/op  %7.2f M ops/sec (总 %dM ops)%n",
                    "旧版 Linear Probe",
                    (double) elapsed / totalOps,
                    1_000.0 * totalOps / elapsed,
                    totalOps / 1_000_000);
        }

        // 新版
        {
            LocalBanCache cache = new LocalBanCache();
            for (long i = 1; i <= 10000; i++) cache.putUserBan(i, 3600);

            long elapsed = runConcurrentBench(threads, opsPerThread, (tid, i) -> {
                long uid = (tid * 100000L) + (i % 10000) + 1;
                if (i % 10 == 0) {
                    cache.putUserBan(uid, 3600);
                } else {
                    cache.isUserBanned(uid);
                }
            });
            long totalOps = (long) threads * opsPerThread;
            System.out.printf("  %-30s %7.1f ns/op  %7.2f M ops/sec (总 %dM ops)%n",
                    "新版 Swiss Table SWAR",
                    (double) elapsed / totalOps,
                    1_000.0 * totalOps / elapsed,
                    totalOps / 1_000_000);
        }
    }

    // ======================== 工具方法 ========================

    @FunctionalInterface
    interface BenchOp {
        void run(int threadId, int iteration);
    }

    private long runConcurrentBench(int threads, int opsPerThread, BenchOp op) throws InterruptedException {
        // 预热
        for (int i = 0; i < 50000; i++) op.run(0, i);

        CountDownLatch startLatch = new CountDownLatch(1);
        CountDownLatch doneLatch = new CountDownLatch(threads);
        AtomicLong totalElapsed = new AtomicLong(0);

        for (int t = 0; t < threads; t++) {
            final int tid = t;
            new Thread(() -> {
                try {
                    startLatch.await();
                } catch (InterruptedException e) {
                    return;
                }
                long start = System.nanoTime();
                for (int i = 0; i < opsPerThread; i++) {
                    op.run(tid, i);
                }
                totalElapsed.addAndGet(System.nanoTime() - start);
                doneLatch.countDown();
            }).start();
        }

        startLatch.countDown();
        doneLatch.await();

        // 返回墙钟时间 (取最大线程耗时的近似 = 总耗时/线程数)
        return totalElapsed.get() / threads;
    }

    private void printSpeedUp(long oldNs, long swissNs) {
        double speedUp = (double) oldNs / swissNs;
        String arrow = speedUp >= 1.0 ? "🚀" : "⚠️";
        System.out.printf("  %s 加速比: %.2fx%n", arrow, speedUp);
    }
}
