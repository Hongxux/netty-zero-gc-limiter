package com.netty.limiter.cache;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.lang.invoke.MethodHandles;
import java.lang.invoke.VarHandle;
import java.util.Arrays;
import java.util.Random;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

/**
 * 🔬 LRU 阈值 (Load Factor Threshold) 极限压测分析
 *
 * 测试目标: 探究当 Hot Table 阈值设置为 20%, 30%, 40%, 50%, 60%, 70% 时:
 * 1. 单线程查询/写入吞吐 (ns/op & ops/sec)
 * 2. 多线程并发混合读写吞吐 (8 线程)
 * 3. 轮转开销 (Rotation Overhead) 与 探查步数 (Probe Depth)
 */
class LruThresholdBenchmarkTest {

    static class ConfigurableLruCache {
        public static final int CAPACITY = 65536;
        public static final int MASK = CAPACITY - 1;
        public static final int MAX_PROBE = 16;
        public static final long EMPTY = 0L;
        public static final long TOMBSTONE = -1L;

        private final int threshold;

        public static class TableHolder {
            private static final VarHandle KEYS_VH = MethodHandles.arrayElementVarHandle(long[].class);
            private static final VarHandle EXPS_VH = MethodHandles.arrayElementVarHandle(long[].class);

            public final long[] uidKeys;
            public final long[] uidExpires;
            public final AtomicInteger count = new AtomicInteger(0);

            public TableHolder(int capacity) {
                this.uidKeys = new long[capacity];
                this.uidExpires = new long[capacity];
            }

            public void clear() {
                Arrays.fill(uidKeys, EMPTY);
                VarHandle.storeStoreFence();
                Arrays.fill(uidExpires, 0L);
                count.set(0);
            }

            public long getKeyAcquire(int idx) { return (long) KEYS_VH.getAcquire(uidKeys, idx); }
            public void setKeyRelease(int idx, long key) { KEYS_VH.setRelease(uidKeys, idx, key); }
            public boolean casKey(int idx, long expected, long newKey) { return KEYS_VH.compareAndSet(uidKeys, idx, expected, newKey); }
            public long getExpAcquire(int idx) { return (long) EXPS_VH.getAcquire(uidExpires, idx); }
            public void setExpRelease(int idx, long exp) { EXPS_VH.setRelease(uidExpires, idx, exp); }
        }

        public static class TablePair {
            public final TableHolder hot;
            public final TableHolder cold;
            public TablePair(TableHolder hot, TableHolder cold) {
                this.hot = hot;
                this.cold = cold;
            }
        }

        private static final VarHandle TABLES_VH;
        static {
            try {
                TABLES_VH = MethodHandles.lookup().findVarHandle(ConfigurableLruCache.class, "tables", TablePair.class);
            } catch (ReflectiveOperationException e) {
                throw new ExceptionInInitializerError(e);
            }
        }

        private final TableHolder tableA = new TableHolder(CAPACITY);
        private final TableHolder tableB = new TableHolder(CAPACITY);
        private final TablePair pairAB = new TablePair(tableA, tableB);
        private final TablePair pairBA = new TablePair(tableB, tableA);

        private TablePair tables = pairAB;
        private final AtomicBoolean rotating = new AtomicBoolean(false);
        public final AtomicLong rotationCount = new AtomicLong(0);

        public ConfigurableLruCache(double thresholdRatio) {
            this.threshold = (int) (CAPACITY * thresholdRatio);
        }

        private TablePair getTablesAcquire() { return (TablePair) TABLES_VH.getAcquire(this); }
        private void setTablesRelease(TablePair newPair) { TABLES_VH.setRelease(this, newPair); }

        public boolean isUserBanned(long userId) {
            if (userId <= 0) return false;
            int baseIndex = (int) (mixHash(userId) & MASK);
            long now = System.currentTimeMillis();

            TablePair pair = getTablesAcquire();
            TableHolder h = pair.hot;

            for (int i = 0; i < MAX_PROBE; i++) {
                int index = (baseIndex + i) & MASK;
                long storedUid = h.getKeyAcquire(index);
                if (storedUid == EMPTY) break;
                if (storedUid == userId) {
                    long expireTime = h.getExpAcquire(index);
                    if (expireTime > now) return true;
                    h.casKey(index, userId, TOMBSTONE);
                    return false;
                }
            }

            TableHolder c = pair.cold;
            for (int i = 0; i < MAX_PROBE; i++) {
                int index = (baseIndex + i) & MASK;
                long storedUid = c.getKeyAcquire(index);
                if (storedUid == EMPTY) break;
                if (storedUid == userId) {
                    long expireTime = c.getExpAcquire(index);
                    if (expireTime > now) {
                        if (c.casKey(index, userId, TOMBSTONE)) c.setExpRelease(index, 0L);
                        putUserBan(userId, (expireTime - now) / 1000L);
                        return true;
                    } else {
                        c.casKey(index, userId, TOMBSTONE);
                        return false;
                    }
                }
            }
            return false;
        }

        public void putUserBan(long userId, long durationSeconds) {
            if (userId <= 0) return;
            long now = System.currentTimeMillis();
            long expireTime = now + (durationSeconds * 1000L);

            TableHolder h = getTablesAcquire().hot;
            if (h.count.get() >= threshold) {
                checkAndRotateTables();
                h = getTablesAcquire().hot;
            }

            int baseIndex = (int) (mixHash(userId) & MASK);
            for (int i = 0; i < MAX_PROBE; i++) {
                int index = (baseIndex + i) & MASK;
                long storedUid = h.getKeyAcquire(index);
                if (storedUid == userId) {
                    h.setExpRelease(index, expireTime);
                    return;
                }
                if (storedUid == EMPTY || storedUid == TOMBSTONE) {
                    if (h.casKey(index, storedUid, userId)) {
                        h.setExpRelease(index, expireTime);
                        h.count.incrementAndGet();
                        return;
                    }
                    storedUid = h.getKeyAcquire(index);
                    if (storedUid == userId) {
                        h.setExpRelease(index, expireTime);
                        return;
                    }
                }
            }
            int idx = baseIndex & MASK;
            if (h.casKey(idx, h.getKeyAcquire(idx), userId)) {
                h.setExpRelease(idx, expireTime);
            } else {
                h.setKeyRelease(idx, userId);
                h.setExpRelease(idx, expireTime);
            }
        }

        private void checkAndRotateTables() {
            if (rotating.compareAndSet(false, true)) {
                try {
                    TablePair currentPair = getTablesAcquire();
                    if (currentPair.hot.count.get() >= threshold) {
                        TableHolder oldCold = currentPair.cold;
                        oldCold.clear();
                        setTablesRelease((currentPair == pairAB) ? pairBA : pairAB);
                        rotationCount.incrementAndGet();
                    }
                } finally {
                    rotating.set(false);
                }
            }
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

    @Test
    @DisplayName("🔥 压测不同 LRU 阈值下的性能表现")
    void testLruThresholds() throws InterruptedException {
        double[] thresholds = { 0.20, 0.30, 0.40, 0.50, 0.60, 0.70 };

        System.out.println("==================================================================================================");
        System.out.println(" 🔬 粗粒度 LRU Hot Table 阈值 (Load Factor Threshold) 极限压测报告");
        System.out.println("==================================================================================================");
        System.out.printf("%-10s | %-16s | %-16s | %-18s | %-14s%n",
                "阈值 Ratio", "单线程读(ns/op)", "并发8线程(ops/sec)", "并发延迟(ns/op)", "触发轮转次数");
        System.out.println("--------------------------------------------------------------------------------------------------");

        for (double ratio : thresholds) {
            ConfigurableLruCache cache = new ConfigurableLruCache(ratio);

            // 1. 动态生成 100,000 个不同的黑名单 Key
            Random rand = new Random(42);
            long[] keys = new long[100_000];
            for (int i = 0; i < keys.length; i++) {
                keys[i] = Math.abs(rand.nextLong() % 10_000_000L) + 1;
            }

            // 预热与预填充
            for (int i = 0; i < 50_000; i++) {
                cache.putUserBan(keys[i], 3600);
            }

            // 2. 单线程读测试 (1M 次)
            long startRead = System.nanoTime();
            int iterations = 1_000_000;
            for (int i = 0; i < iterations; i++) {
                cache.isUserBanned(keys[i % keys.length]);
            }
            long endRead = System.nanoTime();
            double readNsPerOp = (double) (endRead - startRead) / iterations;

            // 3. 多线程高频并发读写 (8 线程, 30% 写入比例以大量触发轮转)
            int threads = 8;
            int opsPerThread = 500_000;
            CountDownLatch latch = new CountDownLatch(threads);
            long startConcurrent = System.nanoTime();

            for (int t = 0; t < threads; t++) {
                final int threadId = t;
                new Thread(() -> {
                    Random tr = new Random(threadId * 100L);
                    for (int i = 0; i < opsPerThread; i++) {
                        long key = keys[Math.abs(tr.nextInt()) % keys.length];
                        if (i % 3 == 0) { // 33% 写入率，强力触发轮转
                            cache.putUserBan(key, 3600);
                        } else {
                            cache.isUserBanned(key);
                        }
                    }
                    latch.countDown();
                }).start();
            }

            latch.await();
            long endConcurrent = System.nanoTime();

            long totalOps = (long) threads * opsPerThread;
            double totalSec = (endConcurrent - startConcurrent) / 1_000_000_000.0;
            double opsPerSec = totalOps / totalSec;
            double concurrentNsPerOp = (endConcurrent - startConcurrent) / (double) totalOps;

            System.out.printf("  %-8.0f%% | %-14.2f ns | %-14.2f M | %-16.2f ns | %-12d%n",
                    ratio * 100, readNsPerOp, opsPerSec / 1_000_000.0, concurrentNsPerOp, cache.rotationCount.get());
        }
        System.out.println("==================================================================================================");
    }
}
