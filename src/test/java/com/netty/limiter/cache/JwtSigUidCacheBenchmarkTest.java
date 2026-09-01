package com.netty.limiter.cache;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.Random;
import java.util.concurrent.CountDownLatch;

/**
 * 🔬 JwtSigUidCache (64-bit Bit-Packing + 24-Byte Interleaved Cache Line) 极限性能基准测试
 */
class JwtSigUidCacheBenchmarkTest {

    @Test
    @DisplayName("🔥 压测 JwtSigUidCache vs ConcurrentHashMap 对比基准 (单线程 & 16 线程)")
    void benchmarkJwtSigUidCache() throws InterruptedException {
        JwtSigUidCache cache = JwtSigUidCache.INSTANCE;
        java.util.concurrent.ConcurrentHashMap<String, Long> chmString = new java.util.concurrent.ConcurrentHashMap<>(65536);
        java.util.concurrent.ConcurrentHashMap<Long, Long> chmLong = new java.util.concurrent.ConcurrentHashMap<>(65536);

        int count = 20_000;
        long[] hashes = new long[count];
        long[] prefixes = new long[count];
        long[] uids = new long[count];
        String[] tokenSignatures = new String[count];
        long nowSec = System.currentTimeMillis() / 1000 + 86400;

        Random rand = new Random(42);
        for (int i = 0; i < count; i++) {
            hashes[i] = Math.abs(rand.nextLong()) + 10;
            prefixes[i] = Math.abs(rand.nextLong()) + 10;
            uids[i] = i + 1;
            tokenSignatures[i] = "eyJhbGciOiJIUzI1NiJ9.eyJzdWIiOiIxMDA4NiJ9." + Long.toHexString(hashes[i]) + Long.toHexString(prefixes[i]);

            cache.put(hashes[i], prefixes[i], uids[i], nowSec);
            chmString.put(tokenSignatures[i], uids[i]);
            chmLong.put(hashes[i], uids[i]);
        }

        // =========================================================================
        // 1. 16 线程高并发读写对比压测 (8,000,000 次，90% 读 + 10% 写)
        // =========================================================================
        int threads = 16;
        int opsPerThread = 500_000;
        long totalOps = (long) threads * opsPerThread;

        // --- Benchmark 1: ConcurrentHashMap<String, Long> ---
        CountDownLatch latch1 = new CountDownLatch(threads);
        long startChmString = System.nanoTime();
        for (int t = 0; t < threads; t++) {
            final int threadId = t;
            new Thread(() -> {
                Random tr = new Random(threadId * 100L);
                for (int i = 0; i < opsPerThread; i++) {
                    int idx = Math.abs(tr.nextInt()) % count;
                    if (i % 10 == 0) {
                        chmString.put(tokenSignatures[idx], uids[idx]);
                    } else {
                        chmString.get(tokenSignatures[idx]);
                    }
                }
                latch1.countDown();
            }).start();
        }
        latch1.await();
        long endChmString = System.nanoTime();
        double chmStringOpsPerSec = totalOps / ((endChmString - startChmString) / 1_000_000_000.0);
        double chmStringNsPerOp = (endChmString - startChmString) / (double) totalOps;

        // --- Benchmark 2: ConcurrentHashMap<Long, Long> ---
        CountDownLatch latch2 = new CountDownLatch(threads);
        long startChmLong = System.nanoTime();
        for (int t = 0; t < threads; t++) {
            final int threadId = t;
            new Thread(() -> {
                Random tr = new Random(threadId * 100L);
                for (int i = 0; i < opsPerThread; i++) {
                    int idx = Math.abs(tr.nextInt()) % count;
                    if (i % 10 == 0) {
                        chmLong.put(hashes[idx], uids[idx]);
                    } else {
                        chmLong.get(hashes[idx]);
                    }
                }
                latch2.countDown();
            }).start();
        }
        latch2.await();
        long endChmLong = System.nanoTime();
        double chmLongOpsPerSec = totalOps / ((endChmLong - startChmLong) / 1_000_000_000.0);
        double chmLongNsPerOp = (endChmLong - startChmLong) / (double) totalOps;

        // --- Benchmark 3: JwtSigUidCache (0-GC Bit-Packing + Interleaved Cache Line) ---
        CountDownLatch latch3 = new CountDownLatch(threads);
        long startCache = System.nanoTime();
        for (int t = 0; t < threads; t++) {
            final int threadId = t;
            new Thread(() -> {
                Random tr = new Random(threadId * 100L);
                for (int i = 0; i < opsPerThread; i++) {
                    int idx = Math.abs(tr.nextInt()) % count;
                    if (i % 10 == 0) {
                        cache.put(hashes[idx], prefixes[idx], uids[idx], nowSec);
                    } else {
                        cache.get(hashes[idx], prefixes[idx]);
                    }
                }
                latch3.countDown();
            }).start();
        }
        latch3.await();
        long endCache = System.nanoTime();
        double cacheOpsPerSec = totalOps / ((endCache - startCache) / 1_000_000_000.0);
        double cacheNsPerOp = (endCache - startCache) / (double) totalOps;

        System.out.println("==================================================================================================");
        System.out.println(" 🚀 16 线程高并发读写基准横向对比 (8,000,000 Ops, 90% 读 + 10% 写)");
        System.out.println("==================================================================================================");
        System.out.printf("  1. ConcurrentHashMap<String, Long> (业界常规)   : %8.2f ns/op | %8.2f M ops/sec | 耗时: %7.2f ms%n",
                chmStringNsPerOp, chmStringOpsPerSec / 1_000_000.0, (endChmString - startChmString) / 1_000_000.0);
        System.out.printf("  2. ConcurrentHashMap<Long, Long>   (装箱包装)   : %8.2f ns/op | %8.2f M ops/sec | 耗时: %7.2f ms%n",
                chmLongNsPerOp, chmLongOpsPerSec / 1_000_000.0, (endChmLong - startChmLong) / 1_000_000.0);
        System.out.printf("  3. JwtSigUidCache                  (0-GC极致交错): %8.2f ns/op | %8.2f M ops/sec | 耗时: %7.2f ms%n",
                cacheNsPerOp, cacheOpsPerSec / 1_000_000.0, (endCache - startCache) / 1_000_000.0);
        System.out.println("--------------------------------------------------------------------------------------------------");
        System.out.printf("  ⚡ 相对常规 CHM<String, Long> 性能提升倍数: %.2fx 倍速度 (耗时下降 %.1f%%)%n",
                cacheOpsPerSec / chmStringOpsPerSec, (1.0 - cacheNsPerOp / chmStringNsPerOp) * 100);
        System.out.printf("  ⚡ 相对 CHM<Long, Long> 吞吐提升倍数       : %.2fx 倍速度 (耗时下降 %.1f%%)%n",
                cacheOpsPerSec / chmLongOpsPerSec, (1.0 - cacheNsPerOp / chmLongNsPerOp) * 100);
        System.out.println("==================================================================================================");
    }
}
