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
    @DisplayName("🔥 压测 JwtSigUidCache 的单线程与 16 线程并发吞吐")
    void benchmarkJwtSigUidCache() throws InterruptedException {
        JwtSigUidCache cache = JwtSigUidCache.INSTANCE;

        int count = 20_000;
        long[] hashes = new long[count];
        long[] prefixes = new long[count];
        long[] uids = new long[count];
        long nowSec = System.currentTimeMillis() / 1000 + 86400;

        Random rand = new Random(42);
        for (int i = 0; i < count; i++) {
            hashes[i] = Math.abs(rand.nextLong()) + 10;
            prefixes[i] = Math.abs(rand.nextLong()) + 10;
            uids[i] = i + 1;
            cache.put(hashes[i], prefixes[i], uids[i], nowSec);
        }

        // 1. 单线程读吞吐压测 (5,000,000 次)
        int iterations = 5_000_000;
        long startRead = System.nanoTime();
        long hitCount = 0;
        for (int i = 0; i < iterations; i++) {
            int idx = i % count;
            long uid = cache.get(hashes[idx], prefixes[idx]);
            if (uid > 0) hitCount++;
        }
        long endRead = System.nanoTime();

        double readNsPerOp = (double) (endRead - startRead) / iterations;
        double readOpsPerSec = iterations / ((endRead - startRead) / 1_000_000_000.0);

        // 2. 16 线程高并发混合读写压测 (8,000,000 次)
        int threads = 16;
        int opsPerThread = 500_000;
        CountDownLatch latch = new CountDownLatch(threads);
        long startConcurrent = System.nanoTime();

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
                latch.countDown();
            }).start();
        }

        latch.await();
        long endConcurrent = System.nanoTime();

        long totalOps = (long) threads * opsPerThread;
        double totalSec = (endConcurrent - startConcurrent) / 1_000_000_000.0;
        double concurrentOpsPerSec = totalOps / totalSec;
        double concurrentNsPerOp = (endConcurrent - startConcurrent) / (double) totalOps;

        System.out.println("==================================================================================================");
        System.out.println(" 🚀 JwtSigUidCache (Bit-Packing + Interleaved Cache Line) 极限吞吐测试报告");
        System.out.println("==================================================================================================");
        System.out.printf("  单线程查询吞吐   : %10.2f ns/op  |  %10.2f M ops/sec  (Hits: %d/%d)%n",
                readNsPerOp, readOpsPerSec / 1_000_000.0, hitCount, iterations);
        System.out.printf("  16 线程并发吞吐  : %10.2f ns/op  |  %10.2f M ops/sec  (Total Ops: %d)%n",
                concurrentNsPerOp, concurrentOpsPerSec / 1_000_000.0, totalOps);
        System.out.println("==================================================================================================");
    }
}
