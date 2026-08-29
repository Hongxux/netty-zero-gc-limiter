package com.netty.limiter.limiter;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicLong;
import static org.junit.jupiter.api.Assertions.*;

public class RateLimiterRealTrafficTest {

    @Test
    @DisplayName("测试真实高并发生产流量下的单机 QPS 吞吐量与速率自适应表现")
    public void testRealTrafficPerformance() throws InterruptedException {
        // 初始化 Master 全局物理桶：容量 500,000 QPS，填充速率 500,000 /s
        LocalGlobalRateLimiter.GlobalTokenBucket bucket = new LocalGlobalRateLimiter.GlobalTokenBucket(500000, 500000);

        int threadCount = 16;
        int requestsPerThread = 50000;
        ExecutorService executor = Executors.newFixedThreadPool(threadCount);
        CountDownLatch latch = new CountDownLatch(threadCount);

        AtomicLong passedRequests = new AtomicLong(0);
        AtomicLong rejectedRequests = new AtomicLong(0);

        long startNs = System.nanoTime();

        for (int i = 0; i < threadCount; i++) {
            executor.submit(() -> {
                LocalGlobalRateLimiter.ThreadTokenBuffer threadBuffer = new LocalGlobalRateLimiter.ThreadTokenBuffer(bucket);
                try {
                    for (int r = 0; r < requestsPerThread; r++) {
                        if (threadBuffer.tryAcquire()) {
                            passedRequests.incrementAndGet();
                        } else {
                            rejectedRequests.incrementAndGet();
                        }
                    }
                } finally {
                    latch.countDown();
                }
            });
        }

        latch.await();
        long elapsedNs = System.nanoTime() - startNs;
        double durationSec = elapsedNs / 1_000_000_000.0;
        long totalRequests = (long) threadCount * requestsPerThread;
        double qps = totalRequests / durationSec;

        System.out.println("=================================================");
        System.out.println(" 真实高并发流量实测结果 (Real Traffic Test)");
        System.out.println("=================================================");
        System.out.println(String.format("并发线程数 (EventLoop Threads) : %d", threadCount));
        System.out.println(String.format("总请求处理数 (Total Requests)   : %d", totalRequests));
        System.out.println(String.format("成功放行数 (Passed Requests)    : %d", passedRequests.get()));
        System.out.println(String.format("拦截拒绝数 (Rejected Requests)  : %d", rejectedRequests.get()));
        System.out.println(String.format("总测试耗时 (Duration)          : %.3f 秒", durationSec));
        System.out.println(String.format("单机极限 QPS (Ops/sec)          : %.2f QPS", qps));
        System.out.println("=================================================");

        assertTrue(passedRequests.get() > 0);
        executor.shutdown();
    }
}
