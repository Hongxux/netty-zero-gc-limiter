package com.netty.limiter.limiter;

import com.netty.limiter.config.GatewayRateLimitProperties;
import com.netty.limiter.util.LuaSha1Util;
import io.lettuce.core.RedisClient;
import io.lettuce.core.ScriptOutputType;
import io.lettuce.core.api.StatefulRedisConnection;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.DefaultEventLoopGroup;
import io.netty.channel.EventLoop;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.embedded.EmbeddedChannel;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 * 🔬 真实 Linux Redis 6379 环境下 Mode B 阻塞式 vs 0-GC 响应式 性能极限对决
 *
 * 场景设定：
 * - 网关拥有 16 个 Netty EventLoop Worker 线程
 * - 处理 16,000 次真实 Redis EVALSHA 限流校验
 * - 直连真实 Linux Redis 服务端 (127.0.0.1:6379)
 *
 * 方案 A (真实阻塞式 Mode B):
 *   16 个工作线程通过同步调用 commands.evalsha() 阻塞等待真实 Redis TCP Socket 回包。
 *   -> 16 个工作线程全部被真实的 TCP 网络往返和 Redis 服务端处理耗时挂起阻塞！
 *
 * 方案 B (本项目 0-GC 响应式 Mode B):
 *   16 个 Netty EventLoop 接收请求后调用自研 UserRateLimiterOperate.acquireReactiveAsync(asyncCtx)，
 *   当前 Worker 线程 0 阻塞立即释放；当真实 Redis TCP 回包到达后，由 Netty EventLoop 触发 resume。
 *   -> 16 个 EventLoop 线程利用率 100%，持续高速吞吐！
 */
public class ModeBBlockingVsReactiveBenchmarkTest {

    private static final int WORKER_THREADS = 16;
    private static final int TOTAL_OPERATIONS = 16000;

    @Test
    @DisplayName("🔥 Mode B 阻塞式 vs 0-GC 响应式 直连真实 Linux Redis 6379 全量对决")
    public void testRealRedisModeBBlockingVsReactive() throws Exception {
        String host = System.getenv().getOrDefault("REDIS_HOST", "127.0.0.1");
        int port = Integer.parseInt(System.getenv().getOrDefault("REDIS_PORT", "6379"));

        EmbeddedRealRedisServer embeddedServer = null;
        RedisClient client = null;
        StatefulRedisConnection<String, String> probe = null;
        UserRateLimiterOperate operate = null;
        try {
            client = RedisClient.create("redis://" + host + ":" + port);
            try {
                probe = client.connect();
                probe.sync().ping();
                System.out.println("🚀 [RealRedis] 成功连接到官方正版 Linux Redis 实例: " + host + ":" + port);
            } catch (Exception unavailable) {
                port = 6389;
                embeddedServer = new EmbeddedRealRedisServer(port);
                embeddedServer.start();
                Thread.sleep(500);
                client.shutdown();
                client = RedisClient.create("redis://" + host + ":" + port);
                probe = client.connect();
                probe.sync().ping();
            }

            String sha = probe.sync().scriptLoad(LuaSha1Util.DEFAULT_LUA_SCRIPT);
            probe.sync().flushdb();

            // 初始化自研 0-GC RESP2 驱动
            operate = new UserRateLimiterOperate();
            GatewayRateLimitProperties properties = new GatewayRateLimitProperties();
            properties.setRedisHost(host);
            properties.setRedisPort(port);
            var field = UserRateLimiterOperate.class.getDeclaredField("properties");
            field.setAccessible(true);
            field.set(operate, properties);
            operate.init();
            Thread.sleep(500);

            // JIT 预热
            warmup(client, operate, sha);

            System.out.println("==================================================================================================");
            System.out.println(" 🚀 Mode B 直连真实 Linux Redis 6379 极限压测 (16 线程, " + TOTAL_OPERATIONS + " 次真实 EVALSHA 校验)");
            System.out.println("==================================================================================================");

            // =========================================================================
            // 1. 真实 Redis 阻塞式 Mode B (16 线程, 逐条阻塞等待真实 Redis TCP 回包)
            // =========================================================================
            ExecutorService blockingPool = Executors.newFixedThreadPool(WORKER_THREADS);
            List<StatefulRedisConnection<String, String>> conns = new ArrayList<>();
            for (int i = 0; i < WORKER_THREADS; i++) {
                conns.add(client.connect());
            }
            CountDownLatch blockingLatch = new CountDownLatch(TOTAL_OPERATIONS);
            long[] blockingLatencies = new long[TOTAL_OPERATIONS];
            long startBlocking = System.nanoTime();

            int opsPerThread = TOTAL_OPERATIONS / WORKER_THREADS;
            for (int t = 0; t < WORKER_THREADS; t++) {
                final int threadIdx = t;
                final var commands = conns.get(t).sync();
                blockingPool.submit(() -> {
                    for (int i = 0; i < opsPerThread; i++) {
                        int idx = threadIdx * opsPerThread + i;
                        String key = "rate:modeb:block:" + threadIdx + ":" + i;
                        long t0 = System.nanoTime();
                        commands.evalsha(sha, ScriptOutputType.VALUE,
                                new String[]{key}, new String[]{"1700000000000", "1000000", "1000000", "2", "1"});
                        blockingLatencies[idx] = (System.nanoTime() - t0) / 1000L;
                        blockingLatch.countDown();
                    }
                });
            }
            blockingLatch.await();
            long endBlocking = System.nanoTime();

            double elapsedSecBlocking = (endBlocking - startBlocking) / 1_000_000_000.0;
            double qpsBlocking = TOTAL_OPERATIONS / elapsedSecBlocking;

            probe.sync().flushdb();
            probe.sync().scriptLoad(LuaSha1Util.DEFAULT_LUA_SCRIPT);

            // =========================================================================
            // 2. 自研 0-GC 响应式 Mode B (单 TCP 连接, 逐条直发 DirectFlush)
            // =========================================================================
            final UserRateLimiterOperate finalOperate = operate;
            EventLoopGroup reactiveWorkers = new DefaultEventLoopGroup(WORKER_THREADS);
            CountDownLatch reactiveLatch = new CountDownLatch(TOTAL_OPERATIONS);
            java.util.concurrent.Semaphore inFlight = new java.util.concurrent.Semaphore(256);
            long[] reactiveDirectLatencies = new long[TOTAL_OPERATIONS];
            long[] reactiveDirectStartTimes = new long[TOTAL_OPERATIONS];

            io.netty.channel.ChannelHandlerContext[] workerContexts = new io.netty.channel.ChannelHandlerContext[WORKER_THREADS];
            EventLoop[] loops = new EventLoop[WORKER_THREADS];
            for (int w = 0; w < WORKER_THREADS; w++) {
                loops[w] = reactiveWorkers.next();
                final EventLoop loop = loops[w];
                workerContexts[w] = (io.netty.channel.ChannelHandlerContext) java.lang.reflect.Proxy.newProxyInstance(
                        io.netty.channel.ChannelHandlerContext.class.getClassLoader(),
                        new Class<?>[]{io.netty.channel.ChannelHandlerContext.class},
                        (proxy, method, methodArgs) -> {
                            if ("executor".equals(method.getName())) {
                                return loop;
                            }
                            return null;
                        }
                );
            }

            ByteBuf rawBuf = Unpooled.wrappedBuffer(new byte[]{1, 2, 3});

            long startReactive = System.nanoTime();
            for (int i = 0; i < TOTAL_OPERATIONS; i++) {
                inFlight.acquire();
                int workerIdx = i % WORKER_THREADS;
                EventLoop eventLoop = loops[workerIdx];
                io.netty.channel.ChannelHandlerContext httpCtx = workerContexts[workerIdx];
                final int opIdx = i;
                final long userId = 100000L + i;

                reactiveDirectStartTimes[opIdx] = System.nanoTime();
                eventLoop.execute(() -> {
                    AsyncRateLimitContext asyncCtx = AsyncRateLimitContext.acquire(
                            httpCtx, rawBuf, userId, allowed -> {
                                reactiveDirectLatencies[opIdx] = (System.nanoTime() - reactiveDirectStartTimes[opIdx]) / 1000L;
                                inFlight.release();
                                reactiveLatch.countDown();
                            });
                    finalOperate.acquireReactiveDirect(asyncCtx, LuaSha1Util.DEFAULT_LUA_SHA1_BYTES);
                });
            }
            reactiveLatch.await();
            long endReactive = System.nanoTime();

            double elapsedSecReactive = (endReactive - startReactive) / 1_000_000_000.0;
            double qpsReactive = TOTAL_OPERATIONS / elapsedSecReactive;

            probe.sync().flushdb();
            probe.sync().scriptLoad(LuaSha1Util.DEFAULT_LUA_SCRIPT);

            // =========================================================================
            // 3. 自研 0-GC 响应式 Mode B (单 TCP 连接, 16条微攒批 Pipeline)
            // =========================================================================
            CountDownLatch batchLatch = new CountDownLatch(TOTAL_OPERATIONS);
            java.util.concurrent.Semaphore batchInFlight = new java.util.concurrent.Semaphore(256);
            final int BATCH_SIZE = 16;
            int totalBatches = TOTAL_OPERATIONS / BATCH_SIZE;
            long[] reactiveBatchLatencies = new long[TOTAL_OPERATIONS];
            long[] reactiveBatchStartTimes = new long[TOTAL_OPERATIONS];

            long startBatch = System.nanoTime();
            for (int b = 0; b < totalBatches; b++) {
                batchInFlight.acquire(BATCH_SIZE);
                int workerIdx = b % WORKER_THREADS;
                EventLoop eventLoop = loops[workerIdx];
                io.netty.channel.ChannelHandlerContext httpCtx = workerContexts[workerIdx];
                final int batchOffset = b * BATCH_SIZE;

                for (int k = 0; k < BATCH_SIZE; k++) {
                    reactiveBatchStartTimes[batchOffset + k] = System.nanoTime();
                }

                eventLoop.execute(() -> {
                    AsyncRateLimitContext[] batchCtxs = new AsyncRateLimitContext[BATCH_SIZE];
                    for (int k = 0; k < BATCH_SIZE; k++) {
                        final int opIdx = batchOffset + k;
                        long userId = 200000L + opIdx;
                        batchCtxs[k] = AsyncRateLimitContext.acquire(
                                httpCtx, rawBuf, userId, allowed -> {
                                    reactiveBatchLatencies[opIdx] = (System.nanoTime() - reactiveBatchStartTimes[opIdx]) / 1000L;
                                    batchInFlight.release();
                                    batchLatch.countDown();
                                });
                    }
                    finalOperate.acquireReactiveBatchAsync(batchCtxs, BATCH_SIZE, LuaSha1Util.DEFAULT_LUA_SHA1_BYTES);
                });
            }
            batchLatch.await();
            long endBatch = System.nanoTime();

            double elapsedSecBatch = (endBatch - startBatch) / 1_000_000_000.0;
            double qpsBatch = TOTAL_OPERATIONS / elapsedSecBatch;

            probe.sync().flushdb();
            probe.sync().scriptLoad(LuaSha1Util.DEFAULT_LUA_SCRIPT);

            System.out.printf("  1. 真实 Redis 阻塞式 Mode B (16 线程同步阻塞等待 Socket)      : %8.2f ms | %8.2f ops/sec | 线程全部阻塞%n",
                    (endBlocking - startBlocking) / 1_000_000.0, qpsBlocking);
            System.out.printf("  2. 自研 0-GC 响应式 Mode B (单 TCP 连接, 逐条直发 DirectFlush)   : %8.2f ms | %8.2f ops/sec | 0 线程阻塞%n",
                    (endReactive - startReactive) / 1_000_000.0, qpsReactive);
            System.out.printf("  3. 自研 0-GC 响应式 Mode B (单 TCP 连接, 16条微攒批 Pipeline)    : %8.2f ms | %8.2f ops/sec | 0 线程阻塞%n",
                    (endBatch - startBatch) / 1_000_000.0, qpsBatch);
            System.out.println("--------------------------------------------------------------------------------------------------");
            System.out.printf("  ⚡ 响应式微攒批 相比 阻塞式提升: %.2fx 倍 (耗时降低 %.1f%%)%n",
                    qpsBatch / qpsBlocking, (1.0 - elapsedSecBatch / elapsedSecBlocking) * 100);
            System.out.printf("  ⚡ 响应式微攒批 相比 响应式直发提升: %.2fx 倍 (耗时降低 %.1f%%)%n",
                    qpsBatch / qpsReactive, (1.0 - elapsedSecBatch / elapsedSecReactive) * 100);
            System.out.println("==================================================================================================");
            System.out.println(" 📊 端到端耗时百分位数统计 (Latency Percentiles Matrix):");
            printLatencyStats("1. 真实 Redis 阻塞式 Mode B (16 线程阻塞)", blockingLatencies);
            printLatencyStats("2. 自研 0-GC 响应式直发 (单连接直发)", reactiveDirectLatencies);
            printLatencyStats("3. 自研 0-GC 响应式微攒批 (16条 Pipeline)", reactiveBatchLatencies);
            System.out.println("==================================================================================================");

            blockingPool.shutdown();
            conns.forEach(StatefulRedisConnection::close);
            reactiveWorkers.shutdownGracefully();
        } finally {
            if (operate != null) operate.destroy();
            if (probe != null) probe.close();
            if (client != null) client.shutdown();
            if (embeddedServer != null) embeddedServer.stop();
        }
    }

    private void printLatencyStats(String name, long[] latenciesUs) {
        java.util.Arrays.sort(latenciesUs);
        long sum = 0;
        for (long l : latenciesUs) sum += l;
        double avg = sum / (double) latenciesUs.length;
        long p50 = latenciesUs[(int) (latenciesUs.length * 0.50)];
        long p90 = latenciesUs[(int) (latenciesUs.length * 0.90)];
        long p99 = latenciesUs[(int) (latenciesUs.length * 0.99)];
        long p999 = latenciesUs[(int) (latenciesUs.length * 0.999)];
        long max = latenciesUs[latenciesUs.length - 1];

        System.out.printf("  %-38s: Avg=%.2f ms | P50=%.2f ms | P90=%.2f ms | P99=%.2f ms | P99.9=%.2f ms | Max=%.2f ms%n",
                name, avg / 1000.0, p50 / 1000.0, p90 / 1000.0, p99 / 1000.0, p999 / 1000.0, max / 1000.0);
    }

    private void warmup(RedisClient client, UserRateLimiterOperate operate, String sha) throws Exception {
        var conn = client.connect();
        for (int i = 0; i < 200; i++) {
            conn.sync().evalsha(sha, ScriptOutputType.VALUE,
                    new String[]{"rate:warmup:" + i}, new String[]{"1700000000000", "1000000", "1000000", "2", "1"});
        }
        conn.close();
    }
}
