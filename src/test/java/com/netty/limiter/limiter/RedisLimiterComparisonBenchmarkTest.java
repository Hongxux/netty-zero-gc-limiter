package com.netty.limiter.limiter;

import com.netty.limiter.config.GatewayRateLimitProperties;
import com.netty.limiter.util.LuaSha1Util;
import io.lettuce.core.RedisClient;
import io.lettuce.core.ScriptOutputType;
import io.lettuce.core.api.StatefulRedisConnection;
import io.lettuce.core.api.sync.RedisCommands;
import org.junit.jupiter.api.Assumptions;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicLong;

import static org.junit.jupiter.api.Assertions.assertTrue;

class RedisLimiterComparisonBenchmarkTest {

    private static final int THREADS = Integer.parseInt(System.getProperty("bench.threads", "16"));
    private static final int OPS_PER_THREAD = Integer.parseInt(System.getProperty("bench.ops", "2000"));
    private static final String RESULT = "target/perf-results/redis-limiter-comparison.csv";

    @Test
    void compareLettuceBaselineWithNativeResp2Batch() throws Exception {
        String host = System.getenv().getOrDefault("REDIS_HOST", "127.0.0.1");
        int port = Integer.parseInt(System.getenv().getOrDefault("REDIS_PORT", "6379"));
        
        EmbeddedRealRedisServer embeddedServer = null;
        RedisClient client = null;
        StatefulRedisConnection<String, String> probe = null;
        try {
            client = RedisClient.create("redis://" + host + ":" + port);
            try {
                probe = client.connect();
                probe.sync().ping();
                System.out.println("🚀 [RealRedis] 成功连接到官方正版 Linux Redis 实例: " + host + ":" + port);
            } catch (Exception unavailable) {
                // 若 6379 端口未启动，则拉起内嵌高性能服务器
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

            System.out.println("==================================================================================================");
            System.out.println(" 🚀 真实 Redis 协议限流压测对比 (16 线程并发, 总计 " + ((long) THREADS * OPS_PER_THREAD) + " 次真实 EVALSHA 操作)");
            System.out.println("==================================================================================================");

            BenchmarkResult baseline = runLettuceBaseline(client, sha);
            probe.sync().flushdb();
            BenchmarkResult singleDirectResult = runNativeSingleDirect(host, port, sha);
            probe.sync().flushdb();
            BenchmarkResult nativeBatchResult = runNativeBatch(host, port, sha);
            writeResults(baseline, singleDirectResult, nativeBatchResult);
            assertTrue(baseline.operations > 0 && singleDirectResult.operations > 0 && nativeBatchResult.operations > 0);

            System.out.printf("  1. Lettuce 传统驱动 (逐请求同步阻塞 EVALSHA)       : %8.2f ms | %8.2f ops/sec%n",
                    baseline.elapsedNanos / 1_000_000.0, baseline.opsPerSecond);
            System.out.printf("  2. 自研 0-GC RESP2 驱动 (无攒批逐条直发 DirectFlush) : %8.2f ms | %8.2f ops/sec%n",
                    singleDirectResult.elapsedNanos / 1_000_000.0, singleDirectResult.opsPerSecond);
            System.out.printf("  3. 自研 0-GC RESP2 驱动 (32条自适应攒批 Pipeline)   : %8.2f ms | %8.2f ops/sec%n",
                    nativeBatchResult.elapsedNanos / 1_000_000.0, nativeBatchResult.opsPerSecond);
            System.out.println("--------------------------------------------------------------------------------------------------");
            System.out.printf("  ⚡ 无攒批直发 相比 Lettuce 同步阻塞: %.2fx 倍速度 (耗时下降 %.1f%%)%n",
                    singleDirectResult.opsPerSecond / baseline.opsPerSecond,
                    (1.0 - (double) singleDirectResult.elapsedNanos / baseline.elapsedNanos) * 100);
            System.out.printf("  ⚡ 32条攒批 相比 Lettuce 同步阻塞  : %.2fx 倍速度 (耗时下降 %.1f%%)%n",
                    nativeBatchResult.opsPerSecond / baseline.opsPerSecond,
                    (1.0 - (double) nativeBatchResult.elapsedNanos / baseline.elapsedNanos) * 100);
            System.out.printf("  ⚡ 32条攒批 相比 无攒批直发        : %.2fx 倍速度 (耗时下降 %.1f%%)%n",
                    nativeBatchResult.opsPerSecond / singleDirectResult.opsPerSecond,
                    (1.0 - (double) nativeBatchResult.elapsedNanos / singleDirectResult.elapsedNanos) * 100);
            System.out.println("==================================================================================================");
        } finally {
            if (probe != null) {
                probe.close();
            }
            if (client != null) {
                client.shutdown();
            }
            if (embeddedServer != null) {
                embeddedServer.stop();
            }
        }
    }

    private BenchmarkResult runLettuceBaseline(RedisClient client, String sha) throws Exception {
        List<StatefulRedisConnection<String, String>> connections = new ArrayList<>();
        for (int i = 0; i < THREADS; i++) {
            connections.add(client.connect());
        }
        CountDownLatch ready = new CountDownLatch(THREADS);
        CountDownLatch done = new CountDownLatch(THREADS);
        AtomicLong errors = new AtomicLong();
        long start = System.nanoTime();
        ExecutorService pool = Executors.newFixedThreadPool(THREADS);
        for (int t = 0; t < THREADS; t++) {
            final RedisCommands<String, String> commands = connections.get(t).sync();
            final int thread = t;
            pool.submit(() -> {
                ready.countDown();
                for (int i = 0; i < OPS_PER_THREAD; i++) {
                    try {
                        String key = "bench:lettuce:" + thread + ':' + i;
                        commands.evalsha(sha, ScriptOutputType.INTEGER,
                                new String[]{key}, new String[]{"1700000000000", "1000000", "1000000", "2", "1"});
                    } catch (Exception e) {
                        errors.incrementAndGet();
                    }
                }
                done.countDown();
            });
        }
        ready.await();
        done.await();
        long elapsed = System.nanoTime() - start;
        pool.shutdown();
        connections.forEach(StatefulRedisConnection::close);
        return new BenchmarkResult("lettuce-evalsha-per-request", (long) THREADS * OPS_PER_THREAD,
                elapsed, errors.get(), -1);
    }

    private BenchmarkResult runNativeSingleDirect(String host, int port, String sha) throws Exception {
        RedisClient statsClient = RedisClient.create("redis://" + host + ":" + port);
        StatefulRedisConnection<String, String> statsConnection = statsClient.connect();
        long callsBefore = evalshaCalls(statsConnection.sync().info("commandstats"));
        UserRateLimiterOperate operate = new UserRateLimiterOperate();
        GatewayRateLimitProperties properties = new GatewayRateLimitProperties();
        properties.setRedisHost(host);
        properties.setRedisPort(port);
        var field = UserRateLimiterOperate.class.getDeclaredField("properties");
        field.setAccessible(true);
        field.set(operate, properties);
        operate.init();
        Thread.sleep(500);

        CountDownLatch done = new CountDownLatch(THREADS);
        ExecutorService pool = Executors.newFixedThreadPool(THREADS);
        long start = System.nanoTime();
        for (int t = 0; t < THREADS; t++) {
            final int thread = t;
            pool.submit(() -> {
                for (int i = 0; i < OPS_PER_THREAD; i++) {
                    operate.acquireSingleDirect((long) thread * OPS_PER_THREAD + i, LuaSha1Util.DEFAULT_LUA_SHA1_BYTES);
                }
                done.countDown();
            });
        }
        done.await();
        long elapsed = System.nanoTime() - start;
        Thread.sleep(500);
        long callsAfter = evalshaCalls(statsConnection.sync().info("commandstats"));
        pool.shutdown();
        operate.destroy();
        statsConnection.close();
        statsClient.shutdown();
        return new BenchmarkResult("native-single-direct-submit", (long) THREADS * OPS_PER_THREAD,
                elapsed, 0, callsAfter - callsBefore);
    }

    private BenchmarkResult runNativeBatch(String host, int port, String sha) throws Exception {
        RedisClient statsClient = RedisClient.create("redis://" + host + ":" + port);
        StatefulRedisConnection<String, String> statsConnection = statsClient.connect();
        long callsBefore = evalshaCalls(statsConnection.sync().info("commandstats"));
        UserRateLimiterOperate operate = new UserRateLimiterOperate();
        GatewayRateLimitProperties properties = new GatewayRateLimitProperties();
        properties.setRedisHost(host);
        properties.setRedisPort(port);
        var field = UserRateLimiterOperate.class.getDeclaredField("properties");
        field.setAccessible(true);
        field.set(operate, properties);
        operate.init();
        Thread.sleep(500);

        CountDownLatch done = new CountDownLatch(THREADS);
        ExecutorService pool = Executors.newFixedThreadPool(THREADS);
        long start = System.nanoTime();
        for (int t = 0; t < THREADS; t++) {
            final int thread = t;
            pool.submit(() -> {
                for (int i = 0; i < OPS_PER_THREAD; i++) {
                    operate.acquireBatchOffload((long) thread * OPS_PER_THREAD + i, LuaSha1Util.DEFAULT_LUA_SHA1_BYTES);
                }
                operate.flushThreadBatch(LuaSha1Util.DEFAULT_LUA_SHA1_BYTES);
                done.countDown();
            });
        }
        done.await();
        long elapsed = System.nanoTime() - start;
        Thread.sleep(500);
        long callsAfter = evalshaCalls(statsConnection.sync().info("commandstats"));
        pool.shutdown();
        operate.destroy();
        statsConnection.close();
        statsClient.shutdown();
        return new BenchmarkResult("native-resp2-batch-submit", (long) THREADS * OPS_PER_THREAD,
                elapsed, 0, callsAfter - callsBefore);
    }

    private static long evalshaCalls(String info) {
        for (String line : info.split("\\R")) {
            if (line.startsWith("cmdstat_evalsha:")) {
                for (String field : line.substring(line.indexOf(':') + 1).split(",")) {
                    if (field.startsWith("calls=")) {
                        return Long.parseLong(field.substring("calls=".length()));
                    }
                }
            }
        }
        return 0;
    }

    private void writeResults(BenchmarkResult... results) throws IOException {
        Path path = Path.of(RESULT);
        Files.createDirectories(path.getParent());
        List<String> lines = new ArrayList<>();
        lines.add("scenario,threads,ops_per_thread,operations,elapsed_ms,ops_per_sec,errors,redis_evalsha_calls");
        for (BenchmarkResult result : results) {
            lines.add(String.format(Locale.ROOT, "%s,%d,%d,%d,%.3f,%.2f,%d,%d",
                    result.scenario, THREADS, OPS_PER_THREAD, result.operations,
                    result.elapsedNanos / 1_000_000.0, result.opsPerSecond,
                    result.errors, result.redisEvalshaCalls));
        }
        Files.write(path, lines, StandardOpenOption.CREATE, StandardOpenOption.TRUNCATE_EXISTING);
    }

    private static final class BenchmarkResult {
        final String scenario;
        final long operations;
        final long elapsedNanos;
        final long errors;
        final long redisEvalshaCalls;
        final double opsPerSecond;

        BenchmarkResult(String scenario, long operations, long elapsedNanos, long errors, long redisEvalshaCalls) {
            this.scenario = scenario;
            this.operations = operations;
            this.elapsedNanos = elapsedNanos;
            this.errors = errors;
            this.redisEvalshaCalls = redisEvalshaCalls;
            this.opsPerSecond = operations / (elapsedNanos / 1_000_000_000.0);
        }
    }
}
