package com.netty.limiter.limiter;

import com.netty.limiter.util.LuaSha1Util;
import io.lettuce.core.RedisClient;
import io.lettuce.core.ScriptOutputType;
import io.lettuce.core.api.StatefulRedisConnection;
import io.lettuce.core.api.sync.RedisCommands;
import io.lettuce.core.pubsub.RedisPubSubAdapter;
import io.lettuce.core.pubsub.StatefulRedisPubSubConnection;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Assumptions;

import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

class RealRedisRateLimiterIntegrationTest {

    private static final String CHANNEL = "NETTY_LIMITER_BAN_CHANNEL";

    @Test
    void executesTokenBucketAndPublishesBanEventOnRealRedis() throws Exception {
        String host = System.getenv().getOrDefault("REDIS_HOST", "127.0.0.1");
        int port = Integer.parseInt(System.getenv().getOrDefault("REDIS_PORT", "6379"));
        RedisClient client = RedisClient.create("redis://" + host + ":" + port);
        StatefulRedisConnection<String, String> connection = null;
        StatefulRedisPubSubConnection<String, String> pubSub = null;
        try {
            try {
                connection = client.connect();
            } catch (Exception unavailable) {
                Assumptions.assumeTrue(false, "Redis unavailable at " + host + ":" + port
                        + "; run scripts/run-real-redis-validation.ps1");
                return;
            }
            RedisCommands<String, String> commands = connection.sync();
            assertEquals("PONG", commands.ping());
            String sha = commands.scriptLoad(LuaSha1Util.DEFAULT_LUA_SCRIPT);
            assertEquals(new String(LuaSha1Util.DEFAULT_LUA_SHA1_BYTES), sha);
            commands.del("rate:test:10086");

            List<String> key = List.of("rate:test:10086");
            List<String> args = List.of("1700000000000", "2", "2", "2", "1");
            Long first = commands.evalsha(sha, ScriptOutputType.INTEGER, key.toArray(new String[0]), args.toArray(new String[0]));
            Long second = commands.evalsha(sha, ScriptOutputType.INTEGER, key.toArray(new String[0]), args.toArray(new String[0]));
            assertEquals(1L, first);
            assertEquals(1L, second);

            CountDownLatch banEvent = new CountDownLatch(1);
            pubSub = client.connectPubSub();
            pubSub.addListener(new RedisPubSubAdapter<String, String>() {
                @Override
                public void message(String channel, String message) {
                    if (CHANNEL.equals(channel) && message.contains("rate:test:10086")) {
                        banEvent.countDown();
                    }
                }
            });
            pubSub.sync().subscribe(CHANNEL);
            Thread.sleep(100);

            Long rejected = commands.evalsha(sha, ScriptOutputType.INTEGER, key.toArray(new String[0]), args.toArray(new String[0]));
            assertEquals(0L, rejected);
            assertTrue(banEvent.await(2, TimeUnit.SECONDS));

            List<String> refillArgs = List.of("1700000001500", "2", "2", "2", "1");
            Long refilled = commands.evalsha(sha, ScriptOutputType.INTEGER, key.toArray(new String[0]), refillArgs.toArray(new String[0]));
            assertEquals(1L, refilled);
        } finally {
            if (pubSub != null) {
                pubSub.close();
            }
            if (connection != null) {
                connection.close();
            }
            client.shutdown();
        }
    }
}
