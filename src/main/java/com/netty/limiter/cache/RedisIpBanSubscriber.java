package com.netty.limiter.cache;

import com.alibaba.fastjson2.JSON;
import com.alibaba.fastjson2.JSONObject;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.CommandLineRunner;
import org.springframework.data.redis.connection.ReactiveRedisConnectionFactory;
import org.springframework.data.redis.listener.ChannelTopic;
import org.springframework.data.redis.listener.ReactiveRedisMessageListenerContainer;
import org.springframework.stereotype.Component;

/**
 * @description: Redis Pub/Sub 全网黑名单同步订阅器
 **/
@Slf4j
@Component
public class RedisIpBanSubscriber implements CommandLineRunner {

    private static final String BAN_PUBSUB_CHANNEL = "NETTY_LIMITER_BAN_CHANNEL";

    @Autowired(required = false)
    private ReactiveRedisConnectionFactory connectionFactory;

    @Autowired
    private LocalBanCache localBanCache;

    @Override
    public void run(String... args) throws Exception {
        if (connectionFactory == null) {
            log.info("ReactiveRedisConnectionFactory is null, skip PubSub ban subscriber.");
            return;
        }

        ReactiveRedisMessageListenerContainer container = new ReactiveRedisMessageListenerContainer(connectionFactory);
        container.receive(ChannelTopic.of(BAN_PUBSUB_CHANNEL))
                .subscribe(message -> {
                    try {
                        String payload = message.getMessage();
                        if (payload == null || payload.isEmpty()) {
                            return;
                        }
                        long userId = 0;
                        long duration = 60; // 默认封禁 60 秒
                        if (payload.startsWith("{")) {
                            JSONObject json = JSON.parseObject(payload);
                            if (json != null) {
                                userId = json.getLongValue("userId");
                                long dur = json.getLongValue("durationSeconds");
                                if (dur > 0) duration = dur;
                            }
                        } else {
                            userId = Long.parseLong(payload.trim());
                        }
                        if (userId > 0) {
                            localBanCache.putUserBan(userId, duration);
                            log.warn("Received PubSub ban message for userId: {}, duration: {}s", userId, duration);
                        }
                    } catch (Exception e) {
                        log.error("Failed to process PubSub ban message: {}", message.getMessage(), e);
                    }
                }, error -> log.error("Error in Redis PubSub subscription", error));
    }
}
