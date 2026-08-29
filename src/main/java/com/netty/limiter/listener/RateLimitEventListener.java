package com.netty.limiter.listener;

/**
 * @description: 限流与安全拦截事件监听器（解耦 Kafka / Log4j2 等上报依赖）
 **/
public interface RateLimitEventListener {

    /**
     * 当触发限流或安全短路拦截时触发
     *
     * @param clientIp   客户端 IP
     * @param userId     用户 ID (未识别为 0)
     * @param rejectCode 401 / 429 / 403
     * @param reason     拦截原因
     */
    void onRateLimitTriggered(String clientIp, long userId, int rejectCode, String reason);
}
