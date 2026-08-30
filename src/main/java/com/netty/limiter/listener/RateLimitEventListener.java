package com.netty.limiter.listener;

/**
 * @description: 限流与安全拦截事件监听器（解耦 Kafka / Log4j2 等上报依赖）
 **/
public interface RateLimitEventListener {

    /**
     * 当触发限流或安全短路拦截时触发 (全基本数据类型，0 堆内存分配，同时支持 IPv4 与 IPv6)
     *
     * @param ipHigh     IP 高 64 位 (IPv4 时为 0)
     * @param ipLow      IP 低 64 位 (IPv4 时存放 32 位数值)
     * @param userId     用户 ID (0 表示未识别)
     * @param rejectCode HTTP 响应码 (401 / 403 / 429)
     * @param reasonCode 拦截原因标识码 (参阅 RateLimitReasonCodes 常量)
     */
    void onRateLimitTriggered(long ipHigh, long ipLow, long userId, int rejectCode, int reasonCode);
}
