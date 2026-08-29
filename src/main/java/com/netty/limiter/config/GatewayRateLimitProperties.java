package com.netty.limiter.config;

import lombok.Data;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.stereotype.Component;

/**
 * @description: 通用 Netty 0-GC 限流 SDK 配置类
 **/
@Data
@Component
@ConfigurationProperties(prefix = "netty.limiter")
public class GatewayRateLimitProperties {

    /**
     * 是否开启纯原生 Netty 独立运行模式 (为 false 时做为 SCG 挂载组件)
     */
    private Boolean standalone = false;

    /**
     * 网关服务监听端口 (独立运行模式下使用)
     */
    private Integer serverPort = 8888;

    /**
     * 是否开启 Netty 极前置限流引擎
     */
    private Boolean enabled = true;

    /**
     * 节点级无锁令牌桶 QPS 容量阈值
     */
    private Integer globalQps = 100000;

    /**
     * 每秒补充令牌速率
     */
    private Integer fillRate = 100000;

    /**
     * 单 UID 每秒访问上限
     */
    private Integer uidMaxPerSec = 20;

    /**
     * Redis 地址 (Host:Port)
     */
    private String redisHost = "127.0.0.1";
    private Integer redisPort = 6379;
}
