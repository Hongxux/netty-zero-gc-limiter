package com.netty.limiter.autoconfigure;

import com.netty.limiter.cache.LocalBanCache;
import com.netty.limiter.cache.RedisIpBanSubscriber;
import com.netty.limiter.config.GatewayRateLimitConfigListener;
import com.netty.limiter.config.GatewayRateLimitProperties;
import com.netty.limiter.handler.JwtHeaderSecurityHandler;
import com.netty.limiter.handler.NettyInboundSecurityHandler;
import com.netty.limiter.handler.NettyJwtHeaderAccumulatorHandler;
import com.netty.limiter.handler.NettySecurityCustomizer;
import com.netty.limiter.limiter.LocalGlobalRateLimiter;
import com.netty.limiter.limiter.UserRateLimiterOperate;
import org.springframework.boot.autoconfigure.condition.ConditionalOnClass;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

/**
 * @description: Spring Boot Starter 自动装配类 (规范的 条件装配 + @ConditionalOnMissingBean 设计)
 **/
@Configuration
@EnableConfigurationProperties(GatewayRateLimitProperties.class)
@ConditionalOnProperty(prefix = "netty.limiter", name = "enabled", havingValue = "true", matchIfMissing = true)
public class NettyRateLimiterAutoConfiguration {

    @Bean
    @ConditionalOnMissingBean
    public LocalGlobalRateLimiter localGlobalRateLimiter() {
        return new LocalGlobalRateLimiter();
    }

    @Bean
    @ConditionalOnMissingBean
    public LocalBanCache localBanCache() {
        return new LocalBanCache();
    }

    @Bean
    @ConditionalOnMissingBean
    public UserRateLimiterOperate userRateLimiterOperate() {
        return new UserRateLimiterOperate();
    }

    @Bean
    @ConditionalOnMissingBean
    public RedisIpBanSubscriber redisIpBanSubscriber() {
        return new RedisIpBanSubscriber();
    }

    @Bean
    @ConditionalOnMissingBean
    public JwtHeaderSecurityHandler jwtHeaderSecurityHandler() {
        return new JwtHeaderSecurityHandler();
    }

    @Bean
    @ConditionalOnMissingBean
    public GatewayRateLimitConfigListener gatewayRateLimitConfigListener() {
        return new GatewayRateLimitConfigListener();
    }

    @Bean
    @ConditionalOnMissingBean
    public NettyJwtHeaderAccumulatorHandler nettyJwtHeaderAccumulatorHandler() {
        return new NettyJwtHeaderAccumulatorHandler();
    }

    @Bean
    @ConditionalOnMissingBean
    public NettyInboundSecurityHandler nettyInboundSecurityHandler() {
        return new NettyInboundSecurityHandler();
    }

    @Bean
    @ConditionalOnMissingBean
    @ConditionalOnClass(name = "org.springframework.boot.web.embedded.netty.NettyServerCustomizer")
    public NettySecurityCustomizer nettySecurityCustomizer(NettyJwtHeaderAccumulatorHandler jwtAccumulatorHandler,
                                                           NettyInboundSecurityHandler securityHandler) {
        return new NettySecurityCustomizer(jwtAccumulatorHandler, securityHandler);
    }
}
