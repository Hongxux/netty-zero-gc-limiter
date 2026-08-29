package com.netty.limiter.annotation;

import com.netty.limiter.autoconfigure.NettyRateLimiterAutoConfiguration;
import org.springframework.context.annotation.Import;

import java.lang.annotation.Documented;
import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * @description: 开启 Netty 极前置 0-GC 无锁限流与安全防御 SDK
 **/
@Target(ElementType.TYPE)
@Retention(RetentionPolicy.RUNTIME)
@Documented
@Import(NettyRateLimiterAutoConfiguration.class)
public @interface EnableNettyZeroGcRateLimiter {
}
