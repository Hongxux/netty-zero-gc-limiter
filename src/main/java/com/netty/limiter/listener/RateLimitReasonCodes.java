package com.netty.limiter.listener;

/**
 * @description: 限流与安全拦截原因码常量
 **/
public class RateLimitReasonCodes {
    public static final int REASON_ANONYMOUS_UNAUTHORIZED = 1;
    public static final int REASON_INVALID_JWT = 2;
    public static final int REASON_LOCAL_BAN = 3;
    public static final int REASON_GLOBAL_RATE_LIMIT = 4;
    public static final int REASON_HEADER_ACCUMULATION_OVERFLOW = 5;
}
