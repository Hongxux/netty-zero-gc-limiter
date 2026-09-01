package com.netty.limiter.limiter;

/**
 * @description: 0-GC 响应式限流校验回调接口 (Reactive Rate Limiting Callback)
 **/
@FunctionalInterface
public interface RateLimitCallback {

    /**
     * 限流校验结果通知
     * @param isAllowed true: 放行通过, false: 超限拦截
     */
    void onResult(boolean isAllowed);
}
