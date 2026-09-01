package com.netty.limiter.limiter;

import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.util.Recycler;
import io.netty.util.Timeout;

import java.util.concurrent.atomic.AtomicInteger;

/**
 * @description: 0-GC 响应式异步限流上下文对象池 (Recycler-Based Zero-GC Async Context)
 *
 * 核心并发安全设计：
 * 1. 状态机原子防护 (CAS State Guard):
 *    - STATE_INIT (0): 初始化/等待中
 *    - STATE_RESOLVED (1): Redis 正常返回并唤醒
 *    - STATE_TIMEOUT (2): 50ms 超时熔断触发
 *    - STATE_CANCELLED (3): 异常断开等主动取消
 * 2. 0 堆内存分配 (Zero Allocation): 基于 Netty Recycler 实现轻量对象复用，请求处理完毕自动归还对象池。
 **/
public class AsyncRateLimitContext {

    public static final int STATE_INIT = 0;
    public static final int STATE_RESOLVED = 1;
    public static final int STATE_TIMEOUT = 2;
    public static final int STATE_CANCELLED = 3;

    private final Recycler.Handle<AsyncRateLimitContext> recyclerHandle;

    public ChannelHandlerContext httpCtx;
    public ByteBuf downstreamBuf;
    public RateLimitCallback callback;
    public Timeout timeoutHandle;
    public long userId;

    public final AtomicInteger state = new AtomicInteger(STATE_INIT);

    private static final Recycler<AsyncRateLimitContext> RECYCLER = new Recycler<AsyncRateLimitContext>() {
        @Override
        protected AsyncRateLimitContext newObject(Handle<AsyncRateLimitContext> handle) {
            return new AsyncRateLimitContext(handle);
        }
    };

    private AsyncRateLimitContext(Recycler.Handle<AsyncRateLimitContext> recyclerHandle) {
        this.recyclerHandle = recyclerHandle;
    }

    /**
     * 从对象池借出 0-GC 异步上下文
     */
    public static AsyncRateLimitContext acquire(ChannelHandlerContext httpCtx,
                                                ByteBuf downstreamBuf,
                                                long userId,
                                                RateLimitCallback callback) {
        AsyncRateLimitContext ctx = RECYCLER.get();
        ctx.httpCtx = httpCtx;
        ctx.downstreamBuf = downstreamBuf;
        ctx.userId = userId;
        ctx.callback = callback;
        ctx.state.set(STATE_INIT);
        ctx.timeoutHandle = null;
        return ctx;
    }

    /**
     * 归还对象池并重置引用，切断物理内存泄露
     */
    public void recycle() {
        this.httpCtx = null;
        this.downstreamBuf = null;
        this.callback = null;
        this.timeoutHandle = null;
        this.userId = 0L;
        this.state.set(STATE_INIT);
        recyclerHandle.recycle(this);
    }
}
