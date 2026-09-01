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

    public static final int STATE_UNPUBLISHED = -1; // 🎯 从对象池借出，正在写入，尚未完成内存发布
    public static final int STATE_INIT = 0;         // 🎯 内存发布完成 (setRelease 屏障)，等待 Redis 响应
    public static final int STATE_RESOLVED = 1;     // 🎯 Redis 正常返回并唤醒
    public static final int STATE_TIMEOUT = 2;      // 🎯 50ms 超时熔断触发
    public static final int STATE_CANCELLED = 3;    // 🎯 异常断开等主动取消

    private final Recycler.Handle<AsyncRateLimitContext> recyclerHandle;

    public ChannelHandlerContext httpCtx;
    public ByteBuf downstreamBuf;
    public RateLimitCallback callback;
    public Timeout timeoutHandle;
    public long userId;
    public int index;

    public final AtomicInteger state = new AtomicInteger(STATE_UNPUBLISHED);

    private static final Recycler<AsyncRateLimitContext> RECYCLER = new Recycler<AsyncRateLimitContext>() {
        @Override
        protected AsyncRateLimitContext newObject(Handle<AsyncRateLimitContext> handle) {
            return new AsyncRateLimitContext(handle);
        }
    };

    AsyncRateLimitContext(Recycler.Handle<AsyncRateLimitContext> recyclerHandle) {
        this.recyclerHandle = recyclerHandle;
    }

    /**
     * 从对象池借出 0-GC 异步上下文 (初始状态为 STATE_UNPUBLISHED)
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
        ctx.timeoutHandle = null;
        ctx.index = 0;
        ctx.state.set(STATE_UNPUBLISHED);
        return ctx;
    }

    /**
     * 🎯 恢复续体执行并 0-GC 归还对象池 (带 try-finally 异常安全保障)
     *
     * 1. 触发注册的续体回调 (resumeContinuation)，通知下游流水线恢复；
     * 2. 无论业务逻辑是否发生异常，在 finally 块中自动归还 Recycler 对象池。
     */
    public void resume(boolean isAllowed) {
        try {
            if (this.callback != null) {
                this.callback.onResult(isAllowed);
            }
        } finally {
            this.recycle();
        }
    }

    /**
     * 响应式续体恢复别名方法 (Continuation.resumeWith)
     */
    public void resumeWith(boolean isAllowed) {
        resume(isAllowed);
    }

    /**
     * 🎯 严格有序的 Reset 与对象回收：
     * 1. 先清空所有外部大对象引用，切断物理内存泄露
     * 2. 重置基本数据类型与状态
     * 3. 最终归还 Recycler 对象池
     */
    public void recycle() {
        this.httpCtx = null;
        this.downstreamBuf = null;
        this.callback = null;
        this.timeoutHandle = null;
        this.userId = 0L;
        this.index = 0;
        this.state.setRelease(STATE_UNPUBLISHED); // 🎯 以 Release 语义发布重置状态
        if (recyclerHandle != null) {
            recyclerHandle.recycle(this);
        }
    }
}
