package com.netty.limiter.limiter;

import io.netty.util.concurrent.FastThreadLocal;

import java.lang.invoke.MethodHandles;
import java.lang.invoke.VarHandle;

/**
 * =========================================================================================
 * 🚀 0-GC 无锁高性能 SyncWaitSlot 环形缓冲区
 *    (物理 Cache Line 隔离 + Safe Zone + FTL per-thread Slot + ZERO_SLOT & CANCELLED_SLOT COW 原子替换)
 *
 * 【体系结构与并发安全 Guarantee】：
 * 1. 56 字节 Cache Line 物理隔离：彻底切断 Consumer (Netty EventLoop) 与 Producer (网关线程) 跨核伪共享。
 * 2. Safe Zone 惰性序列号缓存：临界满/空时才触发 1 次跨核 Bus Sniffing。
 * 3. FTL per-thread SyncWaitSlot：每个生产者线程独占一个 Slot 实例，0-GC 堆分配。
 * 4. 100% 严格 FIFO 顺序保障 (EventLoop 单线程顺序入队)：
 *    - `offer()` 在 EventLoop 线程中与 `writeAndFlush()` 顺序执行。
 *    - 确保：EventLoop 任务队列顺序 == RingBuffer 序列号顺序 == Netty TCP Write 顺序 == Redis 响应顺序 (100% 绝对一致，零并发倒置隐患)。
 * 5. CANCELLED_SLOT 哨兵原子脱钩 (COW Atomic Cancel)：
 *    - 50ms 超时未收到 Redis 响应时，通过 CAS (`ARRAY_VH.compareAndSet`) 将 array[index] 原子替换为 `CANCELLED_SLOT` 哨兵，彻底解决对象别名 (Aliasing Bug)。
 * =========================================================================================
 */
abstract class SyncWaitSlotRingBufferPad0 {
    protected long p00, p01, p02, p03, p04, p05, p06, p07;
}

abstract class SyncWaitSlotRingBufferConsumerFields extends SyncWaitSlotRingBufferPad0 {
    protected long nextNeededAckSequence = 0;
    protected long cachedNextAvailableRequestSequence = 0;
}

abstract class SyncWaitSlotRingBufferPad1 extends SyncWaitSlotRingBufferConsumerFields {
    protected long p10, p11, p12, p13, p14, p15, p16, p17;
}

abstract class SyncWaitSlotRingBufferProducerFields extends SyncWaitSlotRingBufferPad1 {
    protected long nextAvailableRequestSequence = 0;
    protected long cachedNextNeededAckSequence = 0;
}

abstract class SyncWaitSlotRingBufferPad2 extends SyncWaitSlotRingBufferProducerFields {
    protected long p20, p21, p22, p23, p24, p25, p26, p27;
}

public class SyncWaitSlotRingBuffer extends SyncWaitSlotRingBufferPad2 {

    public static final int MAX_SPIN_COUNT = 4096;

    /**
     * 🎯 CANCELLED_CONTEXT 哨兵：标识槽位因 50ms 超时被原子放弃，EventLoop 遇到此哨兵直接 poll() 推进队列。
     */
    public static final AsyncRateLimitContext CANCELLED_CONTEXT = new AsyncRateLimitContext(null);

    private final AsyncRateLimitContext[] array;
    private final int mask;

    private static final VarHandle NEXT_NEEDED_ACK_SEQUENCE_HANDLE;
    private static final VarHandle NEXT_AVAILABLE_REQUEST_SEQUENCE_HANDLE;
    private static final VarHandle ARRAY_VH; // 数组元素引用的 VarHandle

    static {
        try {
            MethodHandles.Lookup l = MethodHandles.lookup();
            NEXT_NEEDED_ACK_SEQUENCE_HANDLE = l.findVarHandle(SyncWaitSlotRingBuffer.class, "nextNeededAckSequence", long.class);
            NEXT_AVAILABLE_REQUEST_SEQUENCE_HANDLE = l.findVarHandle(SyncWaitSlotRingBuffer.class, "nextAvailableRequestSequence", long.class);
            ARRAY_VH = MethodHandles.arrayElementVarHandle(AsyncRateLimitContext[].class);
        } catch (ReflectiveOperationException e) {
            throw new ExceptionInInitializerError(e);
        }
    }

    public SyncWaitSlotRingBuffer(int capacity) {
        int cap = 1;
        while (cap < capacity) cap <<= 1;
        this.array = new AsyncRateLimitContext[cap];
        this.mask = cap - 1;
    }

    /**
     * MPSC/SPSC 生产者：CAS 原子抢占可用序列号，写入 index，并以 ctx.state.setRelease 作为终极发布屏障。
     */
    public boolean offer(AsyncRateLimitContext ctx) {
        long currentAvailableReqSeq;
        do {
            currentAvailableReqSeq = (long) NEXT_AVAILABLE_REQUEST_SEQUENCE_HANDLE.getAcquire(this);
            if (isFull(currentAvailableReqSeq)) {
                return false; // 缓冲区满，Fail-Open 降级
            }
        } while (!NEXT_AVAILABLE_REQUEST_SEQUENCE_HANDLE.compareAndSet(this, currentAvailableReqSeq, currentAvailableReqSeq + 1));

        int index = (int) (currentAvailableReqSeq & mask);
        ctx.index = index;

        // 1. 先写入数组槽位
        ARRAY_VH.set(array, index, ctx);

        // 🎯 2. 终极发布屏障：以 setRelease 发布 STATE_INIT，保证上方所有字段对 Consumer getAcquire 严格可见
        ctx.state.setRelease(AsyncRateLimitContext.STATE_INIT);
        return true;
    }

    /**
     * 🛡️ 生产者超时原子取消：
     * 若 array[index] 中依然是当前 ctx，则用 CAS 将其原子替换为 CANCELLED_CONTEXT 哨兵。
     * 解除 ctx 在 RingBuffer 上的指针引用，防止迟到响应误唤醒。
     */
    public boolean cancel(AsyncRateLimitContext ctx) {
        if (ctx == null || ctx == CANCELLED_CONTEXT) {
            return false;
        }
        int index = ctx.index;
        return ARRAY_VH.compareAndSet(array, index, ctx, CANCELLED_CONTEXT);
    }

    private boolean isFull(long currentAvailableReqSeq) {
        if (currentAvailableReqSeq - this.cachedNextNeededAckSequence >= array.length) {
            long freshNeededAckSeq = (long) NEXT_NEEDED_ACK_SEQUENCE_HANDLE.getAcquire(this);
            if (currentAvailableReqSeq - freshNeededAckSeq >= array.length) {
                return true;
            }
            this.cachedNextNeededAckSequence = freshNeededAckSeq;
        }
        return false;
    }

    /**
     * 读取指定槽位的上下文引用 (Acquire 语义)
     */
    private AsyncRateLimitContext getContextAcquire(int index) {
        return (AsyncRateLimitContext) ARRAY_VH.getAcquire(array, index);
    }

    /**
     * 判定槽位是否尚未就绪：
     * 1. 槽位为 null：生产者尚未写入数组指针；
     * 2. 状态为 STATE_UNPUBLISHED：生产者正在填充字段，尚未执行 setRelease 发布门禁。
     * （注：CANCELLED_CONTEXT 哨兵视为已就绪，直接出队消费）
     */
    private static boolean isUnpublishedOrEmpty(AsyncRateLimitContext ctx) {
        if (ctx == null) {
            return true;
        }
        if (ctx == CANCELLED_CONTEXT) {
            return false;
        }
        return ctx.state.getAcquire() == AsyncRateLimitContext.STATE_UNPUBLISHED;
    }

    /**
     * 有界自旋等待槽位发布落地 (Bounded Spin Wait for Published Entry)
     */
    private AsyncRateLimitContext spinWaitForPublished(int index) {
        int spins = 0;
        AsyncRateLimitContext ctx;
        while (isUnpublishedOrEmpty(ctx = getContextAcquire(index))) {
            if (++spins > MAX_SPIN_COUNT) {
                return null; // 🛡️ 防御性降级
            }
            Thread.onSpinWait();
        }
        return ctx;
    }

    /**
     * SPSC Consumer peek：读取队头 Context（若处于并发发布中途则有界自旋等待落地）
     */
    public AsyncRateLimitContext peek() {
        long currentNeededAckSeq = (long) NEXT_NEEDED_ACK_SEQUENCE_HANDLE.getAcquire(this);
        if (isEmpty(currentNeededAckSeq)) {
            return null;
        }
        int index = (int) (currentNeededAckSeq & mask);
        return spinWaitForPublished(index);
    }

    /**
     * SPSC Consumer poll：弹出队头 Context，严格有序地以 setRelease(null) 清空槽位，推进序列号。
     */
    public AsyncRateLimitContext poll() {
        long currentNeededAckSeq = (long) NEXT_NEEDED_ACK_SEQUENCE_HANDLE.getAcquire(this);
        if (isEmpty(currentNeededAckSeq)) {
            return null;
        }
        int index = (int) (currentNeededAckSeq & mask);

        AsyncRateLimitContext ctx = spinWaitForPublished(index);
        if (ctx == null) {
            return null;
        }

        // 🎯 严格有序的 Reset：
        // 1. 先清空槽位指针引用 (setRelease(null))，切断 GC 根引用
        ARRAY_VH.setRelease(array, index, null);
        // 2. 最终推进消费序列号，允许生产者覆写该槽位
        NEXT_NEEDED_ACK_SEQUENCE_HANDLE.setRelease(this, currentNeededAckSeq + 1);
        return ctx;
    }

    private boolean isEmpty(long currentNeededAckSeq) {
        if (currentNeededAckSeq >= this.cachedNextAvailableRequestSequence) {
            long freshAvailableReqSeq = (long) NEXT_AVAILABLE_REQUEST_SEQUENCE_HANDLE.getAcquire(this);
            if (currentNeededAckSeq >= freshAvailableReqSeq) {
                return true;
            }
            this.cachedNextAvailableRequestSequence = freshAvailableReqSeq;
        }
        return false;
    }
}
