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
     * 🎯 FTL per-thread Slot：每个生产者线程独占一个 SyncWaitSlot 实例，0-GC 复用，无跨线程写竞争。
     */
    public static final FastThreadLocal<SyncWaitSlot> THREAD_SLOT = new FastThreadLocal<>() {
        @Override
        protected SyncWaitSlot initialValue() {
            return new SyncWaitSlot();
        }
    };

    /**
     * 🎯 ZERO_SLOT 哨兵：标识数组槽位为空（待生产者写入），COW 清空的原子占位对象。
     */
    public static final SyncWaitSlot ZERO_SLOT = new SyncWaitSlot();

    /**
     * 🎯 CANCELLED_SLOT 哨兵：标识槽位因 50ms 超时被生产者原子放弃，EventLoop 遇到此哨兵直接 poll() 推进队列。
     */
    public static final SyncWaitSlot CANCELLED_SLOT = new SyncWaitSlot();

    public static class SyncWaitSlot {
        public long userId;
        public int status;        // 0=pending, 1=passed, 2=blocked
        public Thread waiterThread;
        public AsyncRateLimitContext asyncCtx; // 🎯 0-GC 响应式异步上下文
        public int index;         // 🎯 记录当前 Slot 被放入 RingBuffer 的数组下标
    }

    private final SyncWaitSlot[] array;
    private final int mask;

    private static final VarHandle NEXT_NEEDED_ACK_SEQUENCE_HANDLE;
    private static final VarHandle NEXT_AVAILABLE_REQUEST_SEQUENCE_HANDLE;
    private static final VarHandle ARRAY_VH; // 数组元素引用的 VarHandle

    static {
        try {
            MethodHandles.Lookup l = MethodHandles.lookup();
            NEXT_NEEDED_ACK_SEQUENCE_HANDLE = l.findVarHandle(SyncWaitSlotRingBuffer.class, "nextNeededAckSequence", long.class);
            NEXT_AVAILABLE_REQUEST_SEQUENCE_HANDLE = l.findVarHandle(SyncWaitSlotRingBuffer.class, "nextAvailableRequestSequence", long.class);
            ARRAY_VH = MethodHandles.arrayElementVarHandle(SyncWaitSlot[].class);
        } catch (ReflectiveOperationException e) {
            throw new ExceptionInInitializerError(e);
        }
    }

    public SyncWaitSlotRingBuffer(int capacity) {
        int cap = 1;
        while (cap < capacity) cap <<= 1;
        this.array = new SyncWaitSlot[cap];
        this.mask = cap - 1;
        // 初始化所有槽位为 ZERO_SLOT 哨兵
        for (int i = 0; i < cap; i++) {
            ARRAY_VH.setRelease(array, i, ZERO_SLOT);
        }
    }

    /**
     * MPSC/SPSC 生产者：CAS 原子抢占可用序列号，写入 index 并通过 ARRAY_VH.setRelease 原子发布引用。
     * 线程安全：使用 CAS 循环抢占 nextAvailableRequestSequence，彻底杜绝序列号覆盖与并发冲突隐患。
     */
    public boolean offer(SyncWaitSlot slot) {
        long currentAvailableReqSeq;
        do {
            currentAvailableReqSeq = (long) NEXT_AVAILABLE_REQUEST_SEQUENCE_HANDLE.getAcquire(this);
            if (isFull(currentAvailableReqSeq)) {
                return false; // 缓冲区满，Fail-Open 降级
            }
        } while (!NEXT_AVAILABLE_REQUEST_SEQUENCE_HANDLE.compareAndSet(this, currentAvailableReqSeq, currentAvailableReqSeq + 1));

        int index = (int) (currentAvailableReqSeq & mask);
        slot.index = index;
        slot.status = 0;

        // 🎯 单次 ARRAY_VH.setRelease = Release Fence：保证上方 plain 写入对 Consumer getAcquire 可见
        ARRAY_VH.setRelease(array, index, slot);
        return true;
    }

    /**
     * 🛡️ 生产者超时原子取消：
     * 若 array[index] 中依然是当前 slot，则用 CAS 将其原子替换为 CANCELLED_SLOT 哨兵。
     * 解除 FTL slot 在 RingBuffer 上的指针引用，防止对象别名 (Aliasing) 导致迟到响应误唤醒。
     */
    public boolean cancel(SyncWaitSlot slot) {
        if (slot == null || slot == ZERO_SLOT || slot == CANCELLED_SLOT) {
            return false;
        }
        int index = slot.index;
        return ARRAY_VH.compareAndSet(array, index, slot, CANCELLED_SLOT);
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
     * SPSC Consumer peek：ARRAY_VH.getAcquire 有界自旋等待槽位非 ZERO_SLOT。
     */
    public SyncWaitSlot peek() {
        long currentNeededAckSeq = (long) NEXT_NEEDED_ACK_SEQUENCE_HANDLE.getAcquire(this);
        if (isEmpty(currentNeededAckSeq)) {
            return null;
        }
        int index = (int) (currentNeededAckSeq & mask);

        int spins = 0;
        SyncWaitSlot slot;
        while ((slot = (SyncWaitSlot) ARRAY_VH.getAcquire(array, index)) == ZERO_SLOT) {
            if (++spins > MAX_SPIN_COUNT) {
                return null; // 🛡️ 防御性降级
            }
            Thread.onSpinWait();
        }
        return slot;
    }

    /**
     * SPSC Consumer poll：弹出队头 Slot，以 ARRAY_VH.setRelease(ZERO_SLOT) COW 原子清空槽位，推进序列号。
     */
    public SyncWaitSlot poll() {
        long currentNeededAckSeq = (long) NEXT_NEEDED_ACK_SEQUENCE_HANDLE.getAcquire(this);
        if (isEmpty(currentNeededAckSeq)) {
            return null;
        }
        int index = (int) (currentNeededAckSeq & mask);

        int spins = 0;
        SyncWaitSlot slot;
        while ((slot = (SyncWaitSlot) ARRAY_VH.getAcquire(array, index)) == ZERO_SLOT) {
            if (++spins > MAX_SPIN_COUNT) {
                return null;
            }
            Thread.onSpinWait();
        }

        // COW 原子替换：单次 ARRAY_VH.setRelease 将槽位恢复为 ZERO_SLOT
        ARRAY_VH.setRelease(array, index, ZERO_SLOT);
        NEXT_NEEDED_ACK_SEQUENCE_HANDLE.setRelease(this, currentNeededAckSeq + 1);
        return slot;
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
