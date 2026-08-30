package com.netty.limiter.limiter;

import java.lang.invoke.MethodHandles;
import java.lang.invoke.VarHandle;

/**
 * =========================================================================================
 * 🚀 0-GC 无锁高性能 SyncWaitSlot 环形缓冲区 (物理 Cache Line 伪共享隔离 + Safe Zone 缓存 + VarHandle Acquire/Release 内存屏障)
 * 
 * 【体系结构级微观物理优化与并发安全 Guarantee】：
 * 1. 56 字节 Cache Line 物理隔离: 彻底切断 Single-Consumer (Netty EventLoop) 与 Multi-Producer (网关请求线程) 间的 Cache Line 伪共享 (False Sharing)。
 * 2. Safe Zone 惰性读写序列号: 优先只读本地 L1 Cache，仅在临界满/空时触发 1 次跨核 Bus Sniffing 嗅探。
 * 3. VarHandle Acquire/Release 语义管理与防旧线程污染:
 *    - 移除传统 volatile，全面使用 `VarHandle.setRelease` / `VarHandle.getAcquire` 严格约束 Store-Load / Release-Acquire 屏障。
 *    - 针对 `waiterThread` 引入专用 VarHandle 内存控制，在 `reset` 时 `setRelease` 绑定新线程，在 `clear` 时 `setRelease(null)` 彻底清除残留引用，确保绝对不会误读/唤醒旧线程。
 *    - 生产者写入 `reset` 时以 `setRelease` 结束，保证前面的 status 与 waiterThread 写入对 Consumer 语义可见。
 *    - 消费者以 `getAcquire` 在有界自旋 (MAX_SPIN_COUNT = 4096 + Thread.onSpinWait) 中检测 `userId != 0L`。
 * =========================================================================================
 */
abstract class SyncWaitSlotRingBufferPad0 {
    protected long p00, p01, p02, p03, p04, p05, p06, p07;
}

abstract class SyncWaitSlotRingBufferConsumerFields extends SyncWaitSlotRingBufferPad0 {
    // nextNeededAckSequence: 消费者 (Netty EventLoop) 下一个急需 ACK 出队的序列号 (由 VarHandle 语义控制)
    protected long nextNeededAckSequence = 0;
    // cachedNextAvailableRequestSequence: 消费者本地 Safe Zone 缓存的生产者请求序列号 (普通 long，无需跨核嗅探)
    protected long cachedNextAvailableRequestSequence = 0;
}

abstract class SyncWaitSlotRingBufferPad1 extends SyncWaitSlotRingBufferConsumerFields {
    protected long p10, p11, p12, p13, p14, p15, p16, p17;
}

abstract class SyncWaitSlotRingBufferProducerFields extends SyncWaitSlotRingBufferPad1 {
    // nextAvailableRequestSequence: 生产者 (网关请求线程) 下一个可申请/预占的请求序列号 (由 VarHandle 语义控制)
    protected long nextAvailableRequestSequence = 0;
    // cachedNextNeededAckSequence: 生产者本地 Safe Zone 缓存的消费者确认序列号 (普通 long)
    protected long cachedNextNeededAckSequence = 0;
}

abstract class SyncWaitSlotRingBufferPad2 extends SyncWaitSlotRingBufferProducerFields {
    protected long p20, p21, p22, p23, p24, p25, p26, p27;
}

public class SyncWaitSlotRingBuffer extends SyncWaitSlotRingBufferPad2 {

    public static final int MAX_SPIN_COUNT = 4096; // 🎯 仿照 LocalBanCache 探查限制，定义自旋最大尝试次数

    public static class SyncWaitSlot {
        public long userId = 0L; // 0L 标识槽位为空/已被消费清空，非 0 标识已由生产者填充 (由 VarHandle 语义控制)
        public int status = 0;   // 0=pending, 1=passed, 2=blocked
        public Thread waiterThread;

        public void reset(long uid, Thread thread) {
            this.status = 0;
            WAITER_THREAD_HANDLE.setRelease(this, thread);
            // 🎯 VarHandle setRelease 内存屏障: 保证 status 和 waiterThread 的写入在 userId 非零发布前全量刷入 Cache
            USER_ID_HANDLE.setRelease(this, uid);
        }

        public void clear() {
            WAITER_THREAD_HANDLE.setRelease(this, null); // 🎯 明确清空等待线程引用，彻底防止残存旧线程指针
            STATUS_HANDLE.setRelease(this, 0);
            USER_ID_HANDLE.setRelease(this, 0L);
        }

        public long getUserIdAcquire() {
            return (long) USER_ID_HANDLE.getAcquire(this);
        }

        public int getStatusAcquire() {
            return (int) STATUS_HANDLE.getAcquire(this);
        }

        public void setStatusRelease(int st) {
            STATUS_HANDLE.setRelease(this, st);
        }

        public Thread getWaiterThreadAcquire() {
            return (Thread) WAITER_THREAD_HANDLE.getAcquire(this);
        }
    }

    private final SyncWaitSlot[] array;
    private final int mask;

    private static final VarHandle NEXT_NEEDED_ACK_SEQUENCE_HANDLE;
    private static final VarHandle NEXT_AVAILABLE_REQUEST_SEQUENCE_HANDLE;
    private static final VarHandle USER_ID_HANDLE;
    private static final VarHandle STATUS_HANDLE;
    private static final VarHandle WAITER_THREAD_HANDLE;

    static {
        try {
            MethodHandles.Lookup l = MethodHandles.lookup();
            NEXT_NEEDED_ACK_SEQUENCE_HANDLE = l.findVarHandle(SyncWaitSlotRingBuffer.class, "nextNeededAckSequence", long.class);
            NEXT_AVAILABLE_REQUEST_SEQUENCE_HANDLE = l.findVarHandle(SyncWaitSlotRingBuffer.class, "nextAvailableRequestSequence", long.class);
            USER_ID_HANDLE = l.findVarHandle(SyncWaitSlot.class, "userId", long.class);
            STATUS_HANDLE = l.findVarHandle(SyncWaitSlot.class, "status", int.class);
            WAITER_THREAD_HANDLE = l.findVarHandle(SyncWaitSlot.class, "waiterThread", Thread.class);
        } catch (ReflectiveOperationException e) {
            throw new ExceptionInInitializerError(e);
        }
    }

    public SyncWaitSlotRingBuffer(int capacity) {
        int cap = 1;
        while (cap < capacity) cap <<= 1;
        this.array = new SyncWaitSlot[cap];
        this.mask = cap - 1;
        for (int i = 0; i < cap; i++) {
            this.array[i] = new SyncWaitSlot();
        }
    }

    /**
     * MPSC 生产者多线程 CAS 预占下一个可用请求槽位入队 (Safe Zone 本地 cachedNextNeededAckSequence 保护)
     */
    public SyncWaitSlot offer(long uid, Thread thread) {
        long currentAvailableReqSeq;
        do {
            currentAvailableReqSeq = (long) NEXT_AVAILABLE_REQUEST_SEQUENCE_HANDLE.getAcquire(this);
            if (isFull(currentAvailableReqSeq)) {
                return null; // 缓冲区满，Fail-Open 降级处理
            }
        } while (!NEXT_AVAILABLE_REQUEST_SEQUENCE_HANDLE.compareAndSet(this, currentAvailableReqSeq, currentAvailableReqSeq + 1));

        int index = (int) (currentAvailableReqSeq & mask);
        SyncWaitSlot slot = array[index];
        slot.reset(uid, thread);
        return slot;
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
     * SPSC 单消费者 (Netty EventLoop) 查看队头等待槽位 (VarHandle getAcquire 有界自旋 + Thread.onSpinWait 保护)
     */
    public SyncWaitSlot peek() {
        long currentNeededAckSeq = (long) NEXT_NEEDED_ACK_SEQUENCE_HANDLE.getAcquire(this);
        if (isEmpty(currentNeededAckSeq)) {
            return null;
        }
        SyncWaitSlot slot = array[(int) (currentNeededAckSeq & mask)];
        
        // 🎯 VarHandle getAcquire 有界自旋: 最多自旋 4096 次
        int spins = 0;
        while (slot.getUserIdAcquire() == 0L) {
            if (++spins > MAX_SPIN_COUNT) {
                return null; // 🛡️ 超出自旋次数上限，防御性降级返回 null
            }
            Thread.onSpinWait();
        }
        return slot;
    }

    /**
     * SPSC 单消费者 (Netty EventLoop) 弹出队头槽位并推进 nextNeededAckSequence 序列号
     */
    public SyncWaitSlot poll() {
        long currentNeededAckSeq = (long) NEXT_NEEDED_ACK_SEQUENCE_HANDLE.getAcquire(this);
        if (isEmpty(currentNeededAckSeq)) {
            return null;
        }
        int index = (int) (currentNeededAckSeq & mask);
        SyncWaitSlot slot = array[index];
        
        // 🎯 VarHandle getAcquire 有界自旋
        int spins = 0;
        while (slot.getUserIdAcquire() == 0L) {
            if (++spins > MAX_SPIN_COUNT) {
                return null; // 🛡️ 超出自旋次数上限，防御性降级返回 null
            }
            Thread.onSpinWait();
        }
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
