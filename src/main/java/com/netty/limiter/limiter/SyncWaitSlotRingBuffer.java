package com.netty.limiter.limiter;

import java.lang.invoke.MethodHandles;
import java.lang.invoke.VarHandle;

/**
 * =========================================================================================
 * 🚀 0-GC 无锁高性能 SyncWaitSlot 环形缓冲区 (物理 Cache Line 伪共享隔离 + Safe Zone 缓存优化版)
 * 
 * 【体系结构级微观物理优化】：
 * 1. 56 字节 Cache Line 物理隔离: 彻底切断 Single-Consumer (Netty EventLoop) 与 Multi-Producer (网关请求线程) 间的 Cache Line 伪共享 (False Sharing)。
 * 2. Safe Zone 惰性读写指针 (cachedHead / cachedTail): 优先只读本地 L1 Cache，仅在临界满/空时触发 1 次跨核 Bus Sniffing 嗅探。
 * 3. 0-GC 预分配槽位数组: 槽位实例在构造时一次性预分配，只重置状态字段，彻底消除 JVM 堆内存 Node 开销。
 * =========================================================================================
 */
abstract class SyncWaitSlotRingBufferPad0 {
    protected long p00, p01, p02, p03, p04, p05, p06, p07;
}

abstract class SyncWaitSlotRingBufferConsumerFields extends SyncWaitSlotRingBufferPad0 {
    // head: 消费者 (Netty EventLoop) 读取已完成槽位的出队指针
    protected long head = 0;
    // cachedTail: 消费者本地 Safe Zone 缓存的生产者入队指针 (普通 long，无需跨核嗅探)
    protected long cachedTail = 0;
}

abstract class SyncWaitSlotRingBufferPad1 extends SyncWaitSlotRingBufferConsumerFields {
    protected long p10, p11, p12, p13, p14, p15, p16, p17;
}

abstract class SyncWaitSlotRingBufferProducerFields extends SyncWaitSlotRingBufferPad1 {
    // tail: 生产者 (网关请求线程) 入队申请的指针 (CAS 预占)
    protected long tail = 0;
    // cachedHead: 生产者本地 Safe Zone 缓存的消费者出队指针 (普通 long)
    protected long cachedHead = 0;
}

abstract class SyncWaitSlotRingBufferPad2 extends SyncWaitSlotRingBufferProducerFields {
    protected long p20, p21, p22, p23, p24, p25, p26, p27;
}

public class SyncWaitSlotRingBuffer extends SyncWaitSlotRingBufferPad2 {

    public static class SyncWaitSlot {
        public volatile long userId;
        public volatile int status; // 0=pending, 1=passed, 2=blocked
        public volatile Thread waiterThread;

        public void reset(long uid, Thread thread) {
            this.userId = uid;
            this.status = 0;
            this.waiterThread = thread;
        }
    }

    private final SyncWaitSlot[] array;
    private final int mask;

    private static final VarHandle HEAD_HANDLE;
    private static final VarHandle TAIL_HANDLE;

    static {
        try {
            MethodHandles.Lookup l = MethodHandles.lookup();
            HEAD_HANDLE = l.findVarHandle(SyncWaitSlotRingBuffer.class, "head", long.class);
            TAIL_HANDLE = l.findVarHandle(SyncWaitSlotRingBuffer.class, "tail", long.class);
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
     * MPSC 生产者多线程 CAS 申请槽位 (Safe Zone 本地 cachedHead 保护)
     */
    public SyncWaitSlot offer(long uid, Thread thread) {
        long currentTail;
        do {
            currentTail = (long) TAIL_HANDLE.getAcquire(this);
            if (isFull(currentTail)) {
                return null; // 缓冲区满， Fail-Open 降级处理
            }
        } while (!TAIL_HANDLE.compareAndSet(this, currentTail, currentTail + 1));

        int index = (int) (currentTail & mask);
        SyncWaitSlot slot = array[index];
        slot.reset(uid, thread);
        return slot;
    }

    private boolean isFull(long currentTail) {
        if (currentTail - this.cachedHead >= array.length) {
            long freshHead = (long) HEAD_HANDLE.getAcquire(this);
            if (currentTail - freshHead >= array.length) {
                return true;
            }
            this.cachedHead = freshHead;
        }
        return false;
    }

    /**
     * SPSC 单消费者 (Netty EventLoop) 查看队头等待槽位 (Safe Zone 本地 cachedTail 保护)
     */
    public SyncWaitSlot peek() {
        long currentHead = (long) HEAD_HANDLE.getAcquire(this);
        if (isEmpty(currentHead)) {
            return null;
        }
        return array[(int) (currentHead & mask)];
    }

    /**
     * SPSC 单消费者 (Netty EventLoop) 弹出队头槽位并推进 head 指针
     */
    public SyncWaitSlot poll() {
        long currentHead = (long) HEAD_HANDLE.getAcquire(this);
        if (isEmpty(currentHead)) {
            return null;
        }
        int index = (int) (currentHead & mask);
        SyncWaitSlot slot = array[index];
        HEAD_HANDLE.setRelease(this, currentHead + 1);
        return slot;
    }

    private boolean isEmpty(long currentHead) {
        if (currentHead >= this.cachedTail) {
            long freshTail = (long) TAIL_HANDLE.getAcquire(this);
            if (currentHead >= freshTail) {
                return true;
            }
            this.cachedTail = freshTail;
        }
        return false;
    }
}
