package com.netty.limiter.limiter;

import java.lang.invoke.MethodHandles;
import java.lang.invoke.VarHandle;

/**
 * =========================================================================================
 * 🚀 0-GC 无锁高性能 SyncWaitSlot 环形缓冲区 (物理 Cache Line 伪共享隔离 + Safe Zone 缓存 + 序列号发布屏障)
 * 
 * 【体系结构级微观物理优化与并发安全 Guarantee】：
 * 1. 56 字节 Cache Line 物理隔离: 彻底切断 Single-Consumer (Netty EventLoop) 与 Multi-Producer (网关请求线程) 间的 Cache Line 伪共享 (False Sharing)。
 * 2. Safe Zone 惰性读写序列号: 优先只读本地 L1 Cache，仅在临界满/空时触发 1 次跨核 Bus Sniffing 嗅探。
 * 3. 序列号发布屏障 (Slot Sequence Release Barrier):
 *    生产者 CAS 成功预占 `nextAvailableRequestSequence` 后，必须在完成槽位数据赋值后最后 volatile 写入 `slot.sequence`。
 *    消费者在 `peek` / `poll` 时校验 `slot.sequence == currentNeededAckSeq`，彻底消除 CAS 预占与槽位填充间极小时间窗口内的读写竞态！
 * =========================================================================================
 */
abstract class SyncWaitSlotRingBufferPad0 {
    protected long p00, p01, p02, p03, p04, p05, p06, p07;
}

abstract class SyncWaitSlotRingBufferConsumerFields extends SyncWaitSlotRingBufferPad0 {
    // nextNeededAckSequence: 消费者 (Netty EventLoop) 下一个急需 ACK 出队的序列号 (非 volatile，由 VarHandle 语义控制)
    protected long nextNeededAckSequence = 0;
    // cachedNextAvailableRequestSequence: 消费者本地 Safe Zone 缓存的生产者请求序列号 (普通 long，无需跨核嗅探)
    protected long cachedNextAvailableRequestSequence = 0;
}

abstract class SyncWaitSlotRingBufferPad1 extends SyncWaitSlotRingBufferConsumerFields {
    protected long p10, p11, p12, p13, p14, p15, p16, p17;
}

abstract class SyncWaitSlotRingBufferProducerFields extends SyncWaitSlotRingBufferPad1 {
    // nextAvailableRequestSequence: 生产者 (网关请求线程) 下一个可申请/预占的请求序列号 (非 volatile，由 VarHandle 语义控制)
    protected long nextAvailableRequestSequence = 0;
    // cachedNextNeededAckSequence: 生产者本地 Safe Zone 缓存的消费者确认序列号 (普通 long)
    protected long cachedNextNeededAckSequence = 0;
}

abstract class SyncWaitSlotRingBufferPad2 extends SyncWaitSlotRingBufferProducerFields {
    protected long p20, p21, p22, p23, p24, p25, p26, p27;
}

public class SyncWaitSlotRingBuffer extends SyncWaitSlotRingBufferPad2 {

    public static class SyncWaitSlot {
        public volatile long sequence = -1L; // 🎯 序列号发布屏障 (Publish Barrier)
        public volatile long userId;
        public volatile int status; // 0=pending, 1=passed, 2=blocked
        public volatile Thread waiterThread;

        public void reset(long seq, long uid, Thread thread) {
            this.userId = uid;
            this.status = 0;
            this.waiterThread = thread;
            // 🎯 最后 volatile 写入 sequence，作为 Release Barrier，保证 userId/waiterThread 对 Consumer 语义可见
            this.sequence = seq;
        }
    }

    private final SyncWaitSlot[] array;
    private final int mask;

    private static final VarHandle NEXT_NEEDED_ACK_SEQUENCE_HANDLE;
    private static final VarHandle NEXT_AVAILABLE_REQUEST_SEQUENCE_HANDLE;

    static {
        try {
            MethodHandles.Lookup l = MethodHandles.lookup();
            NEXT_NEEDED_ACK_SEQUENCE_HANDLE = l.findVarHandle(SyncWaitSlotRingBuffer.class, "nextNeededAckSequence", long.class);
            NEXT_AVAILABLE_REQUEST_SEQUENCE_HANDLE = l.findVarHandle(SyncWaitSlotRingBuffer.class, "nextAvailableRequestSequence", long.class);
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
        // 🎯 填充数据并发布 sequence 屏障
        slot.reset(currentAvailableReqSeq, uid, thread);
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
     * SPSC 单消费者 (Netty EventLoop) 查看队头等待槽位 (带 Sequence 校验，防止读取生产者半填充槽位)
     */
    public SyncWaitSlot peek() {
        long currentNeededAckSeq = (long) NEXT_NEEDED_ACK_SEQUENCE_HANDLE.getAcquire(this);
        if (isEmpty(currentNeededAckSeq)) {
            return null;
        }
        SyncWaitSlot slot = array[(int) (currentNeededAckSeq & mask)];
        // 🛡️ 发布屏障校验：如果 Producer CAS 占位成功但未完成 slot.reset(...)，视为暂不可用
        if (slot.sequence != currentNeededAckSeq) {
            return null;
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
        // 🛡️ 发布屏障校验
        if (slot.sequence != currentNeededAckSeq) {
            return null;
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
