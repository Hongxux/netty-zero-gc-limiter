package com.netty.limiter.limiter;

import io.netty.util.concurrent.FastThreadLocal;

import java.lang.invoke.MethodHandles;
import java.lang.invoke.VarHandle;

/**
 * =========================================================================================
 * 🚀 0-GC 无锁高性能 SyncWaitSlot 环形缓冲区
 *    (物理 Cache Line 隔离 + Safe Zone + FTL per-thread Slot + ZERO_SLOT COW 原子清空)
 *
 * 【内存屏障模型 Guarantee】：
 * 1. 56 字节 Cache Line 物理隔离：彻底切断 Consumer / Producer 跨核伪共享。
 * 2. Safe Zone 惰性序列号缓存：临界满/空时才触发 1 次跨核 Bus Sniffing。
 * 3. FTL per-thread SyncWaitSlot：每个生产者线程独占一个 Slot 实例，彻底消除槽位内部写竞争。
 * 4. ARRAY_VH Release-Acquire 发布屏障：
 *    - 生产者在 Slot 字段写完后以 ARRAY_VH.setRelease 发布引用 → Release Fence。
 *    - 消费者以 ARRAY_VH.getAcquire 读取引用 → Acquire Fence，建立完整 HB 契约。
 *    - SyncWaitSlot 内部字段均为 plain 字段，无需任何 VarHandle！
 * 5. status 可见性由 LockSupport.unpark → park Happens-Before 链保证。
 * 6. ZERO_SLOT COW 原子清空：poll() 以单次 ARRAY_VH.setRelease 将数组槽位替换为 ZERO_SLOT 哨兵。
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
     * 🎯 ZERO_SLOT 哨兵：标识数组槽位为空，COW 清空的原子占位对象，构造后永不修改其字段。
     */
    public static final SyncWaitSlot ZERO_SLOT = new SyncWaitSlot();

    /**
     * 所有字段均为 plain 字段（无 volatile / VarHandle 内部屏障）：
     * - userId / waiterThread：可见性由 ARRAY_VH Release-Acquire 屏障保证。
     * - status：可见性由 LockSupport.unpark → park Happens-Before 保证。
     */
    public static class SyncWaitSlot {
        public long userId;
        public int status;        // 0=pending, 1=passed, 2=blocked
        public Thread waiterThread;
    }

    private final SyncWaitSlot[] array;
    private final int mask;

    private static final VarHandle NEXT_NEEDED_ACK_SEQUENCE_HANDLE;
    private static final VarHandle NEXT_AVAILABLE_REQUEST_SEQUENCE_HANDLE;
    private static final VarHandle ARRAY_VH; // 数组元素引用的 VarHandle（唯一的跨线程内存屏障点）

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
     * MPSC 生产者：CAS 预占序列号，从 FTL 取出线程专属 Slot，写入 plain 字段后以 ARRAY_VH.setRelease 原子发布引用。
     * Plain 字段写入安全：FTL Slot 是线程私有的，只有本线程写入，无跨线程写竞争。
     */
    public SyncWaitSlot offer(long uid, Thread thread) {
        long currentAvailableReqSeq;
        do {
            currentAvailableReqSeq = (long) NEXT_AVAILABLE_REQUEST_SEQUENCE_HANDLE.getAcquire(this);
            if (isFull(currentAvailableReqSeq)) {
                return null; // 缓冲区满，Fail-Open 降级
            }
        } while (!NEXT_AVAILABLE_REQUEST_SEQUENCE_HANDLE.compareAndSet(this, currentAvailableReqSeq, currentAvailableReqSeq + 1));

        int index = (int) (currentAvailableReqSeq & mask);

        // 🎯 FTL 线程专属 Slot：plain 写入字段（无跨线程竞争），由下方 ARRAY_VH.setRelease 统一发布
        SyncWaitSlot slot = THREAD_SLOT.get();
        slot.userId = uid;
        slot.status = 0;
        slot.waiterThread = thread;

        // 🎯 单次 ARRAY_VH.setRelease = Release Fence：保证上方所有 plain 写对 Consumer getAcquire 可见
        ARRAY_VH.setRelease(array, index, slot);
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
     * SPSC Consumer peek：ARRAY_VH.getAcquire 有界自旋等待槽位非 ZERO_SLOT。
     * getAcquire 成功后，由 HB 契约保证 slot.userId / slot.waiterThread 对 Consumer 完全可见。
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

        // 🎯 COW 原子替换：单次 ARRAY_VH.setRelease 将槽位恢复为 ZERO_SLOT，为下一轮 offer 准备
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
