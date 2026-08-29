package com.netty.limiter.limiter;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.lang.invoke.VarHandle;
import java.util.concurrent.atomic.AtomicBoolean;
import static org.junit.jupiter.api.Assertions.*;

public class UidRingBufferBenchmarkTest {

    public static void main(String[] args) throws InterruptedException {
        new UidRingBufferBenchmarkTest().testRingBufferPerformanceComparison();
    }

    @Test
    @DisplayName("对比压测 UidRingBuffer：Volatile SafeZone vs Non-Volatile SafeZone vs 经典全量跨核")
    public void testRingBufferPerformanceComparison() throws InterruptedException {
        int producerThreads = 16;
        int operationsPerThread = 1_000_000;
        long totalOps = (long) producerThreads * operationsPerThread;

        System.out.println("==========================================================================");
        System.out.println("  UidRingBuffer 三位一体并发压测 (MPSC 16 生产者线程 -> 1600 万次 offer/poll 操作)");
        System.out.println("==========================================================================");

        // Warmup JIT
        UidRingBuffer warmupBuffer = new UidRingBuffer(65536);
        runBenchmark(warmupBuffer, 4, 100_000);

        // 1. Safe Zone + Volatile cachedReadIndex
        UidRingBuffer volatileBuffer = new UidRingBuffer(65536);
        long volatileTimeNs = runBenchmark(volatileBuffer, producerThreads, operationsPerThread);
        double volatileQps = (totalOps / (volatileTimeNs / 1_000_000_000.0));

        System.out.println(String.format("[Volatile SafeZone 优化版]   耗时: %.2f ms | 吞吐量: %,.0f ops/sec (QPS)",
                volatileTimeNs / 1_000_000.0, volatileQps));

        // 2. Safe Zone + Non-Volatile cachedReadIndex (普通 long 变量)
        PlainCachedUidRingBuffer plainBuffer = new PlainCachedUidRingBuffer(65536);
        long plainTimeNs = runBenchmarkPlain(plainBuffer, producerThreads, operationsPerThread);
        double plainQps = (totalOps / (plainTimeNs / 1_000_000_000.0));

        System.out.println(String.format("[Non-Volatile 普通变量版]   耗时: %.2f ms | 吞吐量: %,.0f ops/sec (QPS)",
                plainTimeNs / 1_000_000.0, plainQps));

        // 3. Baseline 全量跨核读 (未优化)
        BaselineUidRingBuffer baselineBuffer = new BaselineUidRingBuffer(65536);
        long baselineTimeNs = runBenchmarkBaseline(baselineBuffer, producerThreads, operationsPerThread);
        double baselineQps = (totalOps / (baselineTimeNs / 1_000_000_000.0));

        System.out.println(String.format("[全量跨核读未优化版]        耗时: %.2f ms | 吞吐量: %,.0f ops/sec (QPS)",
                baselineTimeNs / 1_000_000.0, baselineQps));

        // 4. Unsafe 堆外裸指针 Zero-Bounds-Check 极限版 (MemorySegment 同款物理堆外内存)
        UidRingBuffer unsafeBuffer = new UidRingBuffer(65536);
        long unsafeTimeNs = runBenchmark(unsafeBuffer, producerThreads, operationsPerThread);
        double unsafeQps = (totalOps / (unsafeTimeNs / 1_000_000_000.0));
        unsafeBuffer.free();

        System.out.println(String.format("[Unsafe 单条出队极限版]      耗时: %.2f ms | 吞吐量: %,.0f ops/sec (QPS)",
                unsafeTimeNs / 1_000_000.0, unsafeQps));

        // 5. Unsafe + 批量写屏障稀释 (pollBatchAdaptive 静默 Flush) 终极版
        UidRingBuffer batchBuffer = new UidRingBuffer(65536);
        long batchTimeNs = runBenchmarkBatchAdaptive(batchBuffer, producerThreads, operationsPerThread);
        double batchQps = (totalOps / (batchTimeNs / 1_000_000_000.0));
        batchBuffer.free();

        System.out.println(String.format("[Unsafe + 批量稀释屏障极限版] 耗时: %.2f ms | 吞吐量: %,.0f ops/sec (QPS)",
                batchTimeNs / 1_000_000.0, batchQps));

        System.out.println("==========================================================================");
        assertTrue(volatileQps > 0);
    }

    private long runBenchmarkBatchAdaptive(UidRingBuffer buffer, int threads, int opsPerThread) throws InterruptedException {
        ExecutorService producers = Executors.newFixedThreadPool(threads);
        ExecutorService consumer = Executors.newSingleThreadExecutor();
        CountDownLatch startLatch = new CountDownLatch(1);
        CountDownLatch finishLatch = new CountDownLatch(threads);
        AtomicBoolean running = new AtomicBoolean(true);

        consumer.submit(() -> {
            long[] batch = new long[256];
            while (running.get()) {
                buffer.pollBatchAdaptive(batch, 32, 256, 100_000L);
                Thread.onSpinWait();
            }
            while (buffer.pollBatchAdaptive(batch, 1, 256, 100_000L) > 0) {
                // Drain remaining
            }
        });

        for (int t = 0; t < threads; t++) {
            final long baseUid = (t + 1) * 1000000L;
            producers.submit(() -> {
                try {
                    startLatch.await();
                    for (int i = 0; i < opsPerThread; i++) {
                        while (!buffer.offer(baseUid + i)) {
                            Thread.onSpinWait();
                        }
                    }
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                } finally {
                    finishLatch.countDown();
                }
            });
        }

        long start = System.nanoTime();
        startLatch.countDown();
        finishLatch.await();
        long elapsed = System.nanoTime() - start;

        running.set(false);
        producers.shutdown();
        consumer.shutdown();
        consumer.awaitTermination(5, java.util.concurrent.TimeUnit.SECONDS);
        return elapsed;
    }

    private long runBenchmarkUnsafe(UnsafeDirectMemoryUidRingBuffer buffer, int threads, int opsPerThread) throws InterruptedException {
        ExecutorService producers = Executors.newFixedThreadPool(threads);
        ExecutorService consumer = Executors.newSingleThreadExecutor();
        CountDownLatch startLatch = new CountDownLatch(1);
        CountDownLatch finishLatch = new CountDownLatch(threads);
        AtomicBoolean running = new AtomicBoolean(true);

        consumer.submit(() -> {
            while (running.get() || buffer.poll() > 0) {
                buffer.poll();
                Thread.onSpinWait();
            }
        });

        for (int t = 0; t < threads; t++) {
            final long baseUid = (t + 1) * 1000000L;
            producers.submit(() -> {
                try {
                    startLatch.await();
                    for (int i = 0; i < opsPerThread; i++) {
                        while (!buffer.offer(baseUid + i)) {
                            Thread.onSpinWait();
                        }
                    }
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                } finally {
                    finishLatch.countDown();
                }
            });
        }

        long start = System.nanoTime();
        startLatch.countDown();
        finishLatch.await();
        long elapsed = System.nanoTime() - start;

        running.set(false);
        producers.shutdown();
        consumer.shutdown();
        return elapsed;
    }

    public static class UnsafeDirectMemoryUidRingBuffer {
        private static final sun.misc.Unsafe UNSAFE;
        private final long address;
        private final int capacity;
        private final int mask;

        private static final VarHandle NEXT_NEEDED_ACK_SEQUENCE_HANDLE;
        private static final VarHandle NEXT_AVAILABLE_REQUEST_SEQUENCE_HANDLE;

        private long p00, p01, p02, p03, p04, p05, p06;
        private long nextNeededAckSequence = 0;
        private long cachedNextAvailableRequestSequence = 0;

        private long p10, p11, p12, p13, p14, p15, p16;
        private long nextAvailableRequestSequence = 0;

        private long p20, p21, p22, p23, p24, p25, p26;
        private long cachedNextNeededAckSequence = 0;

        private long p30, p31, p32, p33, p34, p35, p36;

        static {
            try {
                java.lang.reflect.Field f = sun.misc.Unsafe.class.getDeclaredField("theUnsafe");
                f.setAccessible(true);
                UNSAFE = (sun.misc.Unsafe) f.get(null);

                java.lang.invoke.MethodHandles.Lookup l = java.lang.invoke.MethodHandles.lookup();
                NEXT_NEEDED_ACK_SEQUENCE_HANDLE = l.findVarHandle(UnsafeDirectMemoryUidRingBuffer.class, "nextNeededAckSequence", long.class);
                NEXT_AVAILABLE_REQUEST_SEQUENCE_HANDLE = l.findVarHandle(UnsafeDirectMemoryUidRingBuffer.class, "nextAvailableRequestSequence", long.class);
            } catch (Exception e) {
                throw new ExceptionInInitializerError(e);
            }
        }

        public UnsafeDirectMemoryUidRingBuffer(int capacity) {
            this.capacity = capacity;
            this.mask = capacity - 1;
            this.address = UNSAFE.allocateMemory((long) capacity * 8L);
            UNSAFE.setMemory(this.address, (long) capacity * 8L, (byte) 0);
        }

        public boolean offer(long uid) {
            long currentAvailableReqSeq;
            do {
                currentAvailableReqSeq = (long) NEXT_AVAILABLE_REQUEST_SEQUENCE_HANDLE.getAcquire(this);
                if (isFull(currentAvailableReqSeq)) {
                    return false;
                }
            } while (!NEXT_AVAILABLE_REQUEST_SEQUENCE_HANDLE.compareAndSet(this, currentAvailableReqSeq, currentAvailableReqSeq + 1));

            long offset = address + ((currentAvailableReqSeq & mask) << 3);
            UNSAFE.putLongVolatile(null, offset, uid);
            return true;
        }

        private boolean isFull(long currentAvailableReqSeq) {
            if (currentAvailableReqSeq - this.cachedNextNeededAckSequence >= capacity) {
                long freshNeededAckSeq = (long) NEXT_NEEDED_ACK_SEQUENCE_HANDLE.getAcquire(this);
                if (currentAvailableReqSeq - freshNeededAckSeq >= capacity) {
                    return true;
                }
                this.cachedNextNeededAckSequence = freshNeededAckSeq;
            }
            return false;
        }

        public long poll() {
            long currentNeededAckSeq = (long) NEXT_NEEDED_ACK_SEQUENCE_HANDLE.getAcquire(this);
            if (isEmpty(currentNeededAckSeq)) {
                return 0L;
            }

            long offset = address + ((currentNeededAckSeq & mask) << 3);
            long uid;
            while ((uid = UNSAFE.getLongVolatile(null, offset)) == 0L) {
                Thread.onSpinWait();
            }

            UNSAFE.putLongVolatile(null, offset, 0L);
            NEXT_NEEDED_ACK_SEQUENCE_HANDLE.setRelease(this, currentNeededAckSeq + 1);
            return uid;
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

        public void free() {
            UNSAFE.freeMemory(address);
        }
    }

    private long runBenchmark(UidRingBuffer buffer, int threads, int opsPerThread) throws InterruptedException {
        ExecutorService producers = Executors.newFixedThreadPool(threads);
        ExecutorService consumer = Executors.newSingleThreadExecutor();
        CountDownLatch startLatch = new CountDownLatch(1);
        CountDownLatch finishLatch = new CountDownLatch(threads);
        AtomicBoolean running = new AtomicBoolean(true);

        consumer.submit(() -> {
            while (running.get() || buffer.poll() > 0) {
                buffer.poll();
                Thread.onSpinWait();
            }
        });

        for (int t = 0; t < threads; t++) {
            final long baseUid = (t + 1) * 1000000L;
            producers.submit(() -> {
                try {
                    startLatch.await();
                    for (int i = 0; i < opsPerThread; i++) {
                        while (!buffer.offer(baseUid + i)) {
                            Thread.onSpinWait();
                        }
                    }
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                } finally {
                    finishLatch.countDown();
                }
            });
        }

        long start = System.nanoTime();
        startLatch.countDown();
        finishLatch.await();
        long elapsed = System.nanoTime() - start;

        running.set(false);
        producers.shutdown();
        consumer.shutdown();
        consumer.awaitTermination(5, java.util.concurrent.TimeUnit.SECONDS);
        return elapsed;
    }

    private long runBenchmarkPlain(PlainCachedUidRingBuffer buffer, int threads, int opsPerThread) throws InterruptedException {
        ExecutorService producers = Executors.newFixedThreadPool(threads);
        ExecutorService consumer = Executors.newSingleThreadExecutor();
        CountDownLatch startLatch = new CountDownLatch(1);
        CountDownLatch finishLatch = new CountDownLatch(threads);
        AtomicBoolean running = new AtomicBoolean(true);

        consumer.submit(() -> {
            while (running.get() || buffer.poll() > 0) {
                buffer.poll();
                Thread.onSpinWait();
            }
        });

        for (int t = 0; t < threads; t++) {
            final long baseUid = (t + 1) * 1000000L;
            producers.submit(() -> {
                try {
                    startLatch.await();
                    for (int i = 0; i < opsPerThread; i++) {
                        while (!buffer.offer(baseUid + i)) {
                            Thread.onSpinWait();
                        }
                    }
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                } finally {
                    finishLatch.countDown();
                }
            });
        }

        long start = System.nanoTime();
        startLatch.countDown();
        finishLatch.await();
        long elapsed = System.nanoTime() - start;

        running.set(false);
        producers.shutdown();
        consumer.shutdown();
        return elapsed;
    }

    private long runBenchmarkBaseline(BaselineUidRingBuffer buffer, int threads, int opsPerThread) throws InterruptedException {
        ExecutorService producers = Executors.newFixedThreadPool(threads);
        ExecutorService consumer = Executors.newSingleThreadExecutor();
        CountDownLatch startLatch = new CountDownLatch(1);
        CountDownLatch finishLatch = new CountDownLatch(threads);
        AtomicBoolean running = new AtomicBoolean(true);

        consumer.submit(() -> {
            while (running.get() || buffer.poll() > 0) {
                buffer.poll();
                Thread.onSpinWait();
            }
        });

        for (int t = 0; t < threads; t++) {
            final long baseUid = (t + 1) * 1000000L;
            producers.submit(() -> {
                try {
                    startLatch.await();
                    for (int i = 0; i < opsPerThread; i++) {
                        while (!buffer.offer(baseUid + i)) {
                            Thread.onSpinWait();
                        }
                    }
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                } finally {
                    finishLatch.countDown();
                }
            });
        }

        long start = System.nanoTime();
        startLatch.countDown();
        finishLatch.await();
        long elapsed = System.nanoTime() - start;

        running.set(false);
        producers.shutdown();
        consumer.shutdown();
        return elapsed;
    }

    // 普通 non-volatile long 版本的 Safe Zone 实现
    private static class PlainCachedUidRingBuffer {
        private final long[] ringBuffer;
        private final int capacity;
        private final int mask;

        private static final java.lang.invoke.VarHandle READ_INDEX_HANDLE;
        private static final java.lang.invoke.VarHandle WRITE_INDEX_HANDLE;
        private static final java.lang.invoke.VarHandle ARRAY_HANDLE;

        private long p00, p01, p02, p03, p04, p05, p06;
        private long readIndex = 0;
        private long p10, p11, p12, p13, p14, p15, p16;
        private long writeIndex = 0;
        private long p20, p21, p22, p23, p24, p25, p26;

        // 非 volatile 的普通 long
        private long cachedReadIndex = 0;

        private long p30, p31, p32, p33, p34, p35, p36;

        static {
            try {
                java.lang.invoke.MethodHandles.Lookup l = java.lang.invoke.MethodHandles.lookup();
                READ_INDEX_HANDLE = l.findVarHandle(PlainCachedUidRingBuffer.class, "readIndex", long.class);
                WRITE_INDEX_HANDLE = l.findVarHandle(PlainCachedUidRingBuffer.class, "writeIndex", long.class);
                ARRAY_HANDLE = java.lang.invoke.MethodHandles.arrayElementVarHandle(long[].class);
            } catch (ReflectiveOperationException e) {
                throw new ExceptionInInitializerError(e);
            }
        }

        public PlainCachedUidRingBuffer(int capacity) {
            this.capacity = capacity;
            this.mask = capacity - 1;
            this.ringBuffer = new long[capacity];
        }

        public boolean offer(long uid) {
            long currentWriteIndex;
            long currentCachedReadIndex = this.cachedReadIndex;

            do {
                currentWriteIndex = (long) WRITE_INDEX_HANDLE.getAcquire(this);

                if (currentWriteIndex - currentCachedReadIndex >= capacity) {
                    long freshReadIndex = (long) READ_INDEX_HANDLE.getAcquire(this);
                    if (currentWriteIndex - freshReadIndex >= capacity) {
                        return false;
                    }
                    currentCachedReadIndex = freshReadIndex;
                    this.cachedReadIndex = freshReadIndex;
                }
            } while (!WRITE_INDEX_HANDLE.compareAndSet(this, currentWriteIndex, currentWriteIndex + 1));

            int index = (int) (currentWriteIndex & mask);
            ARRAY_HANDLE.setRelease(ringBuffer, index, uid);
            return true;
        }

        public long poll() {
            long currentReadIndex = (long) READ_INDEX_HANDLE.getAcquire(this);
            long currentWriteIndex = (long) WRITE_INDEX_HANDLE.getAcquire(this);
            if (currentReadIndex >= currentWriteIndex) return 0L;

            int index = (int) (currentReadIndex & mask);
            long uid;
            while ((uid = (long) ARRAY_HANDLE.getAcquire(ringBuffer, index)) == 0L) {
                Thread.onSpinWait();
            }
            ARRAY_HANDLE.setRelease(ringBuffer, index, 0L);
            READ_INDEX_HANDLE.setRelease(this, currentReadIndex + 1);
            return uid;
        }
    }

    // Baseline 实现 (模拟每次 offer 都强制 getAcquire(readIndex))
    private static class BaselineUidRingBuffer {
        private final long[] ringBuffer;
        private final int capacity;
        private final int mask;

        private static final java.lang.invoke.VarHandle READ_INDEX_HANDLE;
        private static final java.lang.invoke.VarHandle WRITE_INDEX_HANDLE;
        private static final java.lang.invoke.VarHandle ARRAY_HANDLE;

        private long p00, p01, p02, p03, p04, p05, p06;
        private long readIndex = 0;
        private long p10, p11, p12, p13, p14, p15, p16;
        private long writeIndex = 0;
        private long p20, p21, p22, p23, p24, p25, p26;

        static {
            try {
                java.lang.invoke.MethodHandles.Lookup l = java.lang.invoke.MethodHandles.lookup();
                READ_INDEX_HANDLE = l.findVarHandle(BaselineUidRingBuffer.class, "readIndex", long.class);
                WRITE_INDEX_HANDLE = l.findVarHandle(BaselineUidRingBuffer.class, "writeIndex", long.class);
                ARRAY_HANDLE = java.lang.invoke.MethodHandles.arrayElementVarHandle(long[].class);
            } catch (ReflectiveOperationException e) {
                throw new ExceptionInInitializerError(e);
            }
        }

        public BaselineUidRingBuffer(int capacity) {
            this.capacity = capacity;
            this.mask = capacity - 1;
            this.ringBuffer = new long[capacity];
        }

        public boolean offer(long uid) {
            long currentWriteIndex;
            long currentReadIndex;
            do {
                currentWriteIndex = (long) WRITE_INDEX_HANDLE.getAcquire(this);
                // 每次都强行跨核读取 readIndex (无 Safe Zone 缓存)
                currentReadIndex = (long) READ_INDEX_HANDLE.getAcquire(this);

                if (currentWriteIndex - currentReadIndex >= capacity) {
                    return false;
                }
            } while (!WRITE_INDEX_HANDLE.compareAndSet(this, currentWriteIndex, currentWriteIndex + 1));

            int index = (int) (currentWriteIndex & mask);
            ARRAY_HANDLE.setRelease(ringBuffer, index, uid);
            return true;
        }

        public long poll() {
            long currentReadIndex = (long) READ_INDEX_HANDLE.getAcquire(this);
            long currentWriteIndex = (long) WRITE_INDEX_HANDLE.getAcquire(this);
            if (currentReadIndex >= currentWriteIndex) return 0L;

            int index = (int) (currentReadIndex & mask);
            long uid;
            while ((uid = (long) ARRAY_HANDLE.getAcquire(ringBuffer, index)) == 0L) {
                Thread.onSpinWait();
            }
            ARRAY_HANDLE.setRelease(ringBuffer, index, 0L);
            READ_INDEX_HANDLE.setRelease(this, currentReadIndex + 1);
            return uid;
        }
    }
}
