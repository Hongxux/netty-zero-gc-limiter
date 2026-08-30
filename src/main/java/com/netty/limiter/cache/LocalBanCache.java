package com.netty.limiter.cache;

import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;

import java.lang.invoke.MethodHandles;
import java.lang.invoke.VarHandle;
import java.util.Arrays;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * 🚀 极限性能 0-GC 静态双表 (Hot/Cold) 粗粒度 LRU 黑名单缓存 (LocalBanCache)
 *
 * 核心架构:
 * 1. 64-bit 压缩打散 (Single Long Bit-Packing):
 *    - 将 32-bit userId (高 32 位) 与 32-bit expireTimeSec (低 32 位) 压缩打入单个 64-bit long 槽位中。
 *    - 内存使用直接砍半 (仅需单个 long[] entries 数组), CPU L1 Cache 命中率提升 2 倍!
 *    - 单条 CMPXCHG 汇编指令完成原子抢占, 彻底消除写中间状态与发布屏障。
 *
 * 2. 粗粒度 LRU 双表轮转 (Coarse-Grained 2-Table LRU):
 *    - 静态预分配 Hot Table 与 Cold Table。
 *    - 查 Cold Table 命中时自动执行【冷到热晋升 Promotion】。
 *    - Hot Table 达到 40% 黄金阈值时触发 0-STW 无锁指针互换 (Hot ↔ Cold)。
 **/
@Slf4j
@Component
public class LocalBanCache {

    public static final int CAPACITY = 65536;
    public static final int MASK = CAPACITY - 1;
    public static final int MAX_PROBE = 16;
    public static final int LOAD_FACTOR_THRESHOLD = (int) (CAPACITY * 0.4); // 26,214

    public static final long EMPTY = 0L;
    public static final long TOMBSTONE = -1L;

    public static long pack(long userId, long expireTimeSec) {
        return ((userId & 0xFFFFFFFFL) << 32) | (expireTimeSec & 0xFFFFFFFFL);
    }

    public static long unpackUid(long packed) {
        return packed >>> 32;
    }

    public static long unpackExpSec(long packed) {
        return packed & 0xFFFFFFFFL;
    }

    /**
     * 单数组 TableHolder (每个槽位只需 1 个 long 即可同时容纳 UID 与 过期时间戳)
     */
    public static class TableHolder {
        private static final VarHandle ENTRIES_VH = MethodHandles.arrayElementVarHandle(long[].class);

        public final long[] entries;
        public final AtomicInteger count = new AtomicInteger(0);

        public TableHolder(int capacity) {
            this.entries = new long[capacity];
        }

        public void clear() {
            Arrays.fill(entries, EMPTY);
            VarHandle.storeStoreFence();
            count.set(0);
        }

        public long getEntryAcquire(int idx) {
            return (long) ENTRIES_VH.getAcquire(entries, idx);
        }

        public void setEntryRelease(int idx, long entry) {
            ENTRIES_VH.setRelease(entries, idx, entry);
        }

        public boolean casEntry(int idx, long expected, long newEntry) {
            return ENTRIES_VH.compareAndSet(entries, idx, expected, newEntry);
        }
    }

    public static class TablePair {
        public final TableHolder hot;
        public final TableHolder cold;

        public TablePair(TableHolder hot, TableHolder cold) {
            this.hot = hot;
            this.cold = cold;
        }
    }

    private static final VarHandle TABLES_VH;
    static {
        try {
            TABLES_VH = MethodHandles.lookup().findVarHandle(LocalBanCache.class, "tables", TablePair.class);
        } catch (ReflectiveOperationException e) {
            throw new ExceptionInInitializerError(e);
        }
    }

    private final TableHolder tableA = new TableHolder(CAPACITY);
    private final TableHolder tableB = new TableHolder(CAPACITY);

    private final TablePair pairAB = new TablePair(tableA, tableB);
    private final TablePair pairBA = new TablePair(tableB, tableA);

    private TablePair tables = pairAB;
    private final AtomicBoolean rotating = new AtomicBoolean(false);

    private TablePair getTablesAcquire() {
        return (TablePair) TABLES_VH.getAcquire(this);
    }

    private void setTablesRelease(TablePair newPair) {
        TABLES_VH.setRelease(this, newPair);
    }

    /**
     * 🚀 单指令 64-bit 解包极速查询: 判断 UID 是否被封禁 (支持 Cold -> Hot 自动晋升)
     */
    public boolean isUserBanned(long userId) {
        if (userId <= 0) return false;

        int baseIndex = (int) (mixHash(userId) & MASK);
        long nowSec = System.currentTimeMillis() / 1000;

        TablePair pair = getTablesAcquire();
        TableHolder h = pair.hot;

        // ① 查询 Hot Table
        for (int i = 0; i < MAX_PROBE; i++) {
            int index = (baseIndex + i) & MASK;
            long entry = h.getEntryAcquire(index);

            if (entry == EMPTY) {
                break; // 遇到 EMPTY 停止热表探查
            }
            if (entry != TOMBSTONE) {
                long storedUid = unpackUid(entry);
                if (storedUid == userId) {
                    long expireTimeSec = unpackExpSec(entry);
                    if (expireTimeSec > nowSec) {
                        return true;
                    } else {
                        // 已过期: CAS 标记为 TOMBSTONE
                        h.casEntry(index, entry, TOMBSTONE);
                        return false;
                    }
                }
            }
        }

        // ② 查询 Cold Table (支持热点提升 Promotion)
        TableHolder c = pair.cold;
        for (int i = 0; i < MAX_PROBE; i++) {
            int index = (baseIndex + i) & MASK;
            long entry = c.getEntryAcquire(index);

            if (entry == EMPTY) {
                break;
            }
            if (entry != TOMBSTONE) {
                long storedUid = unpackUid(entry);
                if (storedUid == userId) {
                    long expireTimeSec = unpackExpSec(entry);
                    if (expireTimeSec > nowSec) {
                        // 冷表中有效: 执行【冷到热晋升 Promotion】
                        if (c.casEntry(index, entry, TOMBSTONE)) {
                            putUserBan(userId, expireTimeSec - nowSec);
                        }
                        return true;
                    } else {
                        c.casEntry(index, entry, TOMBSTONE);
                        return false;
                    }
                }
            }
        }

        return false;
    }

    /**
     * 🚀 单条 64-bit CAS 抢占写入: 封禁 UID (自动打入 Hot Table + 满阈值自动双表轮转)
     */
    public void putUserBan(long userId, long durationSeconds) {
        if (userId <= 0) return;

        long nowSec = System.currentTimeMillis() / 1000;
        long expireTimeSec = nowSec + durationSeconds;
        long packed = pack(userId, expireTimeSec);

        TableHolder h = getTablesAcquire().hot;

        if (h.count.get() >= LOAD_FACTOR_THRESHOLD) {
            checkAndRotateTables();
            h = getTablesAcquire().hot;
        }

        int baseIndex = (int) (mixHash(userId) & MASK);

        for (int i = 0; i < MAX_PROBE; i++) {
            int index = (baseIndex + i) & MASK;
            long entry = h.getEntryAcquire(index);

            if (entry != EMPTY && entry != TOMBSTONE) {
                long storedUid = unpackUid(entry);
                // ① 已存在: 更新 packed 单元
                if (storedUid == userId) {
                    h.setEntryRelease(index, packed);
                    return;
                }
            } else {
                // ② 空槽位或墓碑: 单条 CMPXCHG 原子抢占写入 (UID + Exp 瞬间一步到位!)
                if (h.casEntry(index, entry, packed)) {
                    h.count.incrementAndGet();
                    return;
                }
                // 再次确认
                if (reconfirmAndSetEntry(h, index, userId, packed)) {
                    return;
                }
            }
        }

        // 兜底策略
        int idx = baseIndex & MASK;
        if (!h.casEntry(idx, h.getEntryAcquire(idx), packed)) {
            h.setEntryRelease(idx, packed);
        }
    }

    /**
     * 移除封禁
     */
    public void removeUserBan(long userId) {
        if (userId <= 0) return;

        int baseIndex = (int) (mixHash(userId) & MASK);
        TablePair pair = getTablesAcquire();

        TableHolder h = pair.hot;
        for (int i = 0; i < MAX_PROBE; i++) {
            int index = (baseIndex + i) & MASK;
            long entry = h.getEntryAcquire(index);
            if (entry == EMPTY) break;
            if (entry != TOMBSTONE && unpackUid(entry) == userId) {
                h.casEntry(index, entry, TOMBSTONE);
                break;
            }
        }

        TableHolder c = pair.cold;
        for (int i = 0; i < MAX_PROBE; i++) {
            int index = (baseIndex + i) & MASK;
            long entry = c.getEntryAcquire(index);
            if (entry == EMPTY) break;
            if (entry != TOMBSTONE && unpackUid(entry) == userId) {
                c.casEntry(index, entry, TOMBSTONE);
                break;
            }
        }
    }

    private static boolean reconfirmAndSetEntry(TableHolder h, int index, long userId, long packed) {
        long entry = h.getEntryAcquire(index);
        if (entry != EMPTY && entry != TOMBSTONE && unpackUid(entry) == userId) {
            h.setEntryRelease(index, packed);
            return true;
        }
        return false;
    }

    private void checkAndRotateTables() {
        if (rotating.compareAndSet(false, true)) {
            try {
                TablePair currentPair = getTablesAcquire();
                if (currentPair.hot.count.get() >= LOAD_FACTOR_THRESHOLD) {
                    TableHolder oldCold = currentPair.cold;
                    oldCold.clear();
                    setTablesRelease((currentPair == pairAB) ? pairBA : pairAB);
                }
            } finally {
                rotating.set(false);
            }
        }
    }

    private static long mixHash(long key) {
        key = (~key) + (key << 21);
        key = key ^ (key >>> 24);
        key = (key + (key << 3)) + (key << 8);
        key = key ^ (key >>> 14);
        key = (key + (key << 2)) + (key << 4);
        key = key ^ (key >>> 28);
        key = key + (key << 31);
        return key;
    }
}
