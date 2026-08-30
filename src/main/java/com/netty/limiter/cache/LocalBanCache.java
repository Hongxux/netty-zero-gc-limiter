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
    public static final long WARNED_EXP_SEC_MARK = (-2L) & 0xFFFFFFFFL; // 0xFFFFFFFEL, 对应 ExpSec = -2L

    public static final int BAN_STATUS_PASSED = 0;
    public static final int BAN_STATUS_HARD_BANNED = 1;
    public static final int BAN_STATUS_WARNED_SYNC_REQUIRED = 2;

    public static boolean isLiveEntry(long entry) {
        return entry != EMPTY && entry != TOMBSTONE;
    }

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
     * 🚀 极速获取 UID 封禁状态:
     * 0 -> 未封禁 (BAN_STATUS_PASSED, 可走 0-GC 异步非阻塞上报)
     * 1 -> 硬封禁 (BAN_STATUS_HARD_BANNED, 直接拒绝 403)
     * 2 -> 80% 水位降级预警 (BAN_STATUS_WARNED_SYNC_REQUIRED, ExpSec == -2L，必须走同步上报等待 Redis Ack)
     */
    public int getUserBanStatus(long userId) {
        if (userId <= 0) return BAN_STATUS_PASSED;

        int baseIndex = (int) (mixHash(userId) & MASK);
        long nowSec = System.currentTimeMillis() / 1000;

        TablePair pair = getTablesAcquire();
        TableHolder h = pair.hot;

        // ① 查询 Hot Table
        for (int i = 0; i < MAX_PROBE; i++) {
            int index = (baseIndex + i) & MASK;
            long entry = h.getEntryAcquire(index);

            if (entry == EMPTY) {
                break;
            }
            if (entry != TOMBSTONE) {
                long storedUid = unpackUid(entry);
                if (storedUid == userId) {
                    long expireTimeSec = unpackExpSec(entry);
                    if (expireTimeSec == WARNED_EXP_SEC_MARK) {
                        return BAN_STATUS_WARNED_SYNC_REQUIRED;
                    }
                    if (expireTimeSec > nowSec) {
                        return BAN_STATUS_HARD_BANNED;
                    } else {
                        h.casEntry(index, entry, TOMBSTONE);
                        return BAN_STATUS_PASSED;
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
                    if (expireTimeSec == WARNED_EXP_SEC_MARK) {
                        return BAN_STATUS_WARNED_SYNC_REQUIRED;
                    }
                    if (expireTimeSec > nowSec) {
                        if (c.casEntry(index, entry, TOMBSTONE)) {
                            putUserBanWithExactExpSec(userId, expireTimeSec);
                        }
                        return BAN_STATUS_HARD_BANNED;
                    } else {
                        c.casEntry(index, entry, TOMBSTONE);
                        return BAN_STATUS_PASSED;
                    }
                }
            }
        }

        return BAN_STATUS_PASSED;
    }

    /**
     * 🚀 单指令 64-bit 解包极速查询: 判断 UID 是否被硬封禁
     */
    public boolean isUserBanned(long userId) {
        return getUserBanStatus(userId) == BAN_STATUS_HARD_BANNED;
    }

    /**
     * 🚀 80% 水位线预警标记: 将 ExpSec 设为特殊标记 WARNED_EXP_SEC_MARK (-2L)
     * 处于该状态的 UID 后续请求将触发【同步上报 Redis 校验】
     */
    public void putUserWarned(long userId) {
        if (userId <= 0) return;
        putUserBanWithExactExpSec(userId, WARNED_EXP_SEC_MARK);
    }

    public void putUserBanWithExactExpSec(long userId, long expireTimeSec) {
        if (userId <= 0) return;
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

            if (isLiveEntry(entry)) {
                // ① 已有节点: 检查是否相同 UID，匹配则覆写更新
                if (tryUpdateExistingEntryIfSameUid(h, index, entry, userId, packed)) {
                    return;
                }
            } else {
                // ② 快路径插入: 尝试单条 CAS 抢占空槽位/墓碑
                if (tryInsert(h, index, entry, packed)) {
                    return;
                }
                // ③ CAS 争用确认: 若抢占失败，确认是否由并发线程抢先写入了相同 UID
                if (ifTheSameUidTryUpdate(h, index, userId, packed)) {
                    return;
                }
            }
        }

        // 🛡️ 兜底策略: 16 次探查全满或争用失败后退回 baseIndex 强制写入，确保黑名单 100% 落地
        fallbackForceSetEntry(h, baseIndex, packed);
    }

    /**
     * 🚀 兜底强行 Volatile 写入 (Fail-Secure Fallback Insertion):
     * 当探查 MAX_PROBE (16) 次全满/争用失败时，退回首个哈希槽 baseIndex 强行 Release 写入 packed，保证黑名单 100% 落地
     */
    private static void fallbackForceSetEntry(TableHolder h, int baseIndex, long packed) {
        h.setEntryRelease(baseIndex & MASK, packed);
    }

    /**
     * 🚀 已有节点覆写判定: 校验槽位 entry 是否匹配相同 UID；匹配则使用 CAS 覆写更新 ExpSec 时间戳
     */
    private static boolean tryUpdateExistingEntryIfSameUid(TableHolder h, int index, long entry, long userId, long packed) {
        long storedUid = unpackUid(entry);
        if (storedUid == userId) {
            return h.casEntry(index, entry, packed);
        }
        return false;
    }


    /**
     * 🚀 尝试单条 CAS 抢占空槽位/墓碑写入新节点 (若成功则增加 count 节点计数)
     */
    private static boolean tryInsert(TableHolder h, int index, long expectedEntry, long packed) {
        if (h.casEntry(index, expectedEntry, packed)) {
            h.count.incrementAndGet();
            return true;
        }
        return false;
    }

    /**
     * 🚀 单条 64-bit CAS 抢占写入: 封禁 UID (自动打入 Hot Table + 满阈值自动双表轮转)
     */
    public void putUserBan(long userId, long durationSeconds) {
        if (userId <= 0) return;
        long nowSec = System.currentTimeMillis() / 1000;
        putUserBanWithExactExpSec(userId, nowSec + durationSeconds);
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

    /**
     * 🚀 并发 CAS 争用二次确认 (Same-UID Slot Contention Check):
     *
     * 【在确认什么？】
     * 确认当快路径 CAS 抢占空槽位/墓碑失败时，导致失败的原因【是否是因为另一个并发线程抢先写入了相同的 UID】。
     *
     * 1. 场景：线程 A 发现槽位 i 为 EMPTY，准备 CAS 写入 UID=10086；但微秒间隙内线程 B 抢先 CAS 写入成功。
     * 2. 确认：线程 A 在 CAS 失败后，二次读取该槽位 entry，若确认 `unpackUid(entry) == userId`（说明线程 B 写的也是 10086）；
     * 3. 动作：说明槽位已被该 UID 成功占领，无需重复增加 `count` 节点计数，使用 CAS 原子更新 ExpSec 时间戳并返回 true！
     * 4. 否决：若二次确认发现是其他不同的 UID 抢占，则返回 false，外层循环继续探查 (Probe) 下一个槽位。
     */
    private static boolean ifTheSameUidTryUpdate(TableHolder h, int index, long userId, long packed) {
        long entry = h.getEntryAcquire(index);
        if (isLiveEntry(entry) && unpackUid(entry) == userId) {
            return h.casEntry(index, entry, packed);
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
