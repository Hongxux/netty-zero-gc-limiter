package com.netty.limiter.cache;

import com.netty.limiter.util.XxHash64Util;
import io.netty.buffer.ByteBuf;

import java.lang.invoke.MethodHandles;
import java.lang.invoke.VarHandle;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * 极限性能 JWT 签名 0-GC 静态 TablePair 全 VarHandle Acquire/Release 缓存单例 (Enum Singleton)
 *
 * 核心架构优化 (分层交错布局 - Layered Interleaved Memory Layout):
 * 1. keyPrefixes 数组 (2-Long 交错): [2*i -> key] [2*i+1 -> sigPrefix]。
 *    - 探查域 (Probe Domain): 单条 64-Byte CPU L1 Cache Line 完美容纳 4 组 (key + sigPrefix)。
 *    - 线性探查时不会触碰 Value 内存，提升 L1 Cache Line 密度与硬件预取效率!
 *
 * 2. valExps 数组 (独立 Value 域): [i -> packed(UID, ExpSec)]。
 *    - 数据域 (Value Domain): 物理内存解耦隔离。写线程更新 valExps 绝不会清空或失效 keyPrefixes 的 CPU 缓存行 (彻底消除 False Sharing)!
 *
 * 3. 100% 0 堆内存分配与 0-STW 无锁双表轮转。
 **/
public enum JwtSigUidCache {

    /**
     * 唯一单例实例
     */
    INSTANCE;

    public static final int CAPACITY = 65536;
    public static final int MASK = CAPACITY - 1;
    public static final int MAX_PROBE = 8;
    public static final int MAX_SPIN = 64;
    public static final int LOAD_FACTOR_THRESHOLD = (int) (CAPACITY * 0.4); // 26,214

    public static final int KEY_PREFIX_STRIDE = 2;

    public static final long EMPTY = 0L;
    public static final long TOMBSTONE = -1L;

    public static boolean isLive(long key) {
        return key != EMPTY && key != TOMBSTONE;
    }

    public static long packValExp(long uid, long expSec) {
        return ((uid & 0xFFFFFFFFL) << 32) | (expSec & 0xFFFFFFFFL);
    }

    public static long unpackUid(long valExp) {
        return valExp >>> 32;
    }

    public static long unpackExpSec(long valExp) {
        return valExp & 0xFFFFFFFFL;
    }

    /**
     * 分层交错物理存储容器 (keyPrefixes 交错探查域 + valExps 独立 Value 域)
     */
    public static class TableHolder {
        private static final VarHandle KEY_PREFIXES_VH = MethodHandles.arrayElementVarHandle(long[].class);
        private static final VarHandle VAL_EXPS_VH = MethodHandles.arrayElementVarHandle(long[].class);

        public final long[] keyPrefixes; // 交错探查域: [2*i -> key, 2*i+1 -> sigPrefix]
        public final long[] valExps;     // 独立 Value 域: [i -> packed(UID, ExpSec)]
        public final AtomicInteger count = new AtomicInteger(0);

        public TableHolder(int capacity) {
            this.keyPrefixes = new long[capacity * KEY_PREFIX_STRIDE];
            this.valExps = new long[capacity];
        }

        /**
         * SIMD 向量化安全清空
         */
        public void clear() {
            Arrays.fill(keyPrefixes, EMPTY);
            VarHandle.storeStoreFence();
            Arrays.fill(valExps, 0L);
            VarHandle.storeStoreFence();
            count.set(0);
        }

        // ① Key 读取/写入/CAS
        public long getKeyAcquire(int idx) {
            return (long) KEY_PREFIXES_VH.getAcquire(keyPrefixes, idx * KEY_PREFIX_STRIDE);
        }

        public void setKeyRelease(int idx, long key) {
            KEY_PREFIXES_VH.setRelease(keyPrefixes, idx * KEY_PREFIX_STRIDE, key);
        }

        public boolean casKey(int idx, long expected, long newKey) {
            return KEY_PREFIXES_VH.compareAndSet(keyPrefixes, idx * KEY_PREFIX_STRIDE, expected, newKey);
        }

        // ② Signature Prefix 读取/写入 (普通写 Plain Store + 依靠 valExp 作为 Release 屏障发布)
        public long getPrefixAcquire(int idx) {
            return (long) KEY_PREFIXES_VH.getAcquire(keyPrefixes, idx * KEY_PREFIX_STRIDE + 1);
        }

        public void setPrefix(int idx, long prefix) {
            KEY_PREFIXES_VH.set(keyPrefixes, idx * KEY_PREFIX_STRIDE + 1, prefix);
        }

        public void setPrefixRelease(int idx, long prefix) {
            KEY_PREFIXES_VH.setRelease(keyPrefixes, idx * KEY_PREFIX_STRIDE + 1, prefix);
        }

        // ③ ValExp (UID + ExpSec) 读取/写入
        public long getValExpAcquire(int idx) {
            return (long) VAL_EXPS_VH.getAcquire(valExps, idx);
        }

        public void setValExpRelease(int idx, long valExp) {
            VAL_EXPS_VH.setRelease(valExps, idx, valExp);
        }
    }

    /**
     * 双表组合引用容器
     */
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
            TABLES_VH = MethodHandles.lookup().findVarHandle(JwtSigUidCache.class, "tables", TablePair.class);
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

    public static long normalizeKey(long hash) {
        if (hash == EMPTY || hash == TOMBSTONE) {
            return 1L;
        }
        return hash;
    }

    private TablePair getTablesAcquire() {
        return (TablePair) TABLES_VH.getAcquire(this);
    }

    private void setTablesRelease(TablePair newPair) {
        TABLES_VH.setRelease(this, newPair);
    }

    /**
     * 校验槽位 Key 是否在自旋期间被并发覆盖或失效 (Liveness Guard)
     */
    private static boolean isKeyEvictedOrOverwritten(TableHolder table, int idx, long expectedKey) {
        return table.getKeyAcquire(idx) != expectedKey;
    }

    /**
     * 针对 Key 匹配槽位，自旋等待数据发布并进行 64-bit 签名前缀防碰撞校验 (Anti-Collision Defense & Spin Wait)
     *
     * 核心机制：
     * 1. 碰撞防线 (Anti-Collision Defense)：校验 8 字节签名前缀，防止 64-bit 哈希碰撞导致的误授权；
     * 2. 短路退出 (Fast Reject)：由于写线程先写 Prefix 后写 ValExp，当前缀发生碰撞时瞬间退出，省去无谓自旋；
     * 3. 活性守护 (Liveness Guard)：自旋期间持续防并发淘汰/覆写脏读。
     */
    private long spinWaitForEntryWithCollisionCheck(TableHolder table, int idx, long key, long targetPrefix) {
        // ⚡ 1. Fast Path (99.99% 命中)：无自旋极速读取
        long valExp = table.getValExpAcquire(idx);
        long storedPrefix = table.getPrefixAcquire(idx);

        if (valExp != 0L && storedPrefix != 0L) {
            if (targetPrefix != 0L && storedPrefix != targetPrefix) {
                return 0L; // 前缀碰撞冲突，拒绝匹配
            }
            return valExp;
        }

        // 🐢 2. Slow Path：数据写在中途，自旋等待 Prefix 落地
        if (storedPrefix == 0L) {
            for (int spin = 0; spin < MAX_SPIN; spin++) {
                Thread.onSpinWait();
                if (isKeyEvictedOrOverwritten(table, idx, key)) {
                    return 0L;
                }
                storedPrefix = table.getPrefixAcquire(idx);
                if (storedPrefix != 0L) break;
            }
        }

        // 前缀未落地或碰撞冲突 Fast Reject 退出
        if (storedPrefix == 0L || isPrefixMismatched(targetPrefix, storedPrefix)) {
            return 0L;
        }

        // 自旋等待 ValExp 最终落盘
        if (valExp == 0L) {
            for (int spin = 0; spin < MAX_SPIN; spin++) {
                Thread.onSpinWait();
                if (isKeyEvictedOrOverwritten(table, idx, key) || isPrefixMismatched(targetPrefix, table.getPrefixAcquire(idx))) {
                    return 0L;
                }
                valExp = table.getValExpAcquire(idx);
                if (valExp != 0L) break;
            }
        }

        return valExp;
    }

    /**
     * 获取当前 Unix 时间戳（单位：秒）
     */
    public static long nowSecond() {
        return System.currentTimeMillis() / 1000;
    }

    /**
     * 校验 64-bit 签名 8 字节前缀是否冲突不匹配 (Hash Anti-Collision Prefix Mismatch Check)
     */
    private static boolean isPrefixMismatched(long targetPrefix, long storedPrefix) {
        return targetPrefix != 0L && storedPrefix != 0L && storedPrefix != targetPrefix;
    }

    /**
     * 判断槽位数据是否已到期失效 (Expiration Check)
     */
    private static boolean isExpired(long expSec, long nowSec) {
        return expSec > 0 && nowSec >= expSec;
    }

    /**
     * 根据 64-bit 签名 xxHash64 与 签名 8 字节前缀获取缓存的 UID
     */
    public long get(long sigHash, long sigPrefix) {
        long key = normalizeKey(sigHash);
        long nowSec = nowSecond();

        TablePair pair = getTablesAcquire();
        TableHolder h = pair.hot;
        int baseIdx = (int) (key & MASK);

        // 1. 查找 Hot Table
        for (int i = 0; i < MAX_PROBE; i++) {
            int idx = (baseIdx + i) & MASK;
            long k = h.getKeyAcquire(idx);
            if (k == EMPTY) {
                break; // 探查域遇到 EMPTY 停探
            }
            if (k == key) {
                // Key 匹配！自旋等待 Prefix 与 ValExp 完全发布落地并校验碰撞
                long valExp = spinWaitForEntryWithCollisionCheck(h, idx, key, sigPrefix);
                if (valExp != 0L) {
                    long expSec = unpackExpSec(valExp);
                    if (isExpired(expSec, nowSec)) {
                        invalidate(sigHash);
                        return 0L;
                    }
                    return unpackUid(valExp);
                }
            }
        }

        // 2. 查找 Cold Table (带冷到热晋升)
        TableHolder c = pair.cold;
        for (int i = 0; i < MAX_PROBE; i++) {
            int idx = (baseIdx + i) & MASK;
            long k = c.getKeyAcquire(idx);
            if (k == EMPTY) {
                break;
            }
            if (k == key) {
                long valExp = spinWaitForEntryWithCollisionCheck(c, idx, key, sigPrefix);
                if (valExp != 0L) {
                    long expSec = unpackExpSec(valExp);
                    if (isExpired(expSec, nowSec)) {
                        invalidate(sigHash);
                        return 0L;
                    }
                    long uid = unpackUid(valExp);
                    if (c.casKey(idx, key, TOMBSTONE)) {
                        c.setPrefix(idx, 0L);
                        c.setValExpRelease(idx, 0L);
                    }
                    put(sigHash, sigPrefix, uid, expSec);
                    return uid;
                }
            }
        }

        return 0L;
    }

    /**
     * 写入 签名 Hash + 前缀 -> (UID, expSec) 映射到 热表 (Hot Table)
     */
    public void put(long sigHash, long sigPrefix, long uid, long expSec) {
        if (uid <= 0) return;
        long key = normalizeKey(sigHash);
        long packed = packValExp(uid, expSec);

        TableHolder h = getTablesAcquire().hot;

        if (h.count.get() >= LOAD_FACTOR_THRESHOLD) {
            checkAndRotateTables();
            h = getTablesAcquire().hot;
        }

        int baseIdx = (int) (key & MASK);

        for (int i = 0; i < MAX_PROBE; i++) {
            int idx = (baseIdx + i) & MASK;
            long k = h.getKeyAcquire(idx);

            if (k == key) {
                // 覆盖写入发布协议 (Overwrite Publishing Protocol):
                // 先将 valExp 设为 0L (哨兵)，通知并发读线程数据正在更新，强制读线程自旋等待，
                // 彻底擦除 storedPrefix 与 valExp 跨字段更新期间读取到旧 ValExp 的脏读隐患！
                h.setValExpRelease(idx, 0L);
                h.setPrefix(idx, sigPrefix);
                h.setValExpRelease(idx, packed);
                return;
            }

            if (!isLive(k)) {
                if (h.casKey(idx, k, key)) {
                    h.setPrefix(idx, sigPrefix);
                    h.setValExpRelease(idx, packed);
                    h.count.incrementAndGet();
                    return;
                }
            }
        }
    }

    public void put(long sigHash, long uid, long expSec) {
        put(sigHash, 0L, uid, expSec);
    }

    public void put(long sigHash, long uid) {
        put(sigHash, 0L, uid, nowSecond() + 86400);
    }

    /**
     * 主动失效
     */
    public void invalidate(long sigHash) {
        long key = normalizeKey(sigHash);
        int baseIdx = (int) (key & MASK);

        TablePair pair = getTablesAcquire();
        TableHolder h = pair.hot;
        for (int i = 0; i < MAX_PROBE; i++) {
            int idx = (baseIdx + i) & MASK;
            long k = h.getKeyAcquire(idx);
            if (k == EMPTY) break;
            if (k == key) {
                if (h.casKey(idx, key, TOMBSTONE)) {
                    h.setPrefix(idx, 0L);
                    h.setValExpRelease(idx, 0L);
                }
                break;
            }
        }

        TableHolder c = pair.cold;
        for (int i = 0; i < MAX_PROBE; i++) {
            int idx = (baseIdx + i) & MASK;
            long k = c.getKeyAcquire(idx);
            if (k == EMPTY) break;
            if (k == key) {
                if (c.casKey(idx, key, TOMBSTONE)) {
                    c.setPrefix(idx, 0L);
                    c.setValExpRelease(idx, 0L);
                }
                break;
            }
        }
    }

    public void invalidateByToken(String token) {
        if (token == null || token.isEmpty()) return;
        int dot1 = token.indexOf('.');
        if (dot1 < 0) return;
        int dot2 = token.indexOf('.', dot1 + 1);
        if (dot2 < 0 || dot2 + 1 >= token.length()) return;

        String sig = token.substring(dot2 + 1).trim();
        if (sig.isEmpty()) return;

        byte[] sigBytes = sig.getBytes(StandardCharsets.UTF_8);
        long hash = com.netty.limiter.util.XxHash64Util.xxHash64(sigBytes, 0, sigBytes.length);
        invalidate(hash);
    }

    public void invalidateByByteBuf(ByteBuf buf, int sigStart, int sigEnd) {
        if (buf == null || sigEnd <= sigStart) return;
        long sigHash = com.netty.limiter.util.XxHash64Util.fastHash64(buf, sigStart, sigEnd);
        invalidate(sigHash);
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
}
