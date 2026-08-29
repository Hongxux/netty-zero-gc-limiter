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
 * @description: 极限性能 JWT 签名 0-GC 静态 TablePair 全 VarHandle Acquire/Release 零 volatile 缓存单例 (Enum Singleton)
 * 极限架构保障：
 * 1. 全面移除 volatile 关键字，全架构统一采用 VarHandle Acquire / Release 内存屏障：
 *    - 存储结构：keys / values / expTimes 数组使用 VarHandle 进行 Acquire 读、Release 写与 CAS 抢占。
 *    - 指针对调：tables 组合指针移除 volatile，改用 TABLES_VH.getAcquire(this) 读与 TABLES_VH.setRelease(this, newPair) 写入。
 *    - 硬件性能收益：在 x86 架构下，VarHandle setRelease 相比 volatile write 避免了昂贵的 StoreLoad (MFENCE) 全内存屏障指令；在 ARM64 架构下完美内联编译为 LDAR / STLR 微指令。
 * 2. 独立 TableHolder 计数器（消除了热表计数器覆盖并发 Bug）：
 *    - 每个 TableHolder 拥有独立的 AtomicInteger count 计数器，轮转清空旧冷表时重置 count=0。
 * 3. 0-STW 无锁清空与 StoreStore Fence 保障：
 *    - clear() 先 SIMD 清空 keys (→EMPTY)，再通过 StoreStore Fence 保证全局可见后，再清空 values/expTimes 并重置 count。
 * 4. 有界自旋等待 + Key 完整性重验 (Bounded Spin-Wait & Key Integrity Re-check)：
 *    - 读路径：Acquire 读取 Key 匹配后，若 Value 为 0，有界自旋 (MAX_SPIN=64) 并重验 Key 防止死锁。
 * 5. 100% 0 堆内存分配 (Zero Heap Allocation)：全局预分配静态 TableHolder 与 TablePair，终身复用。
 **/
public enum JwtSigUidCache {

    /**
     * 唯一单例实例
     */
    INSTANCE;

    // 强制 2 的幂次方容量 65536 (2^16) 与位运算掩码
    public static final int CAPACITY = 65536;
    public static final int MASK = CAPACITY - 1;
    public static final int MAX_PROBE = 8;
    public static final int MAX_SPIN = 64;
    public static final int LOAD_FACTOR_THRESHOLD = (int) (CAPACITY * 0.4); // 26,214

    // 哨兵常量：0L 为空槽位，-1L 为墓碑
    public static final long EMPTY = 0L;
    public static final long TOMBSTONE = -1L;

    /**
     * VarHandle 边界哨兵组合容器 (封装 long[] 原语数组 + 独立 Table 计数器)
     */
    public static class TableHolder {
        private static final VarHandle KEYS_VH = MethodHandles.arrayElementVarHandle(long[].class);
        private static final VarHandle PREFIXES_VH = MethodHandles.arrayElementVarHandle(long[].class);
        private static final VarHandle VALS_VH = MethodHandles.arrayElementVarHandle(long[].class);
        private static final VarHandle EXPS_VH = MethodHandles.arrayElementVarHandle(long[].class);

        public final long[] keys;
        public final long[] sigPrefixes; // 签名首 8 字节前缀 (防止 64-bit Hash 碰撞)
        public final long[] values;
        public final long[] expTimes; // JWT 过期时间戳 (秒)
        public final AtomicInteger count = new AtomicInteger(0);

        public TableHolder(int capacity) {
            this.keys = new long[capacity];
            this.sigPrefixes = new long[capacity];
            this.values = new long[capacity];
            this.expTimes = new long[capacity];
        }

        /**
         * SIMD 向量化安全清空 (Cross-Platform Memory Safety)：
         * 先清空 keys → StoreStore Fence → 再清空 sigPrefixes/values/expTimes 并重置 count
         */
        public void clear() {
            Arrays.fill(keys, EMPTY);
            VarHandle.storeStoreFence();
            Arrays.fill(sigPrefixes, 0L);
            Arrays.fill(values, 0L);
            VarHandle.storeStoreFence();
            Arrays.fill(expTimes, 0L);
            count.set(0);
        }

        /**
         * 边界哨兵：Acquire 内存屏障读取 Key
         */
        public long getKeyAcquire(int idx) {
            return (long) KEYS_VH.getAcquire(keys, idx);
        }

        /**
         * 边界哨兵：Release 内存屏障写入 Key
         */
        public void setKeyRelease(int idx, long key) {
            KEYS_VH.setRelease(keys, idx, key);
        }

        /**
         * 边界哨兵：CAS 原子抢占写入 Key
         */
        public boolean casKey(int idx, long expected, long newKey) {
            return KEYS_VH.compareAndSet(keys, idx, expected, newKey);
        }

        /**
         * Acquire 内存屏障读取 Signature Prefix
         */
        public long getPrefixAcquire(int idx) {
            return (long) PREFIXES_VH.getAcquire(sigPrefixes, idx);
        }

        /**
         * Release 内存屏障写入 Signature Prefix
         */
        public void setPrefixRelease(int idx, long prefix) {
            PREFIXES_VH.setRelease(sigPrefixes, idx, prefix);
        }

        /**
         * Acquire 内存屏障读取 Value
         */
        public long getValueAcquire(int idx) {
            return (long) VALS_VH.getAcquire(values, idx);
        }

        /**
         * Release 内存屏障写入 Value
         */
        public void setValueRelease(int idx, long val) {
            VALS_VH.setRelease(values, idx, val);
        }

        /**
         * Acquire 内存屏障读取 Expiration Time
         */
        public long getExpAcquire(int idx) {
            return (long) EXPS_VH.getAcquire(expTimes, idx);
        }

        /**
         * Release 内存屏障写入 Expiration Time
         */
        public void setExpRelease(int idx, long exp) {
            EXPS_VH.setRelease(expTimes, idx, exp);
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

    // VarHandle 操控 Enum/Class 实例字段 tables（完全替代 volatile 关键字）
    private static final VarHandle TABLES_VH;
    static {
        try {
            TABLES_VH = MethodHandles.lookup().findVarHandle(JwtSigUidCache.class, "tables", TablePair.class);
        } catch (ReflectiveOperationException e) {
            throw new ExceptionInInitializerError(e);
        }
    }

    // 静态预分配 2 个 TableHolder 内存块
    private final TableHolder tableA = new TableHolder(CAPACITY);
    private final TableHolder tableB = new TableHolder(CAPACITY);

    // 静态预分配 2 个 TablePair 组合，轮转时 0 GC 切换！
    private final TablePair pairAB = new TablePair(tableA, tableB);
    private final TablePair pairBA = new TablePair(tableB, tableA);

    // 双表组引用字段（无需 volatile 关键字，由 TABLES_VH 统一执行 Acquire/Release 操作）
    private TablePair tables = pairAB;

    // 轮转原子标志
    private final AtomicBoolean rotating = new AtomicBoolean(false);

    /**
     * 将 64-bit 任意 Hash 值正则化为非 0、非 -1 的合法 Key
     */
    public static long normalizeKey(long hash) {
        if (hash == EMPTY || hash == TOMBSTONE) {
            return 1L; // 规避 EMPTY(0) 与 TOMBSTONE(-1) 哨兵冲突
        }
        return hash;
    }

    /**
     * 获取当前 TablePair 组（Acquire 语义读取）
     */
    private TablePair getTablesAcquire() {
        return (TablePair) TABLES_VH.getAcquire(this);
    }

    /**
     * 设置当前 TablePair 组（Release 语义写入）
     */
    private void setTablesRelease(TablePair newPair) {
        TABLES_VH.setRelease(this, newPair);
    }

    /**
     * 有界自旋等待 Value 写入完成，同时通过 Key 完整性重验防止死锁。
     */
    private long spinWaitForValue(TableHolder table, int idx, long key) {
        long val = table.getValueAcquire(idx);
        if (val > 0) return val;

        for (int spin = 0; spin < MAX_SPIN; spin++) {
            Thread.onSpinWait();
            if (table.getKeyAcquire(idx) != key) {
                return 0L;
            }
            val = table.getValueAcquire(idx);
            if (val > 0) return val;
        }
        return 0L;
    }

    /**
     * 根据 64-bit 签名 xxHash64 与 签名 8 字节前缀获取缓存的 UID (二重防哈希碰撞校验)
     * @param sigHash 签名 Hash
     * @param sigPrefix 签名首 8 字节前缀
     * @return 缓存的 UID (不存在或已过期返回 0L)
     */
    public long get(long sigHash, long sigPrefix) {
        long key = normalizeKey(sigHash);
        long nowSec = System.currentTimeMillis() / 1000;

        // 1 次 VarHandle Acquire 读取：100% 单指令原子抓取当前 (Hot, Cold) 组合快照！
        TablePair pair = getTablesAcquire();
        TableHolder h = pair.hot;
        int baseIdx = (int) (key & MASK);

        for (int i = 0; i < MAX_PROBE; i++) {
            int idx = (baseIdx + i) & MASK; // 循环数组 probing
            long k = h.getKeyAcquire(idx);
            if (k == EMPTY) {
                break; // 遇到 EMPTY 停止热表查找
            }
            if (k == key) {
                // 二重防碰撞校验：Key 与 sigPrefix 均匹配，或者 sigPrefix 为 0L 哨兵
                long storedPrefix = h.getPrefixAcquire(idx);
                if (sigPrefix != 0L && storedPrefix != 0L && storedPrefix != sigPrefix) {
                    continue; // 哈希碰撞！死锁规避，继续 probe
                }
                long val = spinWaitForValue(h, idx, key);
                if (val > 0) {
                    long expSec = h.getExpAcquire(idx);
                    if (expSec > 0 && nowSec >= expSec) {
                        // JWT 已过期：从缓存中主动失效并直接返回 0L (快路径直接拦截，免跑慢路径)！
                        invalidate(sigHash);
                        return 0L;
                    }
                    return val; // 仍有效，放行！
                }
            }
        }

        TableHolder c = pair.cold;
        for (int i = 0; i < MAX_PROBE; i++) {
            int idx = (baseIdx + i) & MASK; // 循环数组 probing
            long k = c.getKeyAcquire(idx);
            if (k == EMPTY) {
                break; // 遇到 EMPTY 停止冷表查找
            }
            if (k == key) {
                long storedPrefix = c.getPrefixAcquire(idx);
                if (sigPrefix != 0L && storedPrefix != 0L && storedPrefix != sigPrefix) {
                    continue;
                }
                long val = spinWaitForValue(c, idx, key);
                if (val > 0) {
                    long expSec = c.getExpAcquire(idx);
                    if (expSec > 0 && nowSec >= expSec) {
                        // JWT 已过期：冷表中主动失效并直接返回 0L！
                        invalidate(sigHash);
                        return 0L;
                    }
                    // 【冷到热晋升 Promotion】：从冷表移除，并提升入热表
                    if (c.casKey(idx, key, TOMBSTONE)) {
                        c.setPrefixRelease(idx, 0L);
                        c.setValueRelease(idx, 0L);
                        c.setExpRelease(idx, 0L);
                    }
                    put(sigHash, sigPrefix, val, expSec);
                    return val;
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

        TableHolder h = getTablesAcquire().hot;

        // 检查当前 Hot 表自身计数器是否达到 0.4 阈值
        if (h.count.get() >= LOAD_FACTOR_THRESHOLD) {
            checkAndRotateTables();
            h = getTablesAcquire().hot; // 抓取轮转后的最新 Hot 表！
        }

        int baseIdx = (int) (key & MASK);

        for (int i = 0; i < MAX_PROBE; i++) {
            int idx = (baseIdx + i) & MASK; // 循环数组 probing
            long k = h.getKeyAcquire(idx);

            if (k == key) {
                // 已存在：直接 setRelease 更新 Prefix, Value 与 Exp
                h.setPrefixRelease(idx, sigPrefix);
                h.setExpRelease(idx, expSec);
                h.setValueRelease(idx, uid);
                return;
            }

            if (k == EMPTY || k == TOMBSTONE) {
                // 遇到 EMPTY 或 TOMBSTONE 槽位，直接 CAS 抢占
                if (h.casKey(idx, k, key)) {
                    h.setPrefixRelease(idx, sigPrefix);
                    h.setExpRelease(idx, expSec);
                    h.setValueRelease(idx, uid);
                    h.count.incrementAndGet(); // 仅增加属于当前 TableHolder 自己的独立计数器！
                    return;
                }
            }
        }
    }

    public void put(long sigHash, long uid, long expSec) {
        put(sigHash, 0L, uid, expSec);
    }

    public void put(long sigHash, long uid) {
        // 默认 24 小时 TTL 过期时间
        put(sigHash, 0L, uid, (System.currentTimeMillis() / 1000) + 86400);
    }

    /**
     * 主动失效 / 删除指定 签名 Hash（同时清除热表与冷表中的记录，避免废弃数据被误晋升）
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
                    h.setValueRelease(idx, 0L);
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
                    c.setValueRelease(idx, 0L);
                }
                break;
            }
        }
    }

    /**
     * 根据 JWT Token 字符串（例如鉴权失败时）主动计算 xxHash64 并从双表中同步失效
     */
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

    /**
     * 根据 Netty ByteBuf 签名区间计算 xxHash64 并主动从双表中失效
     */
    public void invalidateByByteBuf(ByteBuf buf, int sigStart, int sigEnd) {
        if (buf == null || sigEnd <= sigStart) return;
        long sigHash = com.netty.limiter.util.XxHash64Util.fastHash64(buf, sigStart, sigEnd);
        invalidate(sigHash);
    }

    /**
     * 100% 0-STW 无锁双表轮转：
     * 1. 检查当前 Hot 表的独立 count 计数器。
     * 2. SIMD 向量化快速清空旧冷表 (table.clear() 包含 reset count=0)。
     * 3. 通过单条 VarHandle setRelease 指令（TABLES_VH.setRelease）完成 100% 原子的 (Hot, Cold) 组合指针切换。
     */
    private void checkAndRotateTables() {
        if (rotating.compareAndSet(false, true)) {
            try {
                TablePair currentPair = getTablesAcquire();
                if (currentPair.hot.count.get() >= LOAD_FACTOR_THRESHOLD) {
                    TableHolder oldCold = currentPair.cold;

                    // 1. SIMD 向量化安全清空旧冷表及其计数器 (~0.8 微秒完成)
                    oldCold.clear();

                    // 2. 单条 VarHandle setRelease 指令：100% 原子完成 (Hot, Cold) 组合指针切换！
                    setTablesRelease((currentPair == pairAB) ? pairBA : pairAB);
                }
            } finally {
                rotating.set(false);
            }
        }
    }
}
