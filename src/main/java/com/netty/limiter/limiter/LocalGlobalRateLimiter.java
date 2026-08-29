package com.netty.limiter.limiter;

import com.netty.limiter.config.GatewayRateLimitProperties;
import io.netty.util.concurrent.FastThreadLocal;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import javax.annotation.PostConstruct;
import java.util.concurrent.atomic.AtomicLong;

/**
 * @description: 节点级无锁 64-bit Bit Packing 令牌桶 + FastThreadLocal AIMD 缓冲区
 **/
@Slf4j
@Component
public class LocalGlobalRateLimiter {

    @Autowired
    private GatewayRateLimitProperties properties;

    private GlobalTokenBucket globalTokenBucket;

    private final FastThreadLocal<ThreadTokenBuffer> threadBufferContainer = new FastThreadLocal<>() {
        @Override
        protected ThreadTokenBuffer initialValue() {
            return new ThreadTokenBuffer(globalTokenBucket);
        }
    };

    @PostConstruct
    public void init() {
        int capacity = properties.getGlobalQps() != null ? properties.getGlobalQps() : 100000;
        int rate = properties.getFillRate() != null ? properties.getFillRate() : 100000;
        this.globalTokenBucket = new GlobalTokenBucket(capacity, rate);
        log.info("Initialized LocalGlobalRateLimiter with Capacity={}, FillRate={}", capacity, rate);
    }

    public boolean tryAcquire() {
        return threadBufferContainer.get().tryAcquire();
    }

    public void updateConfig(int newCapacity, int newTokensPerSec) {
        if (newCapacity <= 0 || newTokensPerSec <= 0) {
            log.warn("Rejected invalid rate limiter config update: capacity={}, fillRate={}. Values must be positive (> 0).", newCapacity, newTokensPerSec);
            return;
        }
        if (this.globalTokenBucket != null) {
            this.globalTokenBucket.updateConfig(newCapacity, newTokensPerSec);
        }
    }

    public int fetchBatchTokens(int requestedBatchSize) {
        if (this.globalTokenBucket == null) {
            return requestedBatchSize;
        }
        return this.globalTokenBucket.grantBatchTokens(requestedBatchSize);
    }

    /**
     * 节点级全局物理无锁 64-bit Bit Packing 令牌桶 (Global Token Source)
     */
    public static class GlobalTokenBucket {
        /** 低水位线阈值比例 (当全局存量低于 1% 时触发批次划转上限收缩) */
        private static final double LOW_WATERMARK_RATIO = 0.01;

        /** 默认 EventLoop 线程数量 (以物理/逻辑 CPU 核心数作为基准) */
        private static final int DEFAULT_EVENT_LOOP_THREADS = Math.max(1, Runtime.getRuntime().availableProcessors());

        /** 低水位线最大划转量除数因子 = EventLoop 线程数量 * 2 */
        private static final int LOW_WATERMARK_DIVISOR = DEFAULT_EVENT_LOOP_THREADS * 2;

        private volatile int capacity;
        private volatile int tokensPerSec;
        private final AtomicLong statePacked = new AtomicLong(0L);

        public GlobalTokenBucket(int capacity, int tokensPerSec) {
            this.capacity = capacity;
            this.tokensPerSec = tokensPerSec;
            long nowSec = System.currentTimeMillis() / 1000L;
            long initialPacked = pack((long) capacity, nowSec);
            this.statePacked.set(initialPacked);
        }

        public void updateConfig(int newCapacity, int newTokensPerSec) {
            this.capacity = newCapacity;
            this.tokensPerSec = newTokensPerSec;
        }

        public int grantBatchTokens(int requestSize) {
            while (true) {
                long currentPacked = statePacked.get();
                long lastTimestampSec = unpackTimestamp(currentPacked);
                long nowSec = System.currentTimeMillis() / 1000L;

                // 1. 跨秒惰性填充，计算物理桶最新可用令牌存量 (Available Tokens)
                long availableTokens = lazyRefillTokens(unpackTokens(currentPacked), lastTimestampSec, nowSec);
                if (availableTokens <= 0) {
                    return 0;
                }

                // 2. 实际上最多可以获得的数量 (受限于当前物理桶存量)
                int maxFeasibleTokens = (int) Math.min((long) requestSize, availableTokens);

                // 3. 当处于低水位告警状态时，按细粒度平摊配额保护性裁切，确保残余令牌在多 EventLoop 线程间更均匀分布
                int actualGranted = maxFeasibleTokens;
                if (isLowWatermark(availableTokens)) {
                    int fineGrainedQuota = calculateEvenlyDistributedQuotaOnLowWatermark(availableTokens);
                    actualGranted = Math.min(maxFeasibleTokens, fineGrainedQuota);
                }

                long updatedSec = (nowSec > lastTimestampSec) ? nowSec : lastTimestampSec;

                // 4. 无锁原子 CAS 一次性打包更新剩余令牌与最新时间戳
                if (statePacked.compareAndSet(currentPacked, pack(availableTokens - actualGranted, updatedSec))) {
                    return actualGranted;
                }
            }
        }

        /**
         * 检查当前全局物理桶存量是否落入 10% 低水位线告警区
         */
        private boolean isLowWatermark(long availableTokens) {
            return availableTokens < (long) (capacity * LOW_WATERMARK_RATIO);
        }

        /**
         * 低水位线细粒度平摊配额计算 (限制单次最高划转上限，使残余令牌在多 EventLoop 线程间更均匀分布，防止饥饿)
         */
        private int calculateEvenlyDistributedQuotaOnLowWatermark(long availableTokens) {
            return (int) Math.max(5, availableTokens / LOW_WATERMARK_DIVISOR);
        }

        /**
         * 跨秒惰性填充刷新物理桶令牌 (Lazy Refill)
         */
        private long lazyRefillTokens(long currentTokens, long lastTimestampSec, long nowSec) {
            if (nowSec > lastTimestampSec) {
                return Math.min((long) capacity, currentTokens + (nowSec - lastTimestampSec) * (long) tokensPerSec);
            }
            return currentTokens;
        }

        private static long pack(long tokens, long timestampSec) {
            return ((tokens & 0xFFFFFFFFL) << 32) | (timestampSec & 0xFFFFFFFFL);
        }

        private static long unpackTokens(long packed) {
            return (packed >>> 32) & 0xFFFFFFFFL;
        }

        private static long unpackTimestamp(long packed) {
            return packed & 0xFFFFFFFFL;
        }
    }

    /**
     * Netty EventLoop 线程私有的速率自适应 (Rate-Adaptive) 动态令牌缓冲区
     *
     * 业务意图：
     * 1. 高并发场景：若线程在毫秒级内迅速消耗完上批令牌（高 Rate），自动扩展批次步长，极力消除对全局桶的 CAS 争用；
     * 2. 低并发场景：若线程消耗上批令牌耗时极长（低 Rate），自动收缩批次步长至最小值 (1)，绝不盲目囤积令牌，保证全局公平性。
     */
    public static class ThreadTokenBuffer {

        // 【伪共享消除 (Cache Line Padding)】：前置 56 Bytes 缓存行填充，隔离 CPU L1/L2 Cache 伪共享
        private long p1, p2, p3, p4, p5, p6, p7;

        /** 动态划转：单线程每次向节点桶批发的【最小划转步长】(避免跌落到 1 导致极高频 CAS 争用) */
        private static final int MIN_BATCH_FETCH_STEP = 4;

        /** 动态划转：单线程首次初始化时的【基准划转步长】 */
        private static final int INITIAL_BATCH_FETCH_STEP = 16;

        /** 动态划转：单线程高频顺畅请求下允许扩展的【最大划转步长】 */
        private static final int MAX_BATCH_FETCH_STEP = 512;

        /** 期望 Worker 线程向全局桶 CAS 划转的目标时间间隔 (50 毫秒/次，即控制全局 CAS 频率约 20 次/秒) */
        private static final long TARGET_CAS_INTERVAL_MILLIS = 50L;

        /** 直接持有全局物理令牌桶引用 (Master 令牌源) */
        private final GlobalTokenBucket globalTokenBucket;

        /** 当前 EventLoop 线程私有缓冲区中【剩余可直接消耗的存量令牌数】 */
        private int threadLocalRemainingTokens = 0;

        /** 当前线程向节点全局桶批量划转令牌的【动态自适应步长】 */
        private int currentAdaptiveFetchStep = INITIAL_BATCH_FETCH_STEP;

        /** 节点桶枯竭/争用时触发的【短路冷静避退截止毫秒时间戳】 */
        private long cooldownUntilMillis = 0L;

        /** 记录上一次向全局桶划转令牌的【时间戳】与【成功拿到的令牌总数】 */
        private long lastFetchTimestampMillis = System.currentTimeMillis();
        private int lastBatchGrantedTokens = 0;

        // 【伪共享消除 (Cache Line Padding)】：后置 56 Bytes 缓存行填充
        private long p8, p9, p10, p11, p12, p13, p14;

        public ThreadTokenBuffer(GlobalTokenBucket globalTokenBucket) {
            this.globalTokenBucket = globalTokenBucket;
            this.lastFetchTimestampMillis = System.currentTimeMillis();
        }

        /**
         * 尝试获取 1 个令牌 (优先消耗线程私有缓冲区，本地耗尽时触发速率自适应动态划转)
         */
        public boolean tryAcquire() {
            // 【极限优化】：优先消耗本地私有缓冲区！
            // 99.8% 的极高速热点请求在此瞬间返回 (0 锁, 0 CAS, 0 竞争, 0 时钟读取开销)
            if (threadLocalRemainingTokens > 0) {
                threadLocalRemainingTokens--;
                return true;
            }

            // 只有本地缓冲区耗尽时，才延迟读取系统时间戳 (消除高频 System.currentTimeMillis 系统调用开销)
            long currentTimeMillis = System.currentTimeMillis();

            // 1. 如果处于争用避退冷静期，直接判定为限流拒绝
            if (currentTimeMillis < cooldownUntilMillis) {
                return false;
            }

            // 2. 本地令牌耗尽：自适应计算本次最佳划转步长
            adjustFetchStepByConsumptionRate(currentTimeMillis);

            // 3. 按计算出的最佳步长，直接向全局物理令牌桶【批量划转】
            int actualGrantedTokens = globalTokenBucket.grantBatchTokens(currentAdaptiveFetchStep);

            if (actualGrantedTokens > 0) {
                // 3.1 成功划转到令牌：拿 1 个给当前请求，其余放入本地私有缓冲区，并记录采样点
                threadLocalRemainingTokens = actualGrantedTokens - 1;
                recordSuccessfulFetchSample(currentTimeMillis, actualGrantedTokens);
                return true;
            } else {
                // 3.2 全局桶容量不足/争用剧烈：处理争用避退与步长衰减
                handleContentionBackoff(currentTimeMillis);
                return false;
            }
        }

        /**
         * 记录一次成功划转的净采样点 (Net Sampling Point)
         */
        private void recordSuccessfulFetchSample(long currentTimeMillis, int actualGrantedTokens) {
            this.lastFetchTimestampMillis = currentTimeMillis;
            this.lastBatchGrantedTokens = actualGrantedTokens;
        }

        /**
         * 处理争用避退：平滑折半收缩步长、清除已被避退污染的采样点并触发短路冷静避退
         */
        private void handleContentionBackoff(long currentTimeMillis) {
            decayFetchStepOnContention();
            clearContentionPollutedSamplingPoint();
            triggerContentionCooldown(currentTimeMillis);
        }

        /**
         * 争用/枯竭时平滑折半收缩划转步长 (如 256->128->64)
         */
        private void decayFetchStepOnContention() {
            currentAdaptiveFetchStep = Math.max(MIN_BATCH_FETCH_STEP, currentAdaptiveFetchStep >> 1);
        }

        /**
         * 争用避退时清除已被避退耗时污染的采样点，彻底防止后续错误地根据被污染的旧采样点计算消耗速率
         */
        private void clearContentionPollutedSamplingPoint() {
            this.lastFetchTimestampMillis = 0L;
            this.lastBatchGrantedTokens = 0;
        }

        /**
         * 依据收缩后的步长，触发自适应冷静避退
         */
        private void triggerContentionCooldown(long currentTimeMillis) {
            cooldownUntilMillis = currentTimeMillis + calculateBackoffCooldownMillis(currentAdaptiveFetchStep);
        }

        /**
         * 基于【线程真实令牌消耗速率 (Rate of Consumption)】动态平滑调整下一个批次的划转步长
         */
        private void adjustFetchStepByConsumptionRate(long currentTimeMillis) {
            if (hasValidSamplingPoint()) {
                computeRateBasedFetchStep(currentTimeMillis);
            }
        }

        /**
         * 检查当前线程是否存在有效且未被避退污染的净采样点 (Net Sampling Point)
         */
        private boolean hasValidSamplingPoint() {
            return lastFetchTimestampMillis > 0 && lastBatchGrantedTokens > 0;
        }

        /**
         * 根据当前净采样点，计算基于消耗速率的自适应划转步长 (EMA 平滑)
         */
        private void computeRateBasedFetchStep(long currentTimeMillis) {
            // 保证耗时至少为 1ms，防止同 1 毫秒内极其高频暴击造成零除
            long elapsedMillis = Math.max(1L, currentTimeMillis - lastFetchTimestampMillis);
            int targetFetchStep = (int) Math.min(
                    MAX_BATCH_FETCH_STEP,
                    Math.max(MIN_BATCH_FETCH_STEP, (lastBatchGrantedTokens * TARGET_CAS_INTERVAL_MILLIS) / elapsedMillis)
            );
            // 采用 EMA 指数平滑更新步长：(当前步长 + 目标步长) / 2
            currentAdaptiveFetchStep = (currentAdaptiveFetchStep + targetFetchStep) >> 1;
        }

        /**
         * 根据当前收缩后的划转步长，计算争用避退冷静期窗口 (毫秒)
         */
        private long calculateBackoffCooldownMillis(int fetchStep) {
            if (fetchStep <= 2) return 5L;
            if (fetchStep <= 8) return 3L;
            if (fetchStep <= 32) return 2L;
            return 1L;
        }
    }
}
