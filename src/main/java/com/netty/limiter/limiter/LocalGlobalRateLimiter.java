package com.netty.limiter.limiter;

import com.netty.limiter.config.GatewayRateLimitProperties;
import io.netty.util.concurrent.FastThreadLocal;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import javax.annotation.PostConstruct;
import java.lang.invoke.MethodHandles;
import java.lang.invoke.VarHandle;
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
        /**
         * 不可变配置快照，物理消除配置热更新时的撕裂读 (Torn Read) 隐患
         */
        public record ConfigSnapshot(int capacity, int tokensPerSec) {}

        private static final VarHandle CONFIG_HANDLE;

        static {
            try {
                MethodHandles.Lookup l = MethodHandles.lookup();
                CONFIG_HANDLE = l.findVarHandle(GlobalTokenBucket.class, "configSnapshot", ConfigSnapshot.class);
            } catch (Exception e) {
                throw new ExceptionInInitializerError(e);
            }
        }

        /** 低水位线阈值比例 (当全局存量低于 1% 时触发批次划转上限收缩) */
        private static final double LOW_WATERMARK_RATIO = 0.01;

        /** 默认 EventLoop 线程数量 (以物理/逻辑 CPU 核心数作为基准) */
        private static final int DEFAULT_EVENT_LOOP_THREADS = Math.max(1, Runtime.getRuntime().availableProcessors());

        /** 低水位线最大划转量除数因子 = EventLoop 线程数量 * 2 */
        private static final int LOW_WATERMARK_DIVISOR = DEFAULT_EVENT_LOOP_THREADS * 2;

        private volatile ConfigSnapshot configSnapshot;
        private final AtomicLong statePacked = new AtomicLong(0L);

        public GlobalTokenBucket(int capacity, int tokensPerSec) {
            ConfigSnapshot initialConfig = new ConfigSnapshot(capacity, tokensPerSec);
            CONFIG_HANDLE.setRelease(this, initialConfig);
            long nowSec = System.currentTimeMillis() / 1000L;
            long initialPacked = pack((long) capacity, nowSec);
            this.statePacked.set(initialPacked);
        }

        public void updateConfig(int newCapacity, int newTokensPerSec) {
            CONFIG_HANDLE.setRelease(this, new ConfigSnapshot(newCapacity, newTokensPerSec));
        }

        public ConfigSnapshot getConfigSnapshot() {
            return (ConfigSnapshot) CONFIG_HANDLE.getAcquire(this);
        }

        public int grantBatchTokens(int requestSize) {
            ConfigSnapshot cfg = getConfigSnapshot();
            while (true) {
                long currentPacked = statePacked.get();
                long lastTimestampSec = unpackTimestamp(currentPacked);
                long nowSec = System.currentTimeMillis() / 1000L;

                // 1. 跨秒惰性填充，计算物理桶最新可用令牌存量 (Available Tokens)
                long availableTokens = lazyRefillTokens(cfg, unpackTokens(currentPacked), lastTimestampSec, nowSec);
                if (availableTokens <= 0) {
                    return 0;
                }

                // 2. 实际上最多可以获得的数量 (受限于当前物理桶存量)
                int maxFeasibleTokens = (int) Math.min((long) requestSize, availableTokens);

                // 3. 当处于低水位告警状态时，按多 EventLoop 线程平摊配额保护性裁切，避免单个大 Batch 线程掏空残余令牌造成饥饿
                int actualGranted = applyLowWatermarkQuotaProtection(cfg, maxFeasibleTokens, availableTokens);

                long updatedSec = (nowSec > lastTimestampSec) ? nowSec : lastTimestampSec;

                // 4. 无锁原子 CAS 一次性打包更新剩余令牌与最新时间戳
                if (statePacked.compareAndSet(currentPacked, pack(availableTokens - actualGranted, updatedSec))) {
                    return actualGranted;
                }
            }
        }

        /**
         * 低水位线安全裁切保护：如果进入低水位区，应用 EventLoop 线程公平平摊配额，避免单线程过度囤积
         */
        private int applyLowWatermarkQuotaProtection(ConfigSnapshot cfg, int maxFeasibleTokens, long availableTokens) {
            if (!isLowWatermark(cfg, availableTokens)) {
                return maxFeasibleTokens;
            }
            int threadFairQuota = calculateThreadFairQuota(availableTokens);
            return Math.min(maxFeasibleTokens, threadFairQuota);
        }

        /**
         * 检查当前全局物理桶存量是否落入 1% 低水位线告警区
         */
        private boolean isLowWatermark(ConfigSnapshot cfg, long availableTokens) {
            return availableTokens < (long) (cfg.capacity() * LOW_WATERMARK_RATIO);
        }

        /**
         * 低水位线公平平摊配额计算 (限制单次最高划转上限为 availableTokens / EventLoop线程数，保证残余令牌公平分配，防饥饿)
         */
        private int calculateThreadFairQuota(long availableTokens) {
            return Math.max(1, (int) (availableTokens / DEFAULT_EVENT_LOOP_THREADS));
        }

        /**
         * 跨秒惰性填充刷新物理桶令牌 (Lazy Refill)
         */
        private long lazyRefillTokens(ConfigSnapshot cfg, long currentTokens, long lastTimestampSec, long nowSec) {
            if (nowSec > lastTimestampSec) {
                return Math.min((long) cfg.capacity(), currentTokens + (nowSec - lastTimestampSec) * (long) cfg.tokensPerSec());
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
     */
    public static class ThreadTokenBuffer {

        private static final int MIN_BATCH_FETCH_STEP = 4;
        private static final int INITIAL_BATCH_FETCH_STEP = 16;
        private static final int MAX_BATCH_FETCH_STEP = 512;

        /** 动态划转目标采样间隔上下限 (ms) */
        private static final long MIN_FETCH_INTERVAL_MILLIS = 15L;
        private static final long MAX_FETCH_INTERVAL_MILLIS = 200L;

        private final GlobalTokenBucket globalTokenBucket;
        private int threadLocalRemainingTokens = 0;
        private int currentAdaptiveFetchStep = INITIAL_BATCH_FETCH_STEP;
        private long exhaustionCircuitBreakUntilMillis = 0L;
        private long lastFetchTimestampMillis = System.currentTimeMillis();
        private long leaseExpireTimestampMillis = 0L;
        private int lastBatchGrantedTokens = 0;

        public ThreadTokenBuffer(GlobalTokenBucket globalTokenBucket) {
            this.globalTokenBucket = globalTokenBucket;
            this.lastFetchTimestampMillis = System.currentTimeMillis();
        }

        /**
         * 尝试获取 1 个令牌 (优先消耗线程私有缓冲区有效租约，若租约过期则自动清零重新划转)
         */
        public boolean tryAcquire() {
            long currentTimeMillis = System.currentTimeMillis();

            if (tryConsumeValidLeaseToken(currentTimeMillis)) {
                return true;
            }

            if (isQuotaExhaustionCircuitBroken(currentTimeMillis)) {
                return false;
            }

            adjustFetchStepByConsumptionRate(currentTimeMillis);

            int actualGrantedTokens = globalTokenBucket.grantBatchTokens(currentAdaptiveFetchStep);

            if (actualGrantedTokens > 0) {
                threadLocalRemainingTokens = actualGrantedTokens - 1;
                // 低水位主动降维：若实际划转量小于申请步长，同步收缩当前步长，防止下一次继续用大 Step 暴击全局桶引发争用风暴
                syncAdaptiveFetchStepToSupply(actualGrantedTokens);
                recordSuccessfulFetchSample(currentTimeMillis, actualGrantedTokens);
                return true;
            } else {
                // 全局配额极度不足/划转失败，触发自适应配额枯竭短路熔断
                handleQuotaExhaustionCircuitBreak(currentTimeMillis);
                return false;
            }
        }

        /**
         * 校验当前线程是否正处于全局配额枯竭的短路熔断状态 (Circuit Broken State)
         */
        private boolean isQuotaExhaustionCircuitBroken(long currentTimeMillis) {
            return currentTimeMillis < exhaustionCircuitBreakUntilMillis;
        }

        /**
         * 校验并尝试扣减有效的线程私有租约令牌 (如果租约已过期，自动清空失效旧令牌)
         */
        private boolean tryConsumeValidLeaseToken(long currentTimeMillis) {
            if (threadLocalRemainingTokens <= 0) {
                return false;
            }
            if (isLeaseExpired(currentTimeMillis)) {
                invalidateExpiredLeaseTokens();
                return false;
            }
            threadLocalRemainingTokens--;
            return true;
        }

        private boolean isLeaseExpired(long currentTimeMillis) {
            return currentTimeMillis >= leaseExpireTimestampMillis;
        }

        private void invalidateExpiredLeaseTokens() {
            this.threadLocalRemainingTokens = 0;
        }

        /**
         * 低水位主动降维同步：当实际供给不足申请量时，自适应步长立刻下调对齐供给量
         */
        private void syncAdaptiveFetchStepToSupply(int actualGrantedTokens) {
            if (actualGrantedTokens < currentAdaptiveFetchStep) {
                currentAdaptiveFetchStep = Math.max(MIN_BATCH_FETCH_STEP, actualGrantedTokens);
            }
        }

        /**
         * 记录一次成功划转的净采样点并计算租约到期时间
         */
        private void recordSuccessfulFetchSample(long currentTimeMillis, int actualGrantedTokens) {
            this.lastFetchTimestampMillis = currentTimeMillis;
            this.lastBatchGrantedTokens = actualGrantedTokens;
            updateLeaseExpiration(currentTimeMillis, actualGrantedTokens);
        }

        /**
         * 根据划转令牌数量与当前 QPS 填充速率，计算并更新物理租约到期时间戳 (Lease Expiration Settlement)
         * 公式：leaseDurationMillis = (actualGrantedTokens * 1000ms) / tokensPerSec
         */
        private void updateLeaseExpiration(long currentTimeMillis, int actualGrantedTokens) {
            int tokensPerSec = globalTokenBucket.getConfigSnapshot().tokensPerSec();
            long leaseDurationMillis = Math.max(1L, ((long) actualGrantedTokens * 1000L) / (long) tokensPerSec);
            this.leaseExpireTimestampMillis = currentTimeMillis + leaseDurationMillis;
        }

        /**
         * 处理配额枯竭短路熔断：平滑折半收缩步长、清除污染采样点并触发带 Jitter 随机错峰的短路冷静避退
         */
        private void handleQuotaExhaustionCircuitBreak(long currentTimeMillis) {
            decayFetchStepOnExhaustion();
            clearExhaustionPollutedSamplingPoint();
            triggerExhaustionCircuitCooldown(currentTimeMillis);
        }

        /**
         * 配额枯竭/划转失败时平滑折半收缩划转步长 (如 256->128->64)
         */
        private void decayFetchStepOnExhaustion() {
            currentAdaptiveFetchStep = Math.max(MIN_BATCH_FETCH_STEP, currentAdaptiveFetchStep >> 1);
        }

        /**
         * 划转失败时清除已被避退耗时污染的采样点，彻底防止后续错误地根据被污染的旧采样点计算消耗速率
         */
        private void clearExhaustionPollutedSamplingPoint() {
            this.lastFetchTimestampMillis = 0L;
            this.lastBatchGrantedTokens = 0;
        }

        /**
         * 依据收缩后的步长，触发自适应配额枯竭短路熔断冷静期
         */
        private void triggerExhaustionCircuitCooldown(long currentTimeMillis) {
            exhaustionCircuitBreakUntilMillis = currentTimeMillis + getExhaustionCooldownByFetchStep(currentAdaptiveFetchStep);
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
         * 根据当前净采样点与基于全局 QPS 速率算出的划转目标间隔，计算基于消耗速率的划转步长 (EMA 平滑)
         */
        private void computeRateBasedFetchStep(long currentTimeMillis) {
            long elapsedMillis = Math.max(1L, currentTimeMillis - lastFetchTimestampMillis);
            long targetFetchInterval = calculateRateBasedBaseFetchInterval();

            // 注意：使用 (lastBatchGrantedTokens * targetFetchInterval) / elapsedMillis 避免整数除法截断为 0
            int targetFetchStep = (int) Math.min(
                    MAX_BATCH_FETCH_STEP,
                    Math.max(MIN_BATCH_FETCH_STEP, (lastBatchGrantedTokens * targetFetchInterval) / elapsedMillis)
            );
            // 采用 EMA 指数平滑更新步长：(当前步长 + 目标步长) / 2
            currentAdaptiveFetchStep = (currentAdaptiveFetchStep + targetFetchStep) >> 1;
        }

        /**
         * 基于全局 QPS 填充速率动态计算最佳基础划转采样间隔 (ms)
         */
        private long calculateRateBasedBaseFetchInterval() {
            int tokensPerSec = globalTokenBucket.getConfigSnapshot().tokensPerSec();
            if (tokensPerSec >= 200_000) return 15L; // 超高 QPS：15ms，避开 512 步长截断
            if (tokensPerSec >= 50_000)  return 30L; // 高 QPS：30ms
            if (tokensPerSec >= 10_000)  return 50L; // 标准 QPS：50ms
            return 100L;                             // 低 QPS：100ms，精细化管控
        }

        /**
         * 根据当前划转步长 (fetchStep) 梯度的收缩程度，决议配额枯竭短路熔断冷静期窗口 (ms)
         */
        private long getExhaustionCooldownByFetchStep(int fetchStep) {
            if (fetchStep <= 2) return 5L;
            if (fetchStep <= 8) return 3L;
            if (fetchStep <= 32) return 2L;
            return 1L;
        }
    }
}
