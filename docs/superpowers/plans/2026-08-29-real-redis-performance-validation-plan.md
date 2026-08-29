# Real Redis Performance Validation Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** 修正真实 Redis 的 RESP2 限流协议并提供可复现的功能验证与 Lettuce 基线性能对比。

**Architecture:** 原生 Netty 客户端继续使用批量 RESP2 EVALSHA，但编码完整的 key/ARGV 并在启动时 SCRIPT LOAD；Lettuce 同步客户端执行同一个 Lua 脚本作为常见方案基线。PowerShell 脚本固定 Docker Redis 版本、数据规模和 Maven 参数，测试输出 CSV 与 Markdown 摘要。

**Tech Stack:** Java 17, Maven Surefire/JUnit 5, Netty, Lettuce, Redis 7.2 Docker, PowerShell.

---

### Task 1: 修正 RESP2 Lua 限流命令契约

**Files:**
- Modify: `src/main/java/com/netty/limiter/util/Resp2Encoder.java`
- Modify: `src/main/java/com/netty/limiter/limiter/UserRateLimiterOperate.java`
- Test: `src/test/java/com/netty/limiter/util/Resp2EncoderTest.java`

- [ ] **Step 1: Write the failing encoder test** asserting the encoded command contains `EVALSHA`, `1`, a deterministic key, and five ARGV values (`now/max/refill/ttl/requested`).
- [ ] **Step 2: Run `mvn -Dtest=Resp2EncoderTest test` and observe failure because the current encoder emits only the UID and no ARGV values.**
- [ ] **Step 3: Implement `encodeResp2EvalSha(ByteBuf, byte[], String key, long nowMs, int maxTokens, int refillRate, int ttlSec, int requested)` and update the limiter to pass configured values; preserve the old overload as a delegating compatibility method.
- [ ] **Step 4: Run the focused test and then `mvn -DskipTests package` to verify compilation.**
- [ ] **Step 5: Commit with `git add src/main src/test && git commit -m "fix: encode complete redis token bucket command"`.**

### Task 2: Add real Redis integration validation

**Files:**
- Create: `src/test/java/com/netty/limiter/limiter/RealRedisRateLimiterIntegrationTest.java`
- Modify: `pom.xml` (explicit Lettuce test dependency if required)

- [ ] **Step 1: Write tests that connect to `REDIS_HOST`/`REDIS_PORT`, fail with a clear Docker instruction when unavailable, load `LuaSha1Util.DEFAULT_LUA_SCRIPT`, and assert first request returns 1, exhausted request returns 0, and a later timestamp refills.**
- [ ] **Step 2: Add a Pub/Sub assertion for `NETTY_LIMITER_BAN_CHANNEL` using a dedicated Lettuce connection and a bounded wait.**
- [ ] **Step 3: Run against a Redis 7.2 container and verify all assertions use server responses, not an embedded socket stub.**
- [ ] **Step 4: Commit with `git add pom.xml src/test && git commit -m "test: validate limiter against real redis"`.**

### Task 3: Build reproducible optimized-vs-baseline benchmark

**Files:**
- Create: `src/test/java/com/netty/limiter/limiter/RedisLimiterComparisonBenchmarkTest.java`
- Create: `scripts/run-real-redis-validation.ps1`
- Create: `docs/real-redis-performance.md`

- [ ] **Step 1: Add a benchmark test with fixed defaults (16 threads, 2,000 operations/thread, 32-item native batches, fixed UID seed) and warmup excluded from measurement.**
- [ ] **Step 2: Measure baseline using Lettuce synchronous `EVALSHA` per operation and optimized path using `UserRateLimiterOperate.acquire0GcUidBatch`; collect elapsed time, ops/sec, response/error counters, and percentile samples where responses are available.**
- [ ] **Step 3: Write a CSV row per scenario to `target/perf-results/redis-limiter-comparison.csv` and print a Markdown-compatible summary.**
- [ ] **Step 4: Implement the PowerShell runner to create a uniquely named `redis:7.2-alpine` container on port 6379, wait for `redis-cli ping`, run `mvn -Dtest=RealRedisRateLimiterIntegrationTest,RedisLimiterComparisonBenchmarkTest test`, and remove only that container in a `finally` block.**
- [ ] **Step 5: Document exact prerequisites, commands, fixed parameters, data format, expected artifacts, and interpretation limits (loopback Redis, single JVM, no production extrapolation).**
- [ ] **Step 6: Commit with `git add src/test scripts docs pom.xml && git commit -m "test: add reproducible redis benchmark runner"`.**

### Task 4: Full verification and report refresh

**Files:**
- Modify: `gateway_performance_benchmark_report.md`

- [ ] **Step 1: Run `mvn test` with the Docker Redis runner and capture exit code plus test counts.**
- [ ] **Step 2: Run the runner twice with identical parameters and compare CSV schema/results for reproducibility; retain both raw outputs under `target/perf-results`.**
- [ ] **Step 3: Replace unverified historical numbers in the report with the measured dataset, commands, environment, and caveats; distinguish microbenchmarks from end-to-end Redis results.**
- [ ] **Step 4: Run final verification commands (`mvn test`, runner, `git diff --check`) and report only values present in fresh output.**
- [ ] **Step 5: Commit the report refresh with `git add gateway_performance_benchmark_report.md && git commit -m "docs: publish real redis benchmark results"`.**
