# Comprehensive Performance Comparison Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add reproducible correctness and performance comparisons for the project's major self-built fast paths against common Java/Netty implementations.

**Architecture:** Add test-only baselines and deterministic datasets while making only correctness fixes that are proven by failing tests. Each benchmark validates the same logical result for both implementations, writes machine-readable CSV data, and is documented separately from full-stack claims. Real Redis remains an external Docker dependency for Redis evidence.

**Tech Stack:** Java 17, Maven, JUnit 5, Netty `ByteBuf`, JJWT 0.11.5 test dependencies, JDK concurrency primitives, Docker Redis 7.x, PowerShell.

---

### Task 1: Add Common JWT Baseline Dependency and Dataset Helpers

**Files:**
- Modify: `pom.xml`
- Create: `src/test/java/com/netty/limiter/benchmark/BenchmarkDataset.java`
- Test: `src/test/java/com/netty/limiter/benchmark/BenchmarkDatasetTest.java`

- [ ] **Step 1: Write the failing dataset test**

Create deterministic JWT and numeric datasets with fixed seed, assert stable size, UID range, and valid token shape.

- [ ] **Step 2: Run the focused test and confirm the expected missing-class failure**

Run: `mvn -Dtest=BenchmarkDatasetTest test`

Expected: FAIL because `BenchmarkDataset` does not exist.

- [ ] **Step 3: Add JJWT test dependencies and implement the helper**

Add `jjwt-api`, `jjwt-impl`, and `jjwt-jackson` version `0.11.5` with `test` scope. Implement deterministic token generation using the existing `secret`, fixed UIDs, fixed future expiration, and no random runtime state.

- [ ] **Step 4: Run the focused test and confirm it passes**

Run: `mvn -Dtest=BenchmarkDatasetTest test`

Expected: PASS with all dataset assertions green.

- [ ] **Step 5: Commit**

```bash
git add pom.xml src/test/java/com/netty/limiter/benchmark
git commit -m "test: add deterministic benchmark datasets"
```

### Task 2: Compare Header Matching Against String Parsing

**Files:**
- Modify: `src/test/java/com/netty/limiter/handler/JwtHeaderSecurityBenchmarkTest.java`
- Create: `src/test/java/com/netty/limiter/handler/HeaderMatchingComparisonTest.java`

- [ ] **Step 1: Write correctness cases for common and primitive matchers**

Cover `Authorization`, mixed-case names, `Token`, `Userid`, wrong length, and wrong content. Both matchers must return the same result for the same `ByteBuf` slice.

- [ ] **Step 2: Run the focused test and confirm the new comparison API is missing**

Run: `mvn -Dtest=HeaderMatchingComparisonTest test`

Expected: FAIL because the comparison test class is not present.

- [ ] **Step 3: Implement the test-only common baseline and benchmark**

The baseline must call `buf.toString(start, len, US_ASCII).equalsIgnoreCase(expected)`. The optimized side must invoke the production fixed-width matcher through a reflective test handle because the matcher is private. Use 2,000,000 warmup iterations and 20,000,000 measured iterations, consume boolean results, and write a CSV row with throughput and result counts.

- [ ] **Step 4: Run focused correctness and benchmark tests**

Run: `mvn -Dtest=HeaderMatchingComparisonTest,JwtHeaderSecurityBenchmarkTest test`

Expected: PASS and console output for both throughput comparisons.

- [ ] **Step 5: Commit**

```bash
git add src/test/java/com/netty/limiter/handler
git commit -m "test: compare primitive and string header matching"
```

### Task 3: Compare JWT Parser Cold and Cached Paths

**Files:**
- Create: `src/test/java/com/netty/limiter/util/JwtParserComparisonBenchmarkTest.java`
- Modify: `src/test/java/com/netty/limiter/util/ZeroGcJwtAuthTest.java`

- [ ] **Step 1: Add failing correctness assertions for JJWT parity**

For the same deterministic valid, expired, malformed, and tampered tokens, assert that the production parser and JJWT agree on valid/invalid status and UID where valid.

- [ ] **Step 2: Run the focused test and confirm the comparison class is missing**

Run: `mvn -Dtest=JwtParserComparisonBenchmarkTest test`

Expected: FAIL because the class does not exist.

- [ ] **Step 3: Implement JJWT baseline and two benchmark modes**

Use one mode that forces production cold-path parsing with unique signatures and one mode that repeats a cached token. The JJWT side must parse the same token and verify the same HMAC key. Record throughput, valid count, invalid count, and errors in `target/perf-results/jwt-parser-comparison.csv`.

- [ ] **Step 4: Run focused tests and inspect parity counters**

Run: `mvn -Dtest=JwtParserComparisonBenchmarkTest,ZeroGcJwtAuthTest test`

Expected: PASS with equal valid/invalid counters and no parser disagreement.

- [ ] **Step 5: Commit**

```bash
git add src/test/java/com/netty/limiter/util
git commit -m "test: compare zero gc jwt parser with jjwt"
```

### Task 4: Validate and Benchmark Primitive Blacklist Storage

**Files:**
- Modify: `src/main/java/com/netty/limiter/cache/LocalBanCache.java`
- Create: `src/test/java/com/netty/limiter/cache/LocalBanCacheTest.java`
- Create: `src/test/java/com/netty/limiter/cache/LocalBanCacheComparisonBenchmarkTest.java`

- [ ] **Step 1: Write failing behavior tests**

Cover UID hit/miss/expiry, IPv4 hit/miss/expiry, IPv6 pair hit/miss, and a deliberately found equal-hash/different-IP pair. Assert no false positive for the collision pair.

- [ ] **Step 2: Run the focused test and verify the expected failures**

Run: `mvn -Dtest=LocalBanCacheTest test`

Expected: FAIL on the deliberately colliding IP pair because the current table stores only a mixed hash.

- [ ] **Step 3: Make IP storage collision-verifiable without reintroducing Strings**

Add primitive `ipHighKeys` and `ipLowKeys` arrays. Keep the mixed hash only for choosing the probe start; compare both original 64-bit halves before returning a hit. Preserve the existing UID table API and expiry behavior.

- [ ] **Step 4: Implement a test-only `ConcurrentHashMap<String, Long>` baseline**

Use the same logical IP/UID data, convert only the baseline representation to strings, and measure hit, miss, and expired lookup throughput. The production side must use `LocalBanCache` primitive methods.

- [ ] **Step 5: Run behavior and benchmark tests**

Run: `mvn -Dtest=LocalBanCacheTest,LocalBanCacheComparisonBenchmarkTest test`

Expected: PASS with zero false positives and CSV output at `target/perf-results/local-ban-comparison.csv`.

- [ ] **Step 6: Commit**

```bash
git add src/test/java/com/netty/limiter/cache
git commit -m "test: compare primitive and string ban caches"
```

### Task 5: Compare Local Token Bucket Against Atomic Baseline

**Files:**
- Create: `src/test/java/com/netty/limiter/limiter/LocalGlobalRateLimiterComparisonBenchmarkTest.java`

- [ ] **Step 1: Write accounting tests for capacity and refill boundaries**

Assert that a fixed-capacity bucket grants no more than capacity before refill, never returns negative grants, and refills after a bounded real-time interval using a small test bucket. Record the wait bound so the test remains deterministic enough for local CI.

- [ ] **Step 2: Run the focused test and confirm the test-only baseline is absent**

Run: `mvn -Dtest=LocalGlobalRateLimiterComparisonBenchmarkTest test`

Expected: FAIL because the comparison test is not implemented.

- [ ] **Step 3: Implement the common baseline and concurrent benchmark**

Implement a test-only `AtomicLong` token bucket with synchronized refill timestamp handling and compare it with `GlobalTokenBucket` under 16 producer threads and 800,000 total requests. Record total attempts, grants, rejects, elapsed time, and errors. Keep both buckets configured identically.

- [ ] **Step 4: Run the benchmark and verify accounting**

Run: `mvn -Dtest=LocalGlobalRateLimiterComparisonBenchmarkTest test`

Expected: PASS, grants plus rejects equal attempts for both implementations, CSV at `target/perf-results/local-rate-limiter-comparison.csv`.

- [ ] **Step 5: Commit**

```bash
git add src/test/java/com/netty/limiter/limiter/LocalGlobalRateLimiterComparisonBenchmarkTest.java
git commit -m "test: compare adaptive and atomic token buckets"
```

### Task 6: Compare MPSC RingBuffer Against JDK Queues

**Files:**
- Create: `src/test/java/com/netty/limiter/limiter/UidRingBufferCommonBaselineBenchmarkTest.java`

- [ ] **Step 1: Write queue accounting tests**

Use 16 producers and one consumer with positive unique UIDs. Assert the consumer receives exactly 16,000,000 values, no duplicate UID, no zero sentinel, and no producer-side errors for the custom queue and each JDK baseline.

- [ ] **Step 2: Run the focused test and confirm the new benchmark is missing**

Run: `mvn -Dtest=UidRingBufferCommonBaselineBenchmarkTest test`

Expected: FAIL because the class is not present.

- [ ] **Step 3: Implement JDK bounded and lock-free baselines**

Compare `ArrayBlockingQueue<Long>` and `ConcurrentLinkedQueue<Long>` against `UidRingBuffer`, using the same producer/consumer lifecycle, warmup, operation count, and accounting checks. Report throughput and errors to `target/perf-results/uid-ring-buffer-comparison.csv`.

- [ ] **Step 4: Run the benchmark and inspect all accounting counters**

Run: `mvn -Dtest=UidRingBufferCommonBaselineBenchmarkTest test`

Expected: PASS with exact consumption counts for every scenario.

- [ ] **Step 5: Commit**

```bash
git add src/test/java/com/netty/limiter/limiter/UidRingBufferCommonBaselineBenchmarkTest.java
git commit -m "test: compare offheap ring buffer with jdk queues"
```

### Task 7: Real Redis Reproduction and Report Integration

**Files:**
- Modify: `scripts/run-real-redis-validation.ps1`
- Modify: `docs/real-redis-performance.md`
- Modify: `gateway_performance_benchmark_report.md`
- Modify: `src/test/java/com/netty/limiter/limiter/UserRateLimiterRealRedisBenchmarkTest.java`

- [ ] **Step 1: Label the embedded transport benchmark as synthetic**

Rename or annotate `UserRateLimiterRealRedisBenchmarkTest` so its embedded Netty byte sink is explicitly synthetic transport coverage. Real Redis evidence must come only from `RealRedisRateLimiterIntegrationTest` and `RedisLimiterComparisonBenchmarkTest`.

- [ ] **Step 2: Run the focused Redis tests against Docker Redis**

Run: `.\scripts\run-real-redis-validation.ps1 -UseExistingRedis -RedisPort 6379 -Threads 16 -OpsPerThread 2000`

Expected: PASS with `PING`, `SCRIPT LOAD`, token-bucket, Pub/Sub, and comparison CSV results.

- [ ] **Step 3: Document all new benchmark commands and interpretation**

Add fixed dataset definitions, output paths, baseline semantics, warmup/repetition settings, and explicit notes about fire-and-forget versus response-waiting metrics. Mark synthetic transport tests as synthetic.

- [ ] **Step 4: Run the full verification suite**

Run: `mvn test`

Expected: all tests pass with zero failures, errors, and skips when Redis is available.

- [ ] **Step 5: Run formatting/diff verification**

Run: `git diff --check`

Expected: exit code 0.

- [ ] **Step 6: Commit**

```bash
git add scripts/run-real-redis-validation.ps1 docs/real-redis-performance.md gateway_performance_benchmark_report.md src/test/java/com/netty/limiter/limiter/UserRateLimiterRealRedisBenchmarkTest.java
git commit -m "test: document comprehensive performance comparisons"
```

### Task 8: Add Follow-up Optimization Proposal and Regression Guardrails

**Files:**
- Modify: `gateway_performance_benchmark_report.md`
- Create: `docs/performance-optimization-roadmap.md`

- [ ] **Step 1: Write the roadmap from measured bottlenecks**

For each candidate optimization, document current evidence, expected mechanism, risk, and a falsification metric. Include primitive channel state, JMH allocation profiling, collision-verifiable ban keys, Redis response-drain mode, adaptive flush tuning, and CI confidence intervals.

- [ ] **Step 2: Add regression thresholds to the report**

Define non-blocking reference ranges from repeated local runs and state that thresholds are machine-specific until normalized hardware is used.

- [ ] **Step 3: Run final verification**

Run: `mvn test` and then `git diff --check`.

Expected: both commands exit 0 and the report links every generated CSV.

- [ ] **Step 4: Commit**

```bash
git add docs/performance-optimization-roadmap.md gateway_performance_benchmark_report.md
git commit -m "docs: add performance optimization roadmap"
```
