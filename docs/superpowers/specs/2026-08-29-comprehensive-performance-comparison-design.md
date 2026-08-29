# Comprehensive Performance Comparison Design

## Goal

建立一套可复现的功能验证与性能对比测试，覆盖项目中最有价值的自研路径，并与语义相同的常见实现比较，结果写入 CSV 和说明文档。

## Working Environment

- Java 17、Maven、Netty。
- Docker 中真实 Redis，默认 `127.0.0.1:6379`，当前 Redis 7.4.10。
- 单 JVM、本机回环网络、固定测试数据；结果用于相对比较，不外推生产容量。

## Scope and System Boundary

Inside the boundary:

1. Netty `ByteBuf` Header 字段名匹配。
2. JWT 快路径缓存、HMAC 验签和 Payload 提取。
3. 本地 UID/IP 黑名单查询。
4. 节点级本地令牌桶。
5. UID MPSC RingBuffer。
6. Redis 限流命令的客户端提交和同步往返路径。

Outside the boundary:

- 完整生产 HTTP 服务容量、真实公网网络、TLS、磁盘和下游业务处理。
- 未经实际采集的 GC、P99.9 和异步 Redis 端到端延迟结论。

## Comparisons

| Self-built path | Common baseline | Primary metric |
|---|---|---|
| Fixed-width SWAR/int Header matcher | `ByteBuf.toString` plus case-insensitive string comparison | Matcher throughput and correctness |
| `ZeroGcJwtParser` | JJWT parser with the same HMAC secret and claims | Cold parse and repeated-hit throughput |
| Primitive flat ban table | `ConcurrentHashMap<String, Long>` expiry table | Hit, miss, expiry throughput and allocation-relevant work |
| Thread-local adaptive token bucket | Atomic/synchronized token bucket | 16-thread acquire throughput and decisions |
| Unsafe off-heap MPSC queue | JDK bounded queue and lock-free queue | 16 producers/1 consumer throughput and loss/duplicate count |
| Native RESP2 batching | Lettuce synchronous `EVALSHA` | Real Redis throughput, errors, and server call count |

Each benchmark must use the same logical input, operation count, thread model, warmup, and result validation for both sides. Fire-and-forget submission throughput is reported separately from response-waiting throughput.

## Functional Validation

- Preserve the existing 15-test suite and real Redis integration assertions.
- Add direct tests for UID/IP ban hit, miss, expiry, and collision-safe behavior.
- Add JWT cold-path, cache-hit, malformed, tampered, and expired cases.
- Add queue accounting assertions: every positive UID is consumed exactly once.
- Add token-bucket accounting assertions for capacity and refill boundaries.

## Reproducibility

- Fixed seeds and deterministic JWT/IP/UID datasets.
- Test-only JJWT dependency if needed for the common JWT baseline.
- Benchmark classes emit CSV rows with scenario, threads, operations, elapsed time, throughput, errors, and validation counters.
- PowerShell runner reuses an existing Redis or creates and removes only its own container.
- Documentation records exact commands and interpretation limits.

## Further Optimization Candidates

1. Replace per-request channel `Long` attribute reads with primitive channel state or a dedicated connection state object to avoid boxing on the hot path.
2. Replace hand-timed microbenchmarks with JMH forks, profilers, and allocation rate measurements before publishing absolute numbers.
3. Make the local ban table retain a collision-verifiable key, not only a mixed hash, while preserving primitive storage.
4. Add a bounded response-drain/ack mode for native Redis so submission throughput and end-to-end acceptance can be measured independently.
5. Tune adaptive batch and flush thresholds from latency/throughput curves instead of a single fixed point.
6. Add controlled CPU pinning, heap settings, and repeated-run confidence intervals to CI performance gates.

## Acceptance Criteria

- All comparisons have a documented baseline and metric definition.
- Functional tests fail on incorrect allow/reject, expiry, loss, duplication, or malformed input.
- Real Redis tests use the external Docker Redis, with no embedded fake server counted as Redis evidence.
- `mvn test` and `git diff --check` pass.
- The report distinguishes measured results from historical examples and documents reproduction steps.
