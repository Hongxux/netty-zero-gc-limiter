# Real Redis Performance Validation Design

## Goal

让项目能够在真实 Redis 上完成可复现的功能验证和性能对比，覆盖当前原生 RESP2 批量链路与业界常见的 Lettuce 同步客户端 + Redis Lua 原子令牌桶基线。

## Scope and success criteria

- 修正原生 RESP2 `EVALSHA` 请求，使其与默认 Lua 脚本的 `KEYS/ARGV` 契约一致，并支持脚本预加载后的真实执行。
- 保留现有业务类和默认配置语义；不改动与本任务无关的工作区修改。
- 提供真实 Redis 集成测试：验证放行、耗尽、过期回填、Redis 黑名单 Pub/Sub 事件以及脚本加载。
- 提供固定版本 Docker Redis、固定随机种子/数据规模/并发参数的 PowerShell 复现入口。
- 输出两种方案的吞吐、p50/p95/p99 延迟、放行/拒绝数、错误数和 Redis 命令量，并明确测试边界（本机回环、非生产环境）。

## Architecture

原生链路继续使用 Netty `ByteBuf` 和单连接批量 RESP2 写入，但命令改为完整的 `EVALSHA sha 1 key now max refill ttl requested`。启动/测试阶段通过 `SCRIPT LOAD` 确保 SHA 已存在；运行时遇到 `NOSCRIPT` 由测试直接报错，避免静默失效。基线使用 Lettuce 同步连接，每次请求执行同一个 Lua 脚本，代表常见的 Redis 客户端 + 服务端原子脚本方案。

测试数据采用固定 UID 序列和固定令牌桶参数，默认 16 线程、每线程 2,000 次、批量大小 32；脚本与参数通过环境变量可调整。所有结果写入 `target/perf-results/*.csv` 与 Markdown 摘要，脚本负责启动/清理 Redis 容器并检查健康状态。

## Components

- `Resp2Encoder`：新增完整令牌桶 EVALSHA 编码入口，保留旧入口仅用于兼容性编译。
- `UserRateLimiterOperate`：按配置传递 key、时间、容量、补充速率、TTL 和请求数；连接建立后预加载脚本并记录 NOSCRIPT/Redis 错误。
- `RealRedisRateLimiterIntegrationTest`：连接外部 Redis（默认 `127.0.0.1:6379`），执行真实脚本断言和 Pub/Sub 断言；Redis 不可用时明确跳过并提示复现命令，而不是启动假服务。
- `RedisLimiterComparisonBenchmarkTest`：同一数据集下运行原生批量链路与 Lettuce 基线，输出 CSV/Markdown 所需原始指标。
- `scripts/run-real-redis-validation.ps1`：固定 Redis 镜像、端口、数据卷生命周期、Maven 命令和结果路径。
- `docs/real-redis-performance.md`：一步步复现说明、参数表、结果解读和限制。

## Error handling and safety

测试只允许连接显式指定的本机 Redis 地址；默认 Docker 端口为 6379。脚本使用专用容器名，运行前检查同名容器并在退出时移除，绝不清理用户其他容器/数据。Redis 连接、脚本加载、响应错误和线程异常均计入错误数并使测试失败。

## Verification

先运行 `mvn test` 验证单元/集成测试，再执行 PowerShell 复现脚本完成真实 Redis 对比。结果必须包含命令退出码、Redis `PING`/`SCRIPT LOAD` 成功证据、两组非零请求数以及 CSV 文件路径；性能数字只报告本次实测值，不沿用旧报告中的未验证样例。
