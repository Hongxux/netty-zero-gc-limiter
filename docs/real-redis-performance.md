# 真实 Redis 功能与性能复现

## 前置条件

- Windows 11、JDK 17、Maven 3.9+、Docker Desktop（Linux containers）
- Docker 引擎可响应 `docker version`
- 首次运行可拉取 `redis:7.2-alpine`

## 一步步执行

在项目根目录运行：

```powershell
.\scripts\run-real-redis-validation.ps1
```

脚本会创建唯一命名的 Redis 7.2 容器，等待 `redis-cli ping` 返回 `PONG`，执行真实 Redis 集成测试和性能对比，最后只删除本次创建的容器。可通过参数调整规模：

```powershell
.\scripts\run-real-redis-validation.ps1 -Threads 16 -OpsPerThread 2000 -RedisPort 6379
```

## 固定测试数据

- UID：线程 `t` 的第 `i` 个请求使用 `t * OpsPerThread + i`。
- Lua 参数：`now_ms=1700000000000`、容量/补充速率均为 `1,000,000`、TTL 2 秒、每次请求 1 个令牌。
- 优化方案：`UserRateLimiterOperate.acquire0GcUidBatch`，每线程 32 条攒批，通过原生 Netty RESP2 写入 Redis。
- 基线方案：Lettuce 6.1 同步连接，每个请求单独执行一次同一 Lua `EVALSHA`。

## 输出与解读

结果写入 `target/perf-results/redis-limiter-comparison.csv`，字段包括场景、线程数、操作数、耗时、提交吞吐和错误数。`native-resp2-batch-submit` 是客户端提交吞吐；该类设计为 fire-and-forget，不等待 Redis 响应，因此不能解释为端到端响应延迟。`lettuce-evalsha-per-request` 等待每次 Redis 响应，更接近常见同步限流调用的端到端成本。

集成测试还会验证：`SCRIPT LOAD` 返回预期 SHA、令牌耗尽返回 0、时间推进后回填返回 1，以及拒绝时在 `NETTY_LIMITER_BAN_CHANNEL` 收到 Pub/Sub 消息。

## 复现与限制

使用相同 JDK、Docker 镜像、线程/操作参数重复运行即可复现同一测试定义；操作系统调度、CPU 频率和 Docker 虚拟化会造成数值波动。数据来自本机回环 Redis、单 JVM 和小规模样本，不能直接外推生产 QPS 或尾延迟。旧报告中的示例数字不作为本次结论依据。
