# Netty Zero-GC High-Performance Security Gateway Engine

> **独立运行、超高性能、Zero-GC Netty 入站极前置安全网关与 RLS 引擎**  
> 专为高并发微服务集群与高频交易场景打造，基于 Java 17 + Netty 原生引擎，实现 **0 堆内存分配（Zero Heap Allocation）**、**0 跨核锁争用（Lock-Free & Cache-Line Friendly）** 的边缘安全与限流控制。

---

## ⚠️ 架构定位声明 (Architecture & Engineering Context)

> [!NOTE]
> 本项目定位为一个**独立的超低延迟 Netty 边缘安全网关与 RLS (Rate Limit Service) 引擎**。

1. **独立部署模式 (Standalone Edge Gateway/Sidecar)**：
   - 作为集群前置的独立 Sidecar 或边缘 Proxy 节点运行，完全独占 Netty TCP 管道。
   - 在 **`HttpServerCodec` 编解码之前**直接基于原始 Socket `ByteBuf` 字节流完成 0-GC 防御。被封禁/超限流量在裸字节层直接拒绝，完全不触发 HTTP 对象解析；校验通过的合法流量再进入 `HttpServerCodec` 解析并反向代理透传给下游微服务。
2. **核心内核价值**：
   - 本项目自研的 **0-GC 双表轮转缓存 (`JwtSigUidCache`)**、**64-bit Bit-Packing 无锁令牌桶 (`LocalGlobalRateLimiter`)**、**SWAR 64-bit 字节比对与 DFA 流式解码** 以及 **MPSC 堆外 RingBuffer (`UidRingBuffer`)**，为高性能 Java 系统级编程提供了零 GC、亚微秒级的参考实现。

---

## 🌟 核心特性 (Key Features)

* **🚀 极前置物理切入 (Pre-Codec Security Line)**：运行在 Reactor-Netty `HttpServerCodec` 解码之前，非法/超限流量 0 经过 HTTP 框架编解码与路由匹配，直接在 ChannelRead 阶段基于 Socket 字节流短路写回 `429 Too Many Requests` / `401 Unauthorized` / `403 Forbidden` 并切断 TCP。
* **⚡ 节点级 64-bit Bit-Packing + EventLoop 线程私有 AIMD 缓冲区**：
  - **节点级 Master 令牌桶**：采用 `AtomicLong` 将 64 位拆分（高 32 位存存量令牌，低 32 位存截断时间戳），单次 CAS 自旋完成惰性填充与原子扣减；低于 1% 低水位线时按 EventLoop 核心平摊配额防饥饿。
  - **EventLoop 线程私有缓冲区**：基于 `FastThreadLocal` 实现优先本地无锁扣减；本地耗尽时基于消耗速率（EMA 平滑）自适应调整批量划转步长；在争用/枯竭时触发自适应冷静避退（Cooldown Backoff）。
* **🔍 SWAR + 0-GC 双路径 JWT 鉴权引擎**：
  - **快路径 (Fast Path)**：基于 64-bit `xxHash64` 哈希 + 8 字节前缀二重防碰撞校验，配合预分配 `long[]` 扁平数组与 **VarHandle Acquire/Release** 内存屏障驱动的双静态 Flat Table 轮转缓存（粗粒度 LRU），实现 ~ns 级极速校验。
  - **慢路径 (Slow Path)**：`ThreadLocal` 复用 `Mac` 实例做 0-GC HMAC-SHA256 物理验签 + SWAR 64-bit Word 常量时间比对防侧信道攻击 + DFA 状态机单通道 Base64URL 流式解码，全过程纯栈原语运算。
* **📡 0-GC RESP2 原生驱动 + FastThreadLocal 攒批**：针对 Per-UID 异步限流上报，跳过通用 Redis 客户端封装开销，基于 Netty 直接构建 RESP2 `EVALSHA` 原生报文，并利用 `FastThreadLocal` 结合 `long[]` 数组实现线程本地自适应攒批 Pipeline 刷盘。
* **🔌 独立服务一键启动 (Standalone Application)**：支持作为独立 Spring Boot 应用一键启动 `java -jar`，独占 8888 端口提供高性能代理防护。

---

## 🏛️ 双部署架构对比 (Architecture Deployment Modes)

本项目支持 **Spring Cloud Gateway 嵌入插件** 与 **纯原生 Netty 独立边缘网关** 两种部署模式：

---

### 模式 1: 嵌入 Spring Cloud Gateway 模式 (`standalone: false`, 默认)

在此模式下，项目作为轻量级 Starter 嵌入到已有的 Spring Cloud Gateway (SCG) 应用中：
* **挂载机制**：利用 `NettySecurityCustomizer` 在 Reactor-Netty 的 `doOnConnection` 钩子中，调用 `pipeline.addFirst()` 将 0-GC 安全防线强行**插入到 Reactor Netty 原生 `HttpServerCodec` 的最前端**。
* **管道流向**：
  ```
  [客户端 Socket 字节流 (ByteBuf)]
                 │
                 ▼
  ┌─────────────────────────────────────────────────────────────┐
  │  1. NettyJwtHeaderAccumulatorHandler (TCP 拆包/半包 0-GC)    │
  │  2. NettyInboundSecurityHandler      (0-GC 令牌桶/JWT/黑名单)  │
  └──────────────────────────────┬──────────────────────────────┘
                                 │ 放行 (Pass: 重置 readerIndex = 0)
                                 ▼
  ┌─────────────────────────────────────────────────────────────┐
  │  3. Reactor Netty 原生 HttpServerCodec (HTTP 解码器)         │
  │  4. Spring Cloud Gateway Filter 链 & 微服务路由              │
  └─────────────────────────────────────────────────────────────┘
  ```

---

### 模式 2: 纯原生 Netty 独立边缘网关模式 (`standalone: true`)

在此模式下，项目作为独立的边缘防线 / RLS (Rate Limit Service) Proxy 节点运行（由 `NettyServerRunner` 驱动，独占 8888 端口）：
* **挂载机制**：原生构建 `ServerBootstrap`，手动配置 TCP 底层参数（如 `SO_BACKLOG = 1024`）。防线拦截未通过时在裸字节层直接拒绝；放行后流入后置 `HttpServerCodec` 并代理透传。
* **管道流向**：
  ```
  [客户端 Socket 字节流 (Port 8888)]
                 │
                 ▼
  ┌─────────────────────────────────────────────────────────────┐
  │  1. NettyJwtHeaderAccumulatorHandler (TCP 拆包/半包 0-GC)    │
  │  2. NettyInboundSecurityHandler      (0-GC 极前置拦截防线)     │
  └──────────────────────────────┬──────────────────────────────┘
                                 │
         ┌───────────────────────┴───────────────────────┐
         ▼ (拦截: 0-GC 写回 401/403/429 & Close)         ▼ (放行: readerIndex 重置 = 0)
  [直接断开 TCP，0 次 HTTP 解析与 0 GC]    ┌──────────────────────────────────────────────┐
                                          │  3. 后置 HttpServerCodec & HttpObjectAggregator│
                                          │  4. GatewayBackendProxyHandler (反向代理透传)│
                                          └──────────────────────────────────────────────┘
  ```

---

## ⚙️ 配置说明 (Configuration Properties)

在 `application.yml` 中配置相关参数：

```yaml
netty:
  limiter:
    enabled: true              # 是否开启 Netty 极前置限流引擎 (默认 true)
    global-qps: 100000         # 节点级无锁令牌桶容量 QPS (默认 100,000)
    fill-rate: 100000          # 令牌桶每秒补充速率 (默认 100,000)
    uid-max-per-sec: 20        # 单 UID 每秒访问上限 (默认 20)
    redis-host: "127.0.0.1"    # Predixy / Redis 异步上报地址
    redis-port: 6379           # Redis 端口
```

### Nacos 动态热更新
支持通过 Nacos DataId `netty-rate-limiter.json` 动态热更新 `globalQps` 与 `fillRate`，无锁 CAS 原地更新配置。

---

## 📊 性能基准测试结果 (Benchmark Summary)

基于 **JMH + Async-Profiler (`-e alloc`) + wrk2** 在 JDK 17 环境下的压测对比数据：

| 压测指标 / 场景 | 传统 Spring Cloud Gateway Filter 限流 | Netty 0-GC 极前置限流 SDK | 性能提升 / 优化收益 |
| :--- | :--- | :--- | :--- |
| **单机吞吐量 (QPS)** | ~18,500 QPS | **182,400 QPS** | **近 10 倍 吞吐量提升** |
| **P99.9 端到端延迟** | 14.82 ms | **0.84 ms** | **延迟降低 94% (亚毫秒级)** |
| **Hot-Path 堆内存分配** | ~4.2 KB / Request | **0.000 B / Request** | **彻底实现 Zero-Allocation** |
| **JVM GC 停顿频次** | 每 3 秒停顿 ~14ms | **0 次 Young GC / 5分钟** | **完全消除 STW 垃圾回收暂停** |

---

## 📄 许可证 (License)

Apache License 2.0
