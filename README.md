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
   - 本项目自研的 **0-GC 双表轮转缓存 (`JwtSigUidCache`)**、**64-bit Bit-Packing 无锁令牌桶 (`LocalGlobalRateLimiter`)**、**SWAR 64-bit 字节比对与 DFA 流式解码**、**扁平无锁原子黑名单 (`LocalBanCache`)** 以及 **MPSC 堆外 RingBuffer (`UidRingBuffer`)**，为高性能 Java 系统级编程提供了零 GC、亚微秒级的参考实现。

---

## 🌟 核心特性 (Key Features)

### 1. 🛡️ 零堆内存分配模块（如何实现 0-GC）
* **堆外内存池化 (Pooled Direct Memory)**：利用 Netty `PooledDirectByteBuf` 统一接管底层 TCP/HTTP 报文字节流，实现零拷贝接收、透传与内存即时回收。
* **线程局部缓冲区池化重用 (`FastThreadLocal`)**：在 EventLoop 线程内 $O(1)$ 无锁复用 `Mac` 算法实例、加解密/编解码缓冲区、IP 输出数组与 Mode A 攒批 `long[]` 数组。
* **扁平原子黑名单 (`LocalBanCache`)**：使用 JUC `VarHandle` 驱动的双 `long` 无锁原子数组（开放寻址探查 $MAX\_PROBE = 16$），彻底抛弃 `ConcurrentHashMap<String, BanInfo>`。
* **全基本数据类型解耦**：事件监听器 `RateLimitEventListener` 与状态传参采用全基本类型 `(long ipHigh, long ipLow, long userId, int rejectCode, int reasonCode)`，彻底杜绝对象装箱与 DTO 依赖。
* **静态预编译响应 (`SecurityResponses`)**：预编译 401 / 403 / 429 静态堆外 ByteBuf 报文，拦截写回时仅增减引用计数，0 堆内存分配。

### 2. ⚡ 高并发高吞吐模块（如何实现高并发）
* **极前置物理切入 (Pre-Codec Security Line)**：挂载于 Reactor-Netty `HttpServerCodec` 原生解码器之前，非法与超限请求 0 次 HTTP 对象解析、0 次路由匹配，裸字节层即时短路拒接并切断 TCP，单机吞吐提升近 10 倍。
* **64-bit Bit-Packing 节点令牌桶 (`LocalGlobalRateLimiter`)**：使用 `AtomicLong` 将 64 位拆分（高 32 位存存量令牌，低 32 位存截断时间戳），单次 CAS 完成惰性填充与原子扣减，无锁、无锁争用、Cache-Line 友好。
* **EventLoop 线程私有 AIMD 缓冲区**：优先本地无锁扣减，耗尽时利用 EMA 平滑自适应调整批量划转步长；遇争用/枯竭时触发自适应冷静避退（Cooldown Backoff）。
* **0-GC RESP2 原生驱动 (Raw RESP2 Protocol Driver)**：针对 Per-UID 异步限流上报，绕过 Redis 客户端驱动包装开销，在 Channel 上直接打包写出 raw RESP2 二进制字节流，单机异步并发上报突破 **3,500,000+ ops/sec**。

### 3. 🚀 极速计算模块（如何实现计算的高速）
* **SWAR (SIMD Within A Register) 64 位并行匹配**：使用 `long` 64 位整数与位掩码 `| 0x2020202020202020L` 一次性并行匹配 8 字节（如 `authorization` / `userid`），淘汰传统 `toLowerCase()` 循环与字符串比较。
* **DFA 状态机流式解码**：基于 DFA 状态机在物理字节流上原位解析 JSON `"uid":` 与 Base64URL 字符，无正则表达式，无需语法解析树。
* **0-GC 双表轮转缓存 (`JwtSigUidCache`)**：结合 64-bit `xxHash64` 哈希 + 8 字节前缀二重防碰撞校验，配合 **VarHandle Acquire/Release** 屏障驱动的双静态 Flat Table 轮转缓存（粗粒度 LRU），实现 ~ns 级快路径 (Fast Path) 极速验签。
* **双 64 位 IPv4 / IPv6 统一哈希算法**：基于 Composite Key (`ipHigh ^ (ipLow * 31)`) 的高速位混淆哈希，极速完成双 64 位 IP 的开放寻址检索。

---

## 🛠️ 0-GC 技术手段与工程架构总结 (Zero-GC Architecture)

在 Java 高并发与 Netty 网络编程中，传统框架 90% 以上的临时对象分配都源于“过度通用性”与对象封装。本项目通过以下 **4 大核心技术支柱** 实现 100% 0-GC：

```
                         【过度通用性 (Generality)】
                     Spring / Servlet / Generic Drivers
                        ▲ 灵活、易用、适合普通业务
                        │
                        │ 架构权衡 (Trade-off)
                        │
                        ▼ 高吞吐、微秒延迟、0 堆分配
                       【特化 0-GC (Specialization)】
                    Netty / ByteBuf / SWAR / Raw RESP2
```

1. **堆外内存池化 (Pooled Direct Memory)**：利用 Netty `PooledDirectByteBuf` 统一接管底层 TCP/HTTP 报文字节流，实现零拷贝接收、透传与内存即时回收。
2. **线程局部缓冲区复用 (Thread-Local Buffer Pools)**：结合 Netty `FastThreadLocal` 索引复用机制，在 EventLoop 线程内 $O(1)$ 无锁复用 `Mac` 算法实例、编解码缓冲区以及 Mode A 攒批数组。
3. **特化字节流原位解析 (In-Place Stream Parsing)**：打破传统框架“字节 ➔ 字符串 ➔ DTO”的抽象开销，结合 **SWAR 64 位并行位运算**（`| 0x2020202020202020L`）与 **DFA 状态机**，直接在物理字节流上原位提取与比对 IP、JWT 与 UID。
4. **全基本数据类型与扁平结构 (Primitive Data Structures)**：使用 JUC `VarHandle` 驱动的 `long[]` 扁平无锁原子数组替代 `ConcurrentHashMap`；并在拦截响应与事件回调中全链路采用 `long` / `int` 纯基本数据类型与常量原因码（`RateLimitReasonCodes`）。

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
