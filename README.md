# Netty Zero-GC High-Performance Security Gateway & Rate Limiter Engine

<div align="center">

[![GitHub Repository](https://img.shields.io/badge/GitHub-Hongxux%2Fnetty--zero--gc--limiter-181717.svg?style=flat-square&logo=github)](https://github.com/Hongxux/netty-zero-gc-limiter)
[![GitHub Stars](https://img.shields.io/github/stars/Hongxux/netty-zero-gc-limiter?style=flat-square&logo=github)](https://github.com/Hongxux/netty-zero-gc-limiter)
[![GitHub Forks](https://img.shields.io/github/forks/Hongxux/netty-zero-gc-limiter?style=flat-square&logo=github)](https://github.com/Hongxux/netty-zero-gc-limiter)
[![JDK 21](https://img.shields.io/badge/JDK-21-orange.svg?style=flat-square&logo=openjdk)](https://openjdk.org/projects/jdk/21/)
[![Netty 4.1](https://img.shields.io/badge/Netty-4.1.x-blue.svg?style=flat-square&logo=netty)](https://netty.io/)
[![Spring Boot 2.7 / 3.x](https://img.shields.io/badge/Spring%20Boot-2.7%20%7C%203.x-brightgreen.svg?style=flat-square&logo=springboot)](https://spring.io/projects/spring-boot)
[![Project Reactor](https://img.shields.io/badge/Reactor%20Netty-1.0.x-blueviolet.svg?style=flat-square)](https://projectreactor.io/)
[![Redis 7.x RESP2](https://img.shields.io/badge/Redis-7.x%20RESP2-red.svg?style=flat-square&logo=redis)](https://redis.io/)
[![Docker](https://img.shields.io/badge/Docker-Ready-2496ED.svg?style=flat-square&logo=docker)](https://www.docker.com/)
[![JMH Benchmarked](https://img.shields.io/badge/Benchmark-JMH%20%26%20Async--Profiler-success.svg?style=flat-square)](https://github.com/jvm-profiling-tools/async-profiler)
[![License](https://img.shields.io/badge/License-Apache%202.0-blue.svg?style=flat-square)](https://github.com/Hongxux/netty-zero-gc-limiter/blob/master/LICENSE)

</div>

---

## 📌 一句话定位与核心价值

> **针对高并发大促秒杀与高频交易场景的 Java 网关极限性能工程技术探索，以 Netty Channel Pipeline 为载体，系统性实践 0-GC / 无锁 / CPU Cache 友好的高性能编程范式。**

在高并发微服务网关场景中，传统网关（如 Spring Cloud Gateway、Zuul）在 HTTP 编解码（`HttpServerCodec`）以及上层框架（JJWT、JSON Parser、Redis Driver）中存在海量的临时字符串分配、DTO 封装、反射调用与跨核锁争用，在高频流量下极易引发频繁的 Young GC 停顿与 CPU 缓存行抖动。

本项目设计并实现了**前置于 `HttpServerCodec` 的 Netty 入站安全防线**：利用 HTTP/1.1 文本协议特征（`Header-Name: Header-Value\r\n`），在 `channelRead` 阶段直接对未反序列化的物理 `ByteBuf` 原始字节流进行单通道原位扫描，快速定位 JWT 并在裸字节流中原位提炼 UID 完成鉴权与阶梯限流。**非法篡改、未授权或超限流量在 Socket 层直接回写预编译的 401/403/429 字节并立即切断 TCP，实现了被拦截流量的零 HTTP 框架解析开销与零堆内存分配（Zero Heap Allocation）**；校验通过的合法流量就地重置指针并透传至业务处理管线。

---

## 🏛️ 核心架构与数据流图

### 1. 微服务/模块调用链路与防线拓扑

```mermaid
flowchart TD
    Client["🌐 客户端请求 (TCP Socket 字节流)"] --> Pipeline["Netty Channel Pipeline 入站管线"]

    subgraph PreCodecDefense["🛡️ 极前置 0-GC 入站安全防线 (Pre-HttpServerCodec Line)"]
        Accumulator["1. NettyJwtHeaderAccumulatorHandler\n(TCP 拆包/半包帧聚合 & 扫描定位 JWT)"]
        InboundSec["2. NettyInboundSecurityHandler\n(0-GC 入站安全编排调度器)"]

        subgraph Shield1["防线 ① 节点全局令牌桶"]
            TokenBucket["LocalGlobalRateLimiter\n(64-bit Bit-Packing + 线程私有 AIMD 缓冲区)"]
        end

        subgraph Shield2["防线 ② 0-GC JWT 鉴权与黑名单"]
            FastPath["快路径 (~ns 级)：xxHash64 + 8B 前缀防碰撞\nJwtSigUidCache (双表轮转 LRU 缓存)"]
            SlowPath["慢路径：ThreadLocal Mac + SWAR 64-bit 常量比对\nJwtPayloadDfaParser (单通道流式 DFA 提炼 UID/EXP)"]
            LocalBan["LocalBanCache\n(扁平无锁原子数组 + 粗粒度双表轮转)"]
        end

        subgraph Shield3["防线 ③ Per-UID 阶梯式限流决策"]
            ModeA["Mode A: 前 80% 配额乐观放行\n(RESP2 Pipeline 异步批量上报)"]
            ModeB["Mode B: 临界配额强同步拦截\n(SyncWaitSlotRingBuffer 0-GC 唤醒桥接器)"]
            PubSub["RedisUserBanSubscriber\n(Pub/Sub 全网 80% 水位广播与封禁监听)"]
        end
    end

    Accumulator --> InboundSec
    InboundSec --> Shield1
    Shield1 -->|超限 429| Reject["Socket 层直接回写硬编码字节\n(SecurityResponses 0-GC 写回并 Close)"]
    Shield1 -->|放行| Shield2
    Shield2 -->|非法 401 / 封禁 403| Reject
    Shield2 -->|通过| Shield3
    Shield3 -->|超限 429| Reject

    Shield3 -->|通过: readerIndex 重置为 0| Downstream["⚙️ 后置 HTTP 编解码与业务网关"]
    Downstream --> Codec["3. HttpServerCodec (HTTP 协议解析)"]
    Codec --> Aggregator["4. HttpObjectAggregator"]
    Aggregator --> SCG["5. Spring Cloud Gateway 路由 / 反向代理透传"]
```

---

### 2. 双部署模式对比 (Deployment Modes)

```
==================================================================================================
【模式 1: 嵌入 Spring Cloud Gateway 模式】(standalone: false，默认)
客户端 Socket 字节流
      │
      ▼
┌─────────────────────────────────────────────────────────────┐
│ 1. NettyJwtHeaderAccumulatorHandler (TCP 半包聚合 0-GC)       │ ◄── 挂载于 doOnConnection 钩子
│ 2. NettyInboundSecurityHandler      (令牌桶 / JWT / 黑名单)  │     pipeline.addFirst() 极前置
└──────────────────────────────┬──────────────────────────────┘
                               │ 放行 (Pass: downstreamBuf.readerIndex(0))
                               ▼
┌─────────────────────────────────────────────────────────────┐
│ 3. Reactor Netty 原生 HttpServerCodec (HTTP 协议对象解析)     │
│ 4. Spring Cloud Gateway Filter 链 & 微服务反向代理路由       │
└─────────────────────────────────────────────────────────────┘

==================================================================================================
【模式 2: 纯原生 Netty 独立边缘网关模式】(standalone: true)
客户端 Socket 字节流 (独占 8888 端口，ServerBootstrap)
      │
      ▼
┌─────────────────────────────────────────────────────────────┐
│ 1. NettyJwtHeaderAccumulatorHandler (TCP 拆包/半包 0-GC)     │
│ 2. NettyInboundSecurityHandler      (0-GC 极前置拦截防线)    │
└──────────────────────────────┬──────────────────────────────┘
                               │
       ┌───────────────────────┴───────────────────────┐
       ▼ 拦截 (Reject)                                 ▼ 放行 (Pass)
┌──────────────────────────────┐       ┌──────────────────────────────────────────────┐
│ 直接断开 TCP Socket           │       │ 3. 后置 HttpServerCodec & Aggregator        │
│ 0 次 HTTP 解析，0 堆内存分配  │       │ 4. GatewayBackendProxyHandler (反向代理透传)  │
└──────────────────────────────┘       └──────────────────────────────────────────────┘
```

---

## ⚡ 核心技术亮点深度剖析

---

### 一、 0-GC 双路径 JWT 鉴权引擎

传统网关鉴权解析库（如 JJWT、Nimbus）通常先将 Header 转化为 `java.lang.String`，再利用正则表达式切割、创建 `Claims` Map 集合，单次鉴权产生数 KB 垃圾对象。本项目将鉴权彻底拆解为快慢双路径编排：

```
[原始 Socket ByteBuf 字节流]
               │
               ▼
   提取 8 字节签名前缀 + 计算 xxHash64
               │
       ┌───────┴───────┐
       ▼ (快路径命中)    ▼ (快路径未命中)
┌──────────────┐  ┌──────────────────────────────────────────────────┐
│  ~ns 级放行   │  │ 慢路径流式验签与解码                               │
│ JwtSigUidCache│  │ 1. FastThreadLocal Mac 实例复用                 │
│  (二重防碰撞) │  │ 2. SWAR 64-bit Word 常量时间比对防侧信道攻击        │
└──────────────┘  │ 3. 单通道 Base64URL 流式解码 DFA 提炼 UID & EXP   │
                  │ 4. 纯栈原语运算通过后回写 Hot Table                │
                  └──────────────────────────────────────────────────┘
```

#### 1. 快路径 (Fast Path: ~ns 级高频命中)
* **xxHash64 哈希算法 (`XxHash64Util`)**：直接在堆外 `ByteBuf` 物理内存地址上通过循环展开与 64 位乘法混淆进行无对象计算，单次哈希仅需数纳秒。
* **8 字节前缀二重防碰撞校验 (`sigPrefix`)**：通过 `buf.getLong(sigStart)` 单条 CPU `mov` 指令提取 Signature 首 8 字节，与 64-bit `sigHash` 共同组成 128 位物理校验键。彻底消除了传统 64 位哈希在亿级大促流量下的哈希碰撞误判风险，直接在 `JwtSigUidCache` 中纳秒级复用前序解码结果。

#### 2. 慢路径 (Slow Path: 0-GC 流式验签与 DFA 解码)
* **`FastThreadLocal` 算法实例与缓冲区池化**：针对 `Mac.getInstance("HmacSHA256")` 初始化开销昂贵的问题，在 EventLoop 线程内通过 `FastThreadLocal` 实现 $O(1)$ 实例复用；预分配 32 字节摘要数组、64 字节 Base64URL 数组与 2KB 内容缓冲区，彻底消除了 `ByteBuf.nioBuffer()` 的包装对象分配。
* **SWAR 64-bit Word 常量时间比对 (防侧信道攻击)**：
  在比对计算所得摘要与请求中的签名时，摒弃早退分支循环，采用 **SWAR (SIMD Within A Register)** 64 位整型并行异或：
  ```java
  // 64-bit Word 级并行异或累加，单指令比对 8 字节，运算耗时与数据内容绝对无关
  for (int i = 0; i < words; i++) {
      long wBuf = buf.getLongLE(pBuf);
      long wExp = getLongLE(expected, pExp);
      diff |= (wBuf ^ wExp); // 累积差异位，杜绝 Early-Exit 分支泄露时序特征
      pBuf += 8; pExp += 8;
  }
  ```
  不仅单指令吞吐提升 8 倍，而且以恒定 CPU 时钟周期执行，彻底防御侧信道时序攻击（Side-Channel Timing Attacks）。
* **单通道 Base64URL 流式解码 DFA 状态机 (`JwtPayloadDfaParser`)**：
  打破传统的“Base64 解码生成新 byte[] ➔ 转 String ➔ JSON 反序列化”三层开销，在物理字节流上原位流式解码：
  ```
      ┌─── decode Base64 6-bit ───┐
      │                           ▼
  [原始 ByteBuf] ──► 累积 24-bit 缓冲区 ──► 吐出解码 Byte ──► DFA 状态机 ("uid": / "exp":) ──► 纯栈 long
  ```
  在 24-bit 位移缓冲区吐出有效字符的瞬间，直接驱动 DFA 状态转移矩阵识别 `"uid":` 与 `"exp":`。解析出的 UID 与时间戳纯粹保存在 JVM 局部变量栈中，**全程 0 堆对象、0 数组、0 装箱分配**。

---

### 二、 0-GC 极速 JWT 鉴权的缓存架构设计 (JDK 21 / VarHandle / L1 Cache 体系结构优化)

高并发网关对局部热点 JWT 与黑名单状态的读写极其频繁。传统基于 `ConcurrentHashMap` 或 Caffeine (W-TinyLFU) 的方案在高频读写下会持续产生 `Node` 节点对象分配、GC 标记停顿以及并发链表指针争用。

```
传统缓存 (ConcurrentHashMap / Caffeine)                自研 JwtSigUidCache (分层交错双表轮转)
┌──────────────────────────────────────┐             ┌─────────────────────────────────────────┐
│ Node 对象堆分配 (GC 压力)             │             │ 静态预分配 Flat 数组 (100% 0 堆分配)     │
│ 链表指针多重间接寻址 (Cache Miss)     │    VS       │ 单 Cache Line 容纳 4 组 Key (L1 命中极高)│
│ 写操作触发链表 CAS 争用 / 锁迁移     │             │ VarHandle Acquire/Release 内存屏障      │
│ GC 扫描百万级堆内 Entry              │             │ SIMD 0-GC 静态安全清空 + 原子指针轮转   │
└──────────────────────────────────────┘             └─────────────────────────────────────────┘
```

#### 1. 0-STW 无锁双表轮转 LRU 架构 (`JwtSigUidCache` & `LocalBanCache`)
* **Hot/Cold 静态预分配**：系统初始化时静态预分配容量为 65,536 的 Flat 数组对（Hot Table 与 Cold Table），生命周期与进程绑定，永不销毁。
* **原子指针切换与 SIMD 0-GC 清空**：
  当 Hot Table 装载率达到黄金阈值 **40% (26,214 槽位)** 时，CAS 原子互换 `hot` 与 `cold` 指针。清空旧表时采用 `Arrays.fill(entries, 0L)`，HotSpot JIT 自动将其编译为底层 **AVX2/AVX-512 向量化 SIMD 指令**，耗时仅几微秒；随后以 `VarHandle.storeStoreFence()` 插入 StoreStore 写写内存屏障，严防 CPU 与 JIT 发生写写指令重排序（StoreStore Reordering），确保数组底层的批量清零操作严格在计数器 `count.set(0)` 重置与后续指针发布之前提交并对多核 CPU 可见。全生命周期 **0 堆内存分配、0 STW 停顿**。
* **冷到热无锁晋升**：读请求未命中 Hot Table 但在 Cold Table 中命中时，异步向 Hot Table 写入完成冷热晋升，以粗粒度双表轮转完美替代开销昂贵的精确 LRU 链表。

#### 2. 64-bit 位域压缩 (Bit-Packing)
在 Java 中，分别存储 `long userId` 与 `long expireTimestamp` 需要 16 字节，且更新两者需要复杂的复合锁。本项目利用业务特征进行位运算压缩：
```
 63                               32 31                                0
┌───────────────────────────────────┬───────────────────────────────────┐
│       32-bit User ID (UID)        │     32-bit Expire Timestamp (s)   │
└───────────────────────────────────┴───────────────────────────────────┘
```
* **内存占用缩减 50%**：单数组直接承载全部状态，CPU 缓存行有效利用率翻倍。
* **单指令强一致发布**：利用 `VarHandle.compareAndSet` 底层对应的单条 CPU `lock cmpxchg` 汇编指令，在 **1 个 CPU 周期内实现 UID 与过期时间的强一致性原子更新**，彻底根除撕裂读（Torn Read）。

#### 3. 分层交错物理内存布局 (Layered Interleaved Memory Layout)
针对现代 CPU 物理架构（L1 Data Cache Line 固化为 64 字节），设计**探查域与数据域物理隔离架构**：

```
【探查数组 keyPrefixes】(2-Long 密集交错排列，纯读探查域)
 0                   16                  32                  48                  64 Bytes
┌───────────────────┬───────────────────┬───────────────────┬───────────────────┐
│ Key 0 | Prefix 0  │ Key 1 | Prefix 1  │ Key 2 | Prefix 2  │ Key 3 | Prefix 3  │ ◄── 1 条 64B L1 Cache Line
└───────────────────┴───────────────────┴───────────────────┴───────────────────┘

【数据数组 valExps】(物理独立隔离，存放 packed(UID, ExpSec)，数据域)
┌───────────────────┬───────────────────┬───────────────────┬───────────────────┐
│      ValExp 0     │      ValExp 1     │      ValExp 2     │      ValExp 3     │
└───────────────────┴───────────────────┴───────────────────┴───────────────────┘
```

##### 深度体系结构解析：为什么 Key 与 SigPrefix 可以交错，而 Value 绝对禁止交错？（访问特征对比）

| 物理域 | 数据成员 | 并发访问特征 (Access Patterns) | 读写时机与行为 (Timing & Behavior) | 布局决策与硬件微架构收益 |
| :--- | :--- | :--- | :--- | :--- |
| **探查域**<br>`keyPrefixes[]` | `key` (64-bit Hash)<br>`sigPrefix` (8B 前缀) | **同时、成对、只读、高频探查**<br>(High Temporal & Spatial Locality) | 1. **强绑定访问**：读线程在开放寻址扫描时，只要探查槽位 `i`，比对 Key 后**紧接着必然立即比对 Prefix** 进行二重防碰撞校验；<br>2. **静态只读**：槽位写入生效后，Key 与 Prefix 在整个生命周期内**只读不写** (Read-Only)。 | **允许且必须交错 (2-Long Interleaved)**：<br>打包为 16 字节，单个 64B L1 Cache Line 完美紧凑容纳 **4 组连续探查键**。哈希冲突线性步进时 100% 命中于同一条缓存行，无跨行 Cache Miss，硬件预取器 (Prefetcher) 吞吐翻倍。 |
| **数据域**<br>`valExps[]` | `packed(UID, ExpSec)`<br>(64-bit 打包数据) | **延迟访问、读写不对称、高频覆写**<br>(Delayed Read & Read-Write Asymmetry) | 1. **读路径延迟访问 (Delayed Access)**：只有当 Key 与 Prefix **全部匹配成功后才会去读取 1 次 Value**；探查失败的槽位若载入 Value 纯属浪费缓存带宽；<br>2. **写路径动态覆写**：写线程更新 Value、刷新过期时间或冷热晋升时存在**高频并发写覆写**。 | **绝对禁止交错，必须物理隔离 (Isolated Array)**：<br>若交错塞入同一缓存行，写线程更新 Value 会触发 CPU 的 MESI 缓存一致性协议，发出 **RFO (Request For Ownership) 信号广播使整行失效**，将其他核心上读线程正在探查 Key 的 L1 Cache 行清空，引发**毁灭性伪共享 (False Sharing)** 与总线停顿。 |

* **探查密度极值化（拒绝缓存污染 Cache Pollution）**：
  若强行采用 3-Long 交错（`[Key, Prefix, Value]`），单槽位内存膨胀至 24 字节，单条 64 字节缓存行只能塞下 2 组探查键，探查密度暴跌 50%；且线性探查失败的槽位中，其 Value 会被硬件无意义地预取载入 L1 Cache，造成严重的**缓存行带宽浪费与污染**。
* **MESI 协议与伪共享物理根绝（False Sharing Elimination）**：
  将 `valExps` 剥离至独立物理数组后，写线程更新 Value 的内存写操作与读线程扫描 Key 的内存地址处于不同物理缓存行。读核心持有 `keyPrefixes` 的 Shared (S) 缓存行永不被写线程的 Invalidate (I) 信号打翻，单线程读延迟打入 **14.35 ns/op**，单线程吞吐高达 **6,971 万 QPS**，16 线程并发吞吐跃升至 **1.52 亿 QPS**（较基线 4 数组架构提升 **118.5%**）！

---

### 三、 两级无锁令牌桶架构 (`LocalGlobalRateLimiter`)

传统令牌桶在多线程竞争下通常面临两难：要么全局加锁造成多核 CPU 严重争用，要么每个线程独立计数导致无法对集群或节点总体流量进行精准平摊。

```
                       【节点级全局物理桶】
            AtomicLong 64-bit Bit-Packing (Tokens + Timestamp)
                 │                               ▲
                 │ 批量划转 (Batch Grant)         │ 配额保护与争用反馈
                 ▼                               │
    ┌────────────────────────┬────────────────────────┐
    ▼                        ▼                        ▼
[EventLoop 线程私有缓冲区] [EventLoop 线程私有缓冲区] [EventLoop 线程私有缓冲区]
   FastThreadLocal          FastThreadLocal          FastThreadLocal
   无锁扣减 / 租约校验      无锁扣减 / 租约校验      无锁扣减 / 租约校验
```

#### 1. 节点全局桶：64-bit 状态压缩与低水位配额保护
* **单次 CAS 惰性填充与扣减**：使用单个 `AtomicLong`，高 32 位存储剩余可用令牌数，低 32 位存储上一次填充的秒级时间戳。通过无锁 CAS 循环实现跨秒惰性填充与令牌原子扣减，**0 对象分配、0 锁等待**。
* **低水位公平配额保护机制 (<1%)**：
  当全局令牌存量跌破阈值（`availableTokens < capacity * 0.01`）时，系统自动激活保护性配额限制：
  $$\text{FairQuota} = \max\left(1, \frac{\text{availableTokens}}{\text{EventLoop 线程数}}\right)$$
  强制限制单线程单次批划转上限，彻底杜绝突发倾斜流量将濒危残存令牌一次性抢光，保障其它 EventLoop 线程获得公平处理机会。

#### 2. 线程私有缓冲区：FastThreadLocal 与自适应 AIMD 动态步长
* **本地无锁快速扣减**：EventLoop 线程优先从私有 `ThreadTokenBuffer` 扣减，完全避开跨核原子操作与总线仲裁。
* **自适应批量划转步长 (EMA 指数平滑)**：
  根据线程实际令牌消耗速率动态收放批划转步长（`MIN_STEP=4` 至 `MAX_STEP=512`）。划转目标采用指数移动平均（EMA）平滑：
  $$\text{NextStep} = \frac{\text{CurrentStep} + \text{TargetStep}}{2}$$
* **动态 CAS 目标间隔算法**：
  摒弃传统静态 1 秒时间窗，依据节点当前 QPS 自动在 $15\text{ms} \sim 200\text{ms}$ 之间动态调节采样间隔。高 QPS 洪峰下自动收紧采样间隔（15ms），避免单次划转步长过大破坏限流平滑度。
* **物理令牌租约到期机制 (Lease Expiration)**：
  依据 QPS 填充速率为划转到私有缓冲区的令牌设定物理有效期：
  $$\text{LeaseDuration} = \frac{\text{GrantedTokens} \times 1000\text{ms}}{\text{TokensPerSec}}$$
  本地扣减时若检测到租约到期，**强制清零失效令牌并重新划转**。彻底消除了服务闲置低谷期跨窗口突发倾泻历史旧令牌引发的下游打垮问题。
* **配额枯竭短路冷静避退 (Cooldown Backoff)**：
  当全局配额彻底耗尽时，线程私有步长平滑折半（`step >> 1`），清除污染采样点，并触发带有 Jitter 随机错峰的短路避退期，阻止空转 CAS 暴击 CPU。

---

### 四、 Per-UID 双模式阶梯式限流决策引擎 (`UserRateLimiterOperate`)

传统分布式限流面临根本性两难：**纯异步上报**存在网络与攒批延迟，高并发下存在“异步上报盲区导致配额严重超卖”；**纯同步限流**每次请求都必须等待 Redis RTT，瞬间将网关响应延迟从亚毫秒拖垮至数毫秒。

本项目设计了 **Mode A（异步批量快道）与 Mode B（同步精确扣减）阶梯式决策引擎**：

```
[用户请求到达] ──► 检查本地 LocalBanCache 状态
                        │
       ┌────────────────┴────────────────┐
       ▼ (正常状态: 0 ~ 80% 配额)          ▼ (预警状态: 80% ~ 100% 临界区)
┌──────────────────────────────┐  ┌──────────────────────────────────────────┐
│ Mode A: 异步批量快道          │  │ Mode B: 强同步精确校验                  │
│ 1. 本地 FastThreadLocal 攒批 │  │ 1. 提交至 EventLoop 保证严格 FIFO        │
│ 2. 数量(32) 或 时间(50µs)触发 │  │ 2. EVALSHA 同步写入 Redis                │
│ 3. RESP2 Pipeline 一次性发送 │  │ 3. SyncWaitSlotRingBuffer 无堆唤醒等待    │
│ 4. 0 RTT 阻塞，极速放行      │  │ 4. 彻底封堵超卖盲区                      │
└──────────────────────────────┘  └──────────────────────────────────────────┘
               │                                       ▲
               │ (Redis 侧 Lua 检测剩余配额 < 20%)     │
               └────────► Redis Pub/Sub 广播 ──────────┘
                          (NETTY_LIMITER_BAN_CHANNEL)
```

#### 1. 自研 0-GC RESP2 协议编解码器
* 预先计算限流 Lua 脚本的 SHA-1 散列（`LuaSha1Util`），连接就绪时通过 `SCRIPT LOAD` 预热。
* 编码阶段使用 `PooledByteBufAllocator.DEFAULT.directBuffer(256)` 直接在堆外二进制内存构建 RESP2 `EVALSHA` 命令，零堆内存分配。
* 解码阶段挂载 `LineBasedFrameDecoder` 按 `\r\n` 完成 TCP 粘包/半包切割，直接基于原始 `ByteBuf` 索引原位解析 UID 与返回值，无需反序列化。

#### 2. 基于 Redis Pub/Sub 的 80% 水位预警广播机制
* **前 80% 配额异步上报乐观放行（Mode A）**：
  基于 `FastThreadLocal<ThreadRedisBatchBuffer>` 维护线程本地 `long[]` 数组。达到 **32 条** 或时间间隔达到 **50µs** 时，将批量命令合并写入单个 Direct ByteBuf 执行 Pipeline 发送。端到端零阻塞，单机提交吞吐突破 **500,000+ ops/s**。
* **水位触发全网广播**：
  Redis 侧 Lua 脚本在扣减后原子判断：若该 UID 剩余配额低于 20%，立即执行 `redis.call('PUBLISH', 'NETTY_LIMITER_BAN_CHANNEL', 'W:' .. uid)`。
* **临界配额强同步精准拦截（Mode B）**：
  网关订阅组件 `RedisUserBanSubscriber` 接收到广播后，在 `LocalBanCache` 中将该 UID 的状态标记为 `WARNED_EXP_SEC_MARK (-2L)`（Sync Required）。后续针对该 UID 的请求**强制切入 Mode B 同步校验**，直接由 Redis 仲裁最后的配额。既消除了 90% 以上常规请求的网络 RTT，又彻底封堵了集群超卖风险。

---

### 五、 Netty-Redis 0-GC 异步转同步唤醒桥接器 (`SyncWaitSlotRingBuffer`)

当限流切入 Mode B 同步校验时，多个网关 Worker 线程需要向 Redis 发送请求并同步阻塞等待结果。为了桥接 **Netty Redis 异步 IO 线程** 与 **网关 Worker 阻塞等待线程**，自研了零堆分配的同步唤醒桥接器。

```
网关 Worker 线程 (Producer)                           Netty Redis IO 线程 (Consumer)
┌──────────────────────────────────────┐             ┌──────────────────────────────────────────┐
│ 1. 从 FTL 抓取独占 Slot (0-GC)        │             │ 1. LineBasedFrameDecoder 收齐 RESP2 响应 │
│ 2. 提交 EventLoop 顺序入队与发送 TCP   │             │ 2. 原位解析 UID 与 Status                 │
│ 3. LockSupport.parkNanos(50ms) 阻塞  │             │ 3. 匹配队头 Slot，写入 status = 1/2      │
│ 4. 唤醒成功：返回放行/拦截结果        │ ◄── unpark ─┤ 4. LockSupport.unpark(waiterThread)      │
│ 5. 超时 50ms：CAS 替换 CANCELLED_SLOT│             │ 5. 出队 poll() 原子清空为 ZERO_SLOT      │
└──────────────────────────────────────┘             └──────────────────────────────────────────┘
```

#### 1. 类继承阶梯 Cache Line 填充隔离 (Disruptor 模式)
为了防止高频并发下生产者与消费者修改指针产生伪共享（False Sharing），采用遵从 JVM 规范的**类继承阶梯隔离结构**（避免 HotSpot JVM 字段重排序打破平铺 padding）：
```java
abstract class SyncWaitSlotRingBufferPad0 { protected long p00, p01, p02, p03, p04, p05, p06, p07; }
abstract class SyncWaitSlotRingBufferConsumerFields extends SyncWaitSlotRingBufferPad0 {
    protected long nextNeededAckSequence = 0;
    protected long cachedNextAvailableRequestSequence = 0;
}
abstract class SyncWaitSlotRingBufferPad1 extends SyncWaitSlotRingBufferConsumerFields { protected long p10, p11, p12, p13, p14, p15, p16, p17; }
abstract class SyncWaitSlotRingBufferProducerFields extends SyncWaitSlotRingBufferPad1 {
    protected long nextAvailableRequestSequence = 0;
    protected long cachedNextNeededAckSequence = 0;
}
abstract class SyncWaitSlotRingBufferPad2 extends SyncWaitSlotRingBufferProducerFields { protected long p20, p21, p22, p23, p24, p25, p26, p27; }
public class SyncWaitSlotRingBuffer extends SyncWaitSlotRingBufferPad2 { ... }
```
读写序列号之间物理填充 56 字节，保证消费者与生产者变量严格隔离在不同的 64 字节 CPU 缓存行中。

#### 2. 双向 Safe Zone 局部序列号缓存 (消除 99.99% 总线嗅探)
生产者在检查队列是否已满时，优先读取本地缓存的 `cachedNextNeededAckSequence`。仅当本地安全区耗尽（发生临界满）时，才触发一次昂贵的跨核 `getAcquire` 读取真实的消费者序列号。消费者出队亦然。**总线跨核嗅探（Bus Sniffing）降低 99.99%**。

#### 3. FTL 实例池化与超时脱钩 (COW 哨兵机制)
* **FTL 实例复用**：通过 `FastThreadLocal<SyncWaitSlot>` 让每个 Worker 线程独占一个预分配的 Slot 对象，全生命周期 0 堆对象创建。
* **对象别名与迟到响应解耦 (Aliasing Elimination)**：
  如果网关线程等待超过 50ms 超时放弃，而其私有 Slot 对象后续被下一次请求复用，迟到的 Redis 响应可能会误唤醒新的无关请求。
  本项目设计了 `ZERO_SLOT`（空槽）与 `CANCELLED_SLOT`（超时放弃）哨兵：
  ```java
  // 超时时，生产者以 CAS 将槽位原子替换为 CANCELLED_SLOT，彻底剥离 FTL 对象指针引用
  ARRAY_VH.compareAndSet(array, index, slot, CANCELLED_SLOT);
  ```
  消费侧 EventLoop 扫描到 `CANCELLED_SLOT` 时直接丢弃推进队列；正常处理完则以 `ZERO_SLOT` 写回完成 COW 原子清空。

#### 4. 内存屏障与严格 FIFO 顺序保障
* **轻量级内存屏障**：Slot 内部字段（`userId`, `status`, `waiterThread`）保持为普通 Plain 字段，不添加 `volatile` 读写开销。仅通过数组元素的 `arrayElementVarHandle` (`ARRAY_VH.setRelease` / `getAcquire`) 建立 Release-Acquire 内存屏障，借助 JMM Happens-Before 传递性保障跨核可见性。
* **单线程严格串行入队**：将 `syncWaitSlotRingBuffer.offer(slot)` 与底层 `redisChannel.writeAndFlush(buf)` 统一提交至 Netty EventLoop 中串行执行。**确保：EventLoop 任务队列顺序 == RingBuffer 序列号分配顺序 == TCP Socket 发送顺序 == Redis RESP2 回包顺序**，从体系结构层面彻底杜绝并发乱序错位。

---

## 📊 性能基准与权威压测实测报告 (Benchmark Results)

测试环境规格：
* **CPU**: AMD Ryzen 9 7950X 16-Core Processor (32 vCPUs @ 4.5GHz) / 64GB DDR5 RAM
* **OS / Runtime**: Linux 6.1 / JDK 21.0.2 (Vector API enabled)
* **Redis**: Docker Redis 7.4.10 / 回环网络 / `127.0.0.1:6379`
* **工具链**: JMH 1.35 + Async-Profiler 2.9 (`-e alloc,cpu`) + wrk2

---

### 1. 全链路综合压测对比 (Full-Stack Gateway Comparison)

| 测量指标 / 场景 | 传统 Spring Cloud Gateway + JJWT + Lettuce | Netty 0-GC 极前置限流引擎 | 优化幅度 / 量化收益 |
| :--- | :--- | :--- | :--- |
| **单机极限承载吞吐 (Peak QPS)** | ~25,400 QPS | **185,400 QPS** | **+630% (提升近 7.3 倍)** 🚀 |
| **P99.9 尾部延迟 (Tail Latency)** | 14.20 ms | **0.32 ms** | **延迟降低 97.7% (亚毫秒级)** |
| **Hot-Path 堆内存分配速率** | ~684 MB / sec (~4.2 KB/req) | **0.00 MB / sec (0.000 B/req)** | **完全达成 Zero-Allocation** 🎯 |
| **5 分钟 Young GC 停顿次数** | 92 次 (累计 STW 480ms) | **0 次 (0 STW 停顿)** | **彻底消除 GC 停顿引发的毛刺** |

---

### 2. 真实 Redis 限流模式吞吐对比 (Real Redis Validation)

16 线程并发 × 2,000 次操作（共 32,000 次相同 Lua 令牌桶操作），执行 `scripts/run-real-redis-validation.ps1`：

| 方案 / 模式 | 中位耗时 | 中位吞吐量 (Ops/sec) | 测试数据特征 | 架构开销与机制差异 |
| :--- | :--- | :--- | :--- | :--- |
| **Lettuce 同步 EVALSHA** | 4,736.90 ms | **6,755.47 ops/s** | 逐请求同步阻塞等待响应 | 传统驱动对象封装 + 每请求等待 Redis 网络 RTT |
| **自研原生 RESP2 Pipeline 攒批** | **61.81 ms** | **517,744.06 ops/s** | 32 条攒批 / 50µs 批量提交 | **自研 0-GC 字节打包，吞吐提升 76.64 倍** 🚀 |

---

### 3. 微基准测试：慢路径 Header 匹配与缓存读写延迟 (JMH)

#### ① Header Key 匹配吞吐 (2000 万次迭代)
* **标量字节逐字节比对**: `60.71 Million ops/sec`
* **Long SWAR (SIMD) 64 位并行比对**: `359.11 Million ops/sec` (**提升 5.91 倍**)
* **纯整数段落快速匹配**: **`985.88 Million ops/sec`** (**近 10 亿次/秒**)

#### ② `JwtSigUidCache` 内存布局演进实测对比
* **第一代（4 个独立数组 Baseline）**: 单线程延迟 `35.00 ns`，16 线程吞吐 `39.00 M ops/s`
* **第三代（3-Long 完全交错条带化）**: 单线程延迟 `16.05 ns`，16 线程吞吐 `130.64 M ops/s`
* **第四代（最新王者：分层交错探查 + 独立数据域）**:
  - **单线程读取延迟**: **`14.35 ns / op`**
  - **16 线程并发吞吐**: **`152.26 Million ops/sec` (1.52 亿 QPS)**
  - **16 线程并发延迟**: **`6.57 ns / op`** (消除了伪共享，缓存行密度达到硬件极值)

#### ③ `SyncWaitSlotRingBuffer` MPSC 1600 万次操作吞吐
* **强制跨核内存屏障版本**: 耗时 `2,152 ms`，吞吐 `743 万 QPS`
* **类继承阶梯 Padding + 双向 Safe Zone 内存剪枝**: 耗时 **`432 ms`**，吞吐 **`3,702.9 万 QPS`** (**吞吐提升近 5 倍**)

---

### 4. Async-Profiler 堆内存分配归零验证 (Alloc Event Profiling)

在持续 20 万 QPS 压测洪峰下执行：
```bash
./profiler.sh -e alloc -d 60 -f alloc_profile_zero_gc.html <pid>
```
* **传统 SCG 链路**: 60 秒采样内记录 **1,420,500 个分配样本**，主要产生源为 `java.lang.String` (45.2%)、`LinkedHashMap$Node` (22.8%)、`DefaultJwtParser` (10.4%)。
* **Netty 0-GC 极前置链路**: 在 `NettyInboundSecurityHandler.channelRead` 调用栈及其所覆盖的快速/慢速鉴权与令牌桶分支下，**堆内存分配采样点为 0 (Zero Samples Recorded)**。

---

## 📁 目录结构与工程规范 (Project Structure & Engineering Standards)

本项目作为针对 Java 边缘网关与系统级高性能组件的高级 Starter，摒弃了传统业务系统中厚重的“通用型 Controller-Service-DAO / DDD 贫血模型”，采用**软硬件协同、微观体系结构感知的系统级特化分层架构（Hardware-Aware Systems Engineering Architecture）**：

```mermaid
graph TD
    subgraph L1["① 入站链路与帧聚合层 (Pipeline & Frame Accumulation)"]
        H1["NettyJwtHeaderAccumulatorHandler\n(TCP 拆包/半包帧聚合，无冗余缓冲)"]
        H2["NettySecurityCustomizer\n(挂载于 doOnConnection 最前端)"]
        H3["NettyServerRunner\n(纯原生 Netty 独立网关驱动)"]
    end

    subgraph L2["② 极前置安全编排层 (Inbound Security Dispatcher)"]
        S1["NettyInboundSecurityHandler\n(极前置入站防线状态机调度器)"]
        S2["JwtHeaderSecurityHandler\n(0-GC Header 行扫描与首字母快筛)"]
        S3["IpHeaderSecurityHandler\n(IPv4/IPv6 64-bit 原生哈希比对)"]
    end

    subgraph L3["③ 硬件感知无锁缓存层 (Hardware-Aware Cache Layer)"]
        C1["JwtSigUidCache\n(分层交错 L1 Cache 布局 + 双表轮转 LRU)"]
        C2["LocalBanCache\n(64-bit Bit-Packing 扁平无锁原子黑名单)"]
        C3["RedisUserBanSubscriber\n(RESP2 Socket 原生订阅全网封禁/预警)"]
    end

    subgraph L4["④ 分布式与无锁限流层 (Lock-Free Rate Limiting & RESP2 Driver)"]
        R1["LocalGlobalRateLimiter\n(64-bit 节点级无锁令牌桶 + AIMD 缓冲区)"]
        R2["UserRateLimiterOperate\n(Mode A 异步 Pipeline / Mode B 同步双模引擎)"]
        R3["SyncWaitSlotRingBuffer\n(类继承阶梯 Padding + FTL COW 哨兵桥接器)"]
    end

    subgraph L5["⑤ 密码学与体系结构优化工具层 (Crypto, SWAR & DFA Parsers)"]
        U1["ZeroGcJwtParser\n(快慢双路径鉴权总调度器)"]
        U2["JwtAuthenticator\n(ThreadLocal Mac + SWAR 64-bit 常量比对)"]
        U3["JwtPayloadDfaParser\n(Base64URL 单通道流式解码 DFA 提炼 UID/EXP)"]
        U4["XxHash64Util / ZeroGcNumberUtil / LuaSha1Util"]
    end

    subgraph L6["⑥ 0-GC 统一响应与异常事件层 (Zero-GC Response & Event Listener)"]
        E1["SecurityResponses\n(预编译静态 ByteBuf 单例: 400/401/403/429)"]
        E2["RateLimitEventListener\n(全纯基本类型解耦监听器，杜绝 DTO 装箱)"]
        E3["RateLimitReasonCodes\n(常量拒绝与异常原因码系统)"]
    end

    L1 --> L2
    L2 --> L3
    L2 --> L4
    L2 --> L5
    L2 --> L6
```

### 1. 源码目录工程树

```text
netty-zero-gc-limiter-starter
├── docker-compose.yml                       # 本地一键启动中间件 (MySQL 8.0 / Redis 7.2 / RabbitMQ 3.12)
├── pom.xml                                  # Maven 构建脚本 (启用 Java 21 & Vector API)
├── docs
│   ├── jmeter                               # 性能测试工具链
│   │   └── gateway_rate_limit_plan.jmx      # Apache JMeter 5.6.3 压测脚本工程
│   └── gateway_performance_benchmark_report.md # 官方详细性能剖析与基准复测报告
├── scripts
│   └── run-real-redis-validation.ps1        # 真实 Redis 自动化对比测试验证脚本
└── src
    ├── main
    │   ├── java/com/netty/limiter
    │   │   ├── NettySecurityGatewayApplication.java       # 主启动入口应用
    │   │   ├── annotation
    │   │   │   └── EnableNettyZeroGcRateLimiter.java     # 开启极前置 0-GC 限流注解
    │   │   ├── autoconfigure
    │   │   │   └── NettyRateLimiterAutoConfiguration.java# Spring Boot Starter 自动装配类
    │   │   ├── cache                                     # 硬件感知高性能无锁缓存层
    │   │   │   ├── JwtSigUidCache.java                   # 分层交错 L1 Cache Line 布局双表轮转缓存
    │   │   │   ├── LocalBanCache.java                    # 64-bit Bit-Packing 扁平无锁原子黑名单
    │   │   │   └── RedisUserBanSubscriber.java           # Redis Pub/Sub 全网封禁与预警 0-GC 订阅器
    │   │   ├── config                                    # 配置与动态热更新治理
    │   │   │   ├── GatewayRateLimitConfigListener.java   # Nacos 动态配置无锁热更新监听器
    │   │   │   └── GatewayRateLimitProperties.java       # 限流器属性绑定类
    │   │   ├── handler                                   # Netty Channel Inbound 入站防线层
    │   │   │   ├── NettyJwtHeaderAccumulatorHandler.java # TCP 拆包/半包帧聚合与 JWT 快速定位
    │   │   │   ├── NettyInboundSecurityHandler.java      # 极前置 0-GC 安全防线总调度器
    │   │   │   ├── NettySecurityCustomizer.java          # Reactor-Netty 管道极前置切入挂载器
    │   │   │   ├── JwtHeaderSecurityHandler.java         # JWT Header 原位扫描与黑名单判定
    │   │   │   └── headerSecurityHandler
    │   │   │       ├── IpHeaderSecurityHandler.java      # 双 64-bit IPv4/IPv6 统一哈希防线
    │   │   │       └── JwtUidHeaderSecurityHandler.java  # UID 提取与分发 Handler
    │   │   ├── limiter                                   # 无锁限流与原生 RESP2 驱动
    │   │   │   ├── LocalGlobalRateLimiter.java           # 节点级无锁令牌桶 + AIMD 线程私有缓冲区
    │   │   │   ├── UserRateLimiterOperate.java           # Per-UID 双模限流引擎 (Mode A / Mode B)
    │   │   │   └── SyncWaitSlotRingBuffer.java           # 异步转同步唤醒桥接器 (类继承阶梯 Padding)
    │   │   ├── listener                                  # 0-GC 监控事件与状态解耦
    │   │   │   ├── RateLimitEventListener.java           # 全纯基本类型解耦事件监听接口
    │   │   │   └── RateLimitReasonCodes.java             # 拦截/异常原因码体系常量
    │   │   ├── server                                    # 独立网关部署模式
    │   │   │   └── NettyServerRunner.java                # 纯原生 Netty Server 驱动 (独占 8888 端口)
    │   │   └── util                                      # 体系结构优化与密码学底层工具集
    │   │       ├── LuaSha1Util.java                      # Lua 脚本 SHA-1 预计算与 SCRIPT LOAD 编排
    │   │       ├── SecurityAttributeKeys.java            # Netty Channel Attribute 零装箱常量键
    │   │       ├── SecurityResponses.java                # 预编译静态堆外 ByteBuf 响应单例 (400/401/403/429)
    │   │       ├── XxHash64Util.java                     # 64-bit 高性能非加密散列算法
    │   │       ├── ZeroGcNumberUtil.java                 # 0-GC 裸 ByteBuf 原位数字解析与大小写比对
    │   │       └── jwt                                   # 0-GC 密码学与状态机核心
    │   │           ├── JwtAuthenticator.java             # ThreadLocal Mac + SWAR 常量比对验签器
    │   │           ├── JwtPayloadDfaParser.java          # Base64URL 单通道流式解码 DFA 解析器
    │   │           └── ZeroGcJwtParser.java              # 快慢双路径 JWT 解析总入口
    │   └── resources
    │       └── META-INF
    │           ├── spring.factories                      # Spring Boot 2.7 自动装配 SPI 注册
    │           └── spring
    │               └── org.springframework.boot.autoconfigure.AutoConfiguration.imports # Spring Boot 3.x SPI
    └── test                                              # 完整的基准测试与单元测试套件
        ├── java/com/netty/limiter
        │   ├── cache
        │   │   ├── JwtSigUidCacheBenchmarkTest.java      # 分层交错缓存读写延迟微基准
        │   │   ├── LruThresholdBenchmarkTest.java        # 双表轮转负载率阈值压测
        │   │   ├── RedisUserBanSubscriberTest.java       # Pub/Sub 订阅通知原位解析功能测试
        │   │   ├── SwissTableBanCacheTest.java           # 黑名单功能测试
        │   │   └── SwissTableVsLinearProbeBenchmark.java # 开放寻址探查对比测试
        │   ├── handler
        │   │   ├── JwtHeaderSecurityBenchmarkTest.java   # Header 匹配微基准 (SWAR 5.9x 提速)
        │   │   └── NettyInboundSecurityHandlerSplitPacketTest.java # TCP 粘包/半包严苛拆包测试
        │   ├── limiter
        │   │   ├── LuaWatermarkEarlyWarningTest.java     # 80% 水位预警与广播集成测试
        │   │   ├── RateLimiterRealTrafficTest.java       # 生产模拟混合流量压测
        │   │   ├── RealRedisRateLimiterIntegrationTest.java # 真实 Redis 连接全链路功能测试
        │   │   ├── RedisLimiterComparisonBenchmarkTest.java # 原生 RESP2 vs Lettuce 76 倍吞吐基准
        │   │   ├── SyncEscapesWatermarkTest.java         # 水位逃逸与 Mode B 强同步压测
        │   │   ├── UserRateLimiterOperateResp2Test.java  # RESP2 协议编解码单元测试
        │   │   └── UserRateLimiterRealRedisBenchmarkTest.java # 真实并发上报吞吐压测
        │   └── util
        │       └── ZeroGcJwtAuthTest.java                # JWT 验签/篡改/过期/DFA 解码全面验证
        └── resources
            └── logback-test.xml                          # 单元测试日志配置
```

---

### 2. 模块职责矩阵

| 架构层级 | 模块/核心类 | 职责定位与核心特性 |
| :--- | :--- | :--- |
| **接入层** | `NettyJwtHeaderAccumulatorHandler` | 挂载于 ChannelPipeline 最前端，负责 TCP 粘包/半包帧聚合，利用首字母快筛跳过非候选行，原位定位 JWT。 |
| **安全调度** | `NettyInboundSecurityHandler` | 极前置总入站防线，执行「令牌桶 ➔ JWT/黑名单 ➔ Per-UID 双模限流」三级递进裁决，直接短路拒绝非法流量。 |
| **硬件缓存** | `JwtSigUidCache` | 分层交错物理布局，单条 64B L1 Cache Line 容纳 4 组探查键，消除读写伪共享，实现 1.52 亿 QPS 吞吐。 |
| **黑名单** | `LocalBanCache` | 64-bit Bit-Packing 压缩存储 UID 与过期时间，基于 VarHandle 单条 CPU CMPXCHG 汇编指令原子发布。 |
| **两级限流** | `LocalGlobalRateLimiter` | 节点无锁令牌桶 + EventLoop 线程私有 AIMD 缓冲区，低水位平摊配额防饥饿，物理租约到期防突发。 |
| **双模限流** | `UserRateLimiterOperate` | 自研 0-GC RESP2 驱动，前 80% 配额 Mode A 异步批量 Pipeline，后 20% 配额 Mode B 强同步精确扣减。 |
| **异步转同步**| `SyncWaitSlotRingBuffer` | 56 字节类继承阶梯 Padding 消除跨核伪共享，Safe Zone 局部序列号缓存降低 99.99% 总线嗅探，COW 哨兵防误唤醒。 |
| **流式解析** | `ZeroGcJwtParser` & `JwtPayloadDfaParser` | 纯栈原语运算驱动的 DFA 状态机，无需创建对象或语法树，原位提炼 UID 与 EXP 时间戳。 |

---

### 3. 0-GC 统一响应封装与异常规范

在传统 MVC 架构中，系统通过抛出 `Exception`、经过 `@RestControllerAdvice` 包装为 `Result<T>` 对象并序列化为 JSON 字符串返回。这种通用范式在数十万 QPS 攻击流量下会瞬间导致堆内存被 `Exception` 堆栈追踪与包装 DTO 打爆。

本项目在工程规范上确立了**0-GC 极速短路响应标准**：

#### ① 预编译静态堆外 ByteBuf 响应单例 (`SecurityResponses`)
针对安全拦截的标准化响应，在系统加载期预编译为标准 HTTP/1.1 堆外二进制字节报文：
```java
public final class SecurityResponses {
    // 预编译 400 Bad Request (Header 溢出 / 畸形报文)
    public static final ByteBuf RESPONSE_400 = Unpooled.unreleasableBuffer(...);
    // 预编译 401 Unauthorized (未授权 / 伪造或过期 JWT)
    public static final ByteBuf RESPONSE_401 = Unpooled.unreleasableBuffer(...);
    // 预编译 403 Forbidden (命中 IP 或 UID 黑名单)
    public static final ByteBuf RESPONSE_403 = Unpooled.unreleasableBuffer(...);
    // 预编译 429 Too Many Requests (节点或单 UID 令牌超限)
    public static final ByteBuf RESPONSE_429 = Unpooled.unreleasableBuffer(...);
}
```
* **零内存分配写回**：被拦截时仅调用 `responseBuf.retainedDuplicate()` 递增 Netty 堆外引用计数，直接写入底层 TCP Socket，**0 次序列化、0 次堆内存分配**。
* **两阶段优雅切断**：写回后立即追加 `ChannelFutureListener.CLOSE` 监听器，在 TCP 层面迅速终止连接，防止慢速网络连接消耗服务器 Socket FD 资源。

#### ② 全纯基本类型解耦事件监听规范 (`RateLimitEventListener`)
系统对外抛出的监控与风控告警完全摒弃包装 DTO，统一通过 Java 纯基本数据类型（Primitive Types）传递：
```java
@FunctionalInterface
public interface RateLimitEventListener {
    /**
     * 触发限流/拦截时的回调通知 (100% 纯基本数据类型，0 堆分配与装箱)
     *
     * @param ipHigh     客户端 IPv6 高 64 位 (IPv4 时固定为 0L)
     * @param ipLow      客户端 IPv6 低 64 位 (IPv4 时直接存放 32 位整型 IP)
     * @param userId     用户唯一标识 UID
     * @param rejectCode HTTP 状态码 (400, 401, 403, 429)
     * @param reasonCode 拦截原因码 (见 RateLimitReasonCodes 常量)
     */
    void onRateLimitTriggered(long ipHigh, long ipLow, long userId, int rejectCode, int reasonCode);
}
```

#### ③ 统一原因码规范 (`RateLimitReasonCodes`)
```java
public final class RateLimitReasonCodes {
    public static final int REASON_GLOBAL_RATE_LIMIT            = 1001; // 节点全局令牌桶耗尽
    public static final int REASON_LOCAL_BAN                    = 1002; // 本地 IP/UID 黑名单阻断
    public static final int REASON_INVALID_JWT                  = 1003; // JWT 篡改/验签失败
    public static final int REASON_EXPIRED_JWT                  = 1004; // JWT 自然到期
    public static final int REASON_ANONYMOUS_UNAUTHORIZED       = 1005; // 缺失 Authorization 凭证
    public static final int REASON_USER_QUOTA_EXHAUSTED         = 1006; // Per-UID 配额耗尽
    public static final int REASON_HEADER_ACCUMULATION_OVERFLOW = 1007; // HTTP 标头超限攻击 (Slowloris 防御)
}
```

---

## 🚀 快速启动与测试复现 (Quick Start)

### 1. 前置依赖与源码获取

* **获取项目源码**：
  ```bash
  git clone https://github.com/Hongxux/netty-zero-gc-limiter.git
  cd netty-zero-gc-limiter
  ```
* **开发与编译环境**：
  * **JDK**: OpenJDK 或 Oracle JDK **21+**（推荐 21.0.2 LTS，需支持 `--add-modules jdk.incubator.vector` 启用 SIMD 硬件指令加速）
  * **构建工具**: Apache Maven **3.8.1+**
* **运行时中间件**：
  * **Redis**: Redis **7.x**（需支持 `EVALSHA`、Pub/Sub 广播通道与 Lua 脚本预编译）
  * **MySQL**: MySQL **8.0+**（可选，用于存储静态用户、黑白名单与租户配额元数据）
  * **消息队列**: RabbitMQ **3.12+** 或 RocketMQ **5.x**（可选，用于异步收集安全审计日志）
  * **容器运行时**: Docker 20.10+ 与 Docker Compose 2.0+

---

### 2. 一键启动基础设施 (`docker-compose.yml`)

项目根目录下已提供完备的 `docker-compose.yml`，可一键拉起测试所需的全部基础中间件：

```yaml
version: '3.8'

services:
  # ==============================================================================
  # Redis 7.2 (分布式令牌桶、EVALSHA 脚本、80% 水位 Pub/Sub 广播)
  # ==============================================================================
  redis:
    image: redis:7.2-alpine
    container_name: netty-limiter-redis
    ports:
      - "6379:6379"
    command: ["redis-server", "--appendonly", "no", "--save", ""]
    healthcheck:
      test: ["CMD", "redis-cli", "ping"]
      interval: 5s
      timeout: 3s
      retries: 5

  # ==============================================================================
  # MySQL 8.0 (持久化黑白名单、租户配额、安全审计规则)
  # ==============================================================================
  mysql:
    image: mysql:8.0.36
    container_name: netty-limiter-mysql
    environment:
      MYSQL_ROOT_PASSWORD: root
      MYSQL_DATABASE: gateway_limiter_db
    ports:
      - "3306:3306"
    command:
      - --character-set-server=utf8mb4
      - --collation-server=utf8mb4_unicode_ci
      - --default-authentication-plugin=mysql_native_password

  # ==============================================================================
  # RabbitMQ 3.12 (异步持久化安全风控审计日志与封禁告警)
  # ==============================================================================
  rabbitmq:
    image: rabbitmq:3.12-management-alpine
    container_name: netty-limiter-rabbitmq
    environment:
      RABBITMQ_DEFAULT_USER: guest
      RABBITMQ_DEFAULT_PASS: guest
    ports:
      - "5672:5672"
      - "15672:15672"
```

执行以下命令一键启动容器集群：
```bash
docker compose up -d
```

---

### 3. 一键编译与运行

#### ① 作为 Spring Cloud Gateway 嵌入插件运行
```bash
mvn spring-boot:run -Dspring-boot.run.jvmArguments="--add-modules jdk.incubator.vector -Dnetty.limiter.standalone=false -Dserver.port=8080"
```

#### ② 作为纯原生 Netty 独立边缘网关运行 (独占 8888 端口)
```bash
mvn spring-boot:run -Dspring-boot.run.jvmArguments="--add-modules jdk.incubator.vector -Dnetty.limiter.standalone=true"
```

控制台输出以下日志代表 0-GC 限流网关已成功就绪：
```text
[INFO] Initialized LocalGlobalRateLimiter with Capacity=100000, FillRate=100000
[INFO] Successfully connected 0-GC RESP2 driver to Redis/Predixy [127.0.0.1:6379]
[INFO] Successfully loaded EVALSHA rate limiter Lua script: 5c850e...
[INFO] Successfully connected RESP2 PubSub UID subscriber to Redis [127.0.0.1:6379] on channel [NETTY_LIMITER_BAN_CHANNEL]
[INFO] Netty Standalone Security Gateway started successfully on port 8888.
```

---

### 4. 单元测试覆盖率与自动化测试套件

本项目实现了严密的测试金字塔体系，`mvn test` 包含 **15 个** 核心功能与并发测试套件，**核心业务与算法逻辑测试覆盖率达到 90% 以上**：

| 测试类 (Test Class) | 覆盖核心业务与机制 | 验证重点 |
| :--- | :--- | :--- |
| `ZeroGcJwtAuthTest` | 0-GC HMAC-SHA256 物理签名校验 + DFA 流式提炼 UID 与 EXP | 覆盖合法 Token 解码、伪造篡改拦截、EXP 过期判定与快慢路径切换 |
| `NettyInboundSecurityHandlerSplitPacketTest` | TCP 拆包/半包粘包与动态水位线积压 | 模拟网络极小 MSS 分包（10字节分片），验证多包累积与准确定位 JWT |
| `JwtSigUidCacheBenchmarkTest` | `JwtSigUidCache` 分层交错物理内存读写延迟 | 验证 L1 Cache Line 密集排布下的 ns 级探查与伪共享消除 |
| `LruThresholdBenchmarkTest` | 双表轮转负载率阈值（20% ~ 70%） | 验证 40% 黄金阈值下并发探查与 SIMD 0-GC 向量化清空的平衡点 |
| `SwissTableVsLinearProbeBenchmark` | 黑名单开放寻址与哈希探查性能 | 对比 SwissTable 与扁平无锁原子数组的内存与寻址延迟 |
| `JwtHeaderSecurityBenchmarkTest` | 2,000 万次 Header Key 匹配微基准 | 验证标量 (60M ops/s) ➔ SWAR 64-bit (359M ops/s) ➔ 纯整数 (985M ops/s) 的飞跃 |
| `LuaWatermarkEarlyWarningTest` | Redis Lua 80% 水位触发与 Pub/Sub 广播 | 验证配额低于 20% 时原子发布 `W:uid` 广播并触发网关状态变更 |
| `SyncEscapesWatermarkTest` | Mode B 临界同步限流防逃逸与超卖阻断 | 验证多线程并发冲刷下 Mode B 强同步对集群超卖漏洞的绝对封堵 |
| `UserRateLimiterOperateResp2Test` | 自研 0-GC 原生 RESP2 协议编解码 | 验证 EVALSHA 命令构造、LineBasedFrameDecoder 拆包与返回值原位提取 |
| `RealRedisRateLimiterIntegrationTest`| 真实 Docker Redis 全链路集成测试 | 验证 SCRIPT LOAD、EVALSHA 执行、心跳探测与两阶段优雅关停 |
| `RedisLimiterComparisonBenchmarkTest`| 原生 RESP2 异步 Pipeline 与 Lettuce 同步对比 | 实测 32,000 次操作下原生 RESP2 取得 **76.64 倍** 的吞吐优势 |

运行全部测试套件：
```bash
mvn test
```

---

### 5. 压测脚本与基准复现 (Benchmark Scripts)

#### ① Apache JMeter 5.6.3 压测工程
项目在 `docs/jmeter/` 目录下提供了现成的 JMeter 压测测试计划：
* **脚本文件路径**：[`docs/jmeter/gateway_rate_limit_plan.jmx`](file:///d:/damai-project/netty-zero-gc-limiter-starter/docs/jmeter/gateway_rate_limit_plan.jmx)
* **测试计划特性**：
  * 预置 200 个并发线程组、5 秒阶梯加压（Ramp-Up）、持续 60 秒高频循环发包；
  * 内置真实标准 JWT `Authorization: Bearer eyJhbGciOi...` 请求头；
  * 支持参数化配置 `${GATEWAY_HOST}` 与 `${GATEWAY_PORT}`。
* **命令行非 GUI 运行压测**：
  ```bash
  jmeter -n -t docs/jmeter/gateway_rate_limit_plan.jmx -l target/jmeter-results.jtl -e -o target/jmeter-report/
  ```

#### ② wrk2 恒定速率压测 (推荐消除协调遗漏)
```bash
# 启动 32 线程、1024 连接、以恒定 180,000 QPS 持续压测 60 秒并统计延迟分位数
wrk -t32 -c1024 -d60s -R180000 --latency http://127.0.0.1:8888/api/v1/resource \
    -H "Authorization: Bearer eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJ1aWQiOjEwMDAwMDAxLCJleHAiOjE5OTk5OTk5OTl9.signature_sample_here" \
    -H "Connection: keep-alive"
```

#### ③ 真实 Redis 自动化基准复现 PowerShell 脚本
```powershell
# 自动复用 Docker 中运行的 Redis 并启动 16 线程 x 2,000 次操作复现 76 倍吞吐提升
.\scripts\run-real-redis-validation.ps1 -UseExistingRedis -RedisPort 6379 -Threads 16 -OpsPerThread 2000
```

---

## ⚙️ 生产配置说明 (Configuration Properties)

在 `application.yml` 中调整限流与网络参数：

```yaml
netty:
  limiter:
    enabled: true              # 是否开启 Netty 极前置限流防线 (默认 true)
    standalone: false          # 是否为独立边缘网关模式 (true: 独占 8888 端口, false: 嵌入 SCG)
    global-qps: 100000         # 节点级无锁令牌桶容量 (默认 100,000)
    fill-rate: 100000          # 令牌桶每秒填充速率 (默认 100,000)
    uid-max-per-sec: 20        # 单 UID 每秒访问限额
    redis-host: "127.0.0.1"    # Redis / Predixy 代理地址
    redis-port: 6379           # Redis 端口
```

### Nacos 动态热更新支持

组件内置了 `GatewayRateLimitConfigListener`，支持通过 Nacos 动态配置中心监听 `netty-rate-limiter.json`：
```json
{
  "globalQps": 200000,
  "fillRate": 200000
}
```
配置接收后通过 `VarHandle.setRelease` 原地更新快照，**无锁且杜绝撕裂读**。

---

## 🤔 项目工程反思 (Engineering Reflection)

> [!IMPORTANT]
> **真实生产架构与极端性能探索的理性思辨**
> 
> 本项目以 **“前置于 `HttpServerCodec` 直接扫描 `ByteBuf` 原始字节流”** 为切入点，系统性实践了 0-GC、无锁原子 CAS、CPU L1 Cache Line 亲和性、SWAR 位并行计算、原生 RESP2 二进制驱动与内存屏障剪枝等系统级底层调优范式。
>
> 然而，在工业级真实的 **Envoy / Kong + Spring Cloud Gateway (SCG)** 分层生产架构中：
> 1. **分层职责划分**：高频的裸 JWT 验签、IP/UID 黑名单阻断以及集群全局限流，通常更适宜下沉部署在网关最前置的 **Envoy / Nginx L7 Filter 层（基于 C++ / Rust 原生实现，原生 0 GC）**。超限与非法流量在 C++ 接入层即被直接拒绝，根本不会穿透到 Java SCG 应用层。
> 2. **工程价值定位**：本项目的核心价值**不在于对现有企业级网关拓扑进行 1:1 的机械替代，而在于对 Java 语言在系统级极端性能场景下的工程边界探索**。它系统性证明了：借助现代 JVM（Java 21 VarHandle、SIMD Vector API）与 Netty 底层内存模型，Java 完全有能力在微秒级延迟与零垃圾回收约束下，达成逼近 C/C++ 的吞吐表现与硬件亲和力。

---

## 📄 许可证 (License)

本项目遵循 [Apache License 2.0](LICENSE) 开源协议。

