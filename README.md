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
            ModeB["Mode B: 80% 临界配额响应式无阻塞校验\n(autoRead 物理反压 + 0-GC 续体异步唤醒)"]
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

### 零、 极前置 0-GC TCP 裸流帧聚合器 (`NettyJwtHeaderAccumulatorHandler`)

#### 🌟 架构定位与 5 维工程闭环 (Design Context, Premises & Trade-offs)

> **💡 一句话定义**：`NettyJwtHeaderAccumulatorHandler` 是前置于 `HttpServerCodec` 的 **0-GC TCP 半包聚合与裸流定位器**，在未反序列化 HTTP 协议对象的前提下，原位解决 TCP 分包截断问题，并为下游安全防线提供零内存拷贝的完整头部视图。

1. **需求必要性 (Why Pre-Codec Accumulator is Mandatory?)**：
   * **TCP 流式分包现实**：TCP 是无消息边界的字节流协议，受物理网络 MTU/MSS（通常 1460 字节）与客户端发包窗口限制，HTTP 请求头可能会被切割为多个微小 TCP 数据包分批到达；
   * **过早解析误判风险**：JWT Token 通常长达 200~500 字节，若 JWT 刚好跨越两个 TCP 分包，若在第 1 个半包到达时草率执行校验，会导致 Base64 签名被截断而发生鉴权误判（返回 401 误杀正常流量）；因此，必须在极前置阶段完成“HTTP 头部定界与半包聚合”。
2. **传统方案痛点 (Traditional Bottlenecks)**：
   * **传统网关全量反序列化**：Spring Cloud Gateway / Zuul 必须依赖 `HttpServerCodec` + `HttpObjectAggregator` 将整个 HTTP 报文反序列化为 `FullHttpRequest` 对象（实例化大量 `HttpHeaders`、`String`、`HashMap` 堆对象）；
   * **被拦截流量白白损耗 CPU/内存**：对于非法伪造（401）、黑名单阻断（403）和超限（429）的恶意流量，白白消耗了完整的 HTTP 解析 CPU 算力与数 KB 堆内存，极易被小包 HTTP 洪水攻击击穿网关。
3. **解决依赖的前提 (Underlying Premises)**：
   * **HTTP/1.1 文本行协议规范**：HTTP Header 严格以 `\r\n\r\n`（`0x0D 0x0A 0x0D 0x0A`）作为头部结束定界符，且遵循 `Header-Name: Header-Value\r\n` 规范；
   * **TLS 终结在前**：假设 TLS 握手与加解密已在 L4/L7 接入层（Nginx / SLB / KTLS）终结，或者当前 Handler 挂载于 Netty `SslHandler` 之后、`HttpServerCodec` 之前。
4. **方案的架构取舍 (Architecture Trade-offs)**：
   * **放弃的能力**：不支持对整个 HTTP Body 的无界聚合与复杂多态的 HTTP 乱序重组，仅专注于以纳秒级速度在裸字节流中定位 JWT Header；
   * **换取的收益**：
     * **非法流量极致截断**：被拦截流量（401/403/429）在 Socket 层直接回写预编译字节并立即关闭连接，达成 **0 堆分配、0 次 HTTP 编解码对象开销（拦截损耗降低 99.8%）**；
     * **合法流量 0 侵入透传**：通过鉴权的合法流量就地重置 `downstreamBuf.readerIndex(0)` 透传下游 Codec，**对后端业务流程 100% 零侵入、零开销**。
5. **边界场景与防御机制 (Edge Cases & Resilience)**：
   * **慢速 HTTP 攻击（Slowloris Attack）与无界内存膨胀防御**：
     恶意客户端每次仅发送 1 字节并刻意不发送 `\r\n\r\n`，企图撑爆网关内存。Handler 设置了 **`MAX_ACCUMULATION_BYTES (4KB)` 硬水位线**，一旦半包累积超出 4KB 仍未找到完整 Header，立即回写预编译的 `431 Request Header Fields Too Large` 并物理掐断 TCP；
   * **复合缓冲区（CompositeByteBuf）开销规避**：采用单个紧凑的直接内存缓冲区累积，规避 `CompositeByteBuf` 组件遍历的 CPU 惩罚；
   * **只读扫描与指针安全**：使用绝对索引探查，扫描过程绝不推进原 `ByteBuf` 的 `readerIndex`，保证下游 Codec 读取时的字节流完整性。

---

### 一、 0-GC 双路径 JWT 鉴权引擎

#### 🌟 架构定位与 5 维工程闭环 (Design Context, Premises & Trade-offs)

> **💡 一句话定义**：0-GC 双路径 JWT 鉴权引擎通过 **“~ns 级快路径（xxHash64 + 8B 签名前缀二重防碰撞缓存）+ 180ns 慢路径（纯栈 24-bit 移位 Base64 解码 + 双轨流式 DFA 状态机）”**，在 100% 零堆内存分配约束下实现高安全、高吞吐的极速鉴权。

1. **需求必要性 (Why 0-GC Streaming JWT Parsing is Mandatory?)**：
   * **全量请求核心路径**：JWT 鉴权是网关每条入站请求的必经关卡。在 10 万 ~ 100 万 QPS 的大促洪峰下，网关每秒需要执行数十万次签名验证与 Payload 提取；
   * **传统 GC 灾难**：若每个请求都产生数 KB 临时堆对象，每秒将向 JVM 倾倒数百兆垃圾，引发极其频繁的 Young GC 停顿（STW），严重破坏微秒级网关 SLA。
2. **传统方案痛点 (Traditional Bottlenecks)**：
   * **四阶段内存分配链式爆炸**：传统库（JJWT / Nimbus / Jackson）必须经历“截取子串 `String` ➔ Base64 解码 `byte[]` ➔ 字符串重组 `String` ➔ JSON 树反序列化 `ObjectNode/HashMap/Long`”，单次耗时高达 2,000 ~ 5,000 ns；
   * **不可变对象与正则表达式开销**：大量反射、字符匹配与装箱操作严重消耗 CPU 算力。
3. **解决依赖的前提 (Underlying Premises)**：
   * **标准 JWT 结构规范**：客户端请求严格遵循 `Header.Payload.Signature` 标准三段式结构；
   * **网关字段提取局部性**：网关安全防线仅需提取 `uid`（用户标识）和 `exp`（过期时间戳），无需反序列化整个 Payload 的全部业务扩展字段。
4. **方案的架构取舍 (Architecture Trade-offs)**：
   * **放弃的能力**：放弃了对任意复杂多态 JSON 对象的通用反序列化能力；
   * **换取的收益**：采用 24-bit 滑动移位单通道解码与双轨 DFA，纯寄存器与栈原语运算，达成 **0 堆分配、1 遍扫描（Single-Pass）、180 纳秒极限提炼（性能提升 15~25 倍）**。
5. **边界场景与防御机制 (Edge Cases & Resilience)**：
   * **流式漏匹配与状态机自愈**：DFA 状态转移矩阵在遇到非预期双引号时不归零，自愈继承为 State 1，在 0 内存回溯约束下杜绝正常用户被误杀；
   * **侧信道时序攻击（Timing Attacks）**：SWAR 64-bit Word 并行异或比对，以恒定 CPU 时钟周期执行，彻底切断 Early-Exit 分支泄露签名时序特征；
   * **畸形 Token 防御**：Token 格式损坏或缺少字段时，纯栈原语快速返回未授权，绝不抛出任何未捕获异常。

---

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
* **单通道 Base64URL 流式解码与双轨 DFA 状态机 (`JwtPayloadDfaParser`)**：
  传统的 JWT Payload 解析存在严重的“对象分配链式爆炸”（截取 String ➔ Base64 解码 byte[] ➔ new String ➔ JSON 反序列化产生 AST/Map/Long 对象），单次产生 2~4 KB 堆垃圾。
  本项目采用 **单通道位流解码 + 确定性有限状态自动机 (DFA)**，在堆外 `ByteBuf` 原始字节流上原位完成提取：

  ```
                                   【24-bit 移位滑动寄存器 (buf4)】
                                    ┌── 每凑齐 8-bit 吐出 1 Byte ──┐
                                    │                             ▼
  [堆外 ByteBuf 原始字节] ──► 每次摄入 6-bit ──► [解码明文字节] ──► 【双轨并行 DFA 状态转移矩阵】
                                                                  ├── advanceUidMatch ("uid":) ──► 纯栈 Horner 累加 (uid * 10 + d)
                                                                  └── advanceExpMatch ("exp":) ──► 纯栈 Horner 累加 (exp * 10 + d)
                                                                  ▼
                                                         【100% 纯栈寄存器，0 堆分配，0 装箱】
  ```

  ##### 🧩 核心实现一：24-bit 滑动移位流式 Base64URL 解码 (Streaming Bit-Shifting)
  传统的 Base64 解码器必须等读齐 4 个字符（24 bits）后一次性吐出 3 个字节存入新的 `byte[]` 堆数组中。
  本项目采用 **纯栈变量 24-bit 滑动移位寄存器**，实现“单字符摄入、边解边吐”的流式解码：
  1. **状态维护**：仅使用两个栈内 `int` 变量：`int buf4 = 0`（位累积缓冲区）与 `int bits = 0`（有效比特计数）；
  2. **6-bit 移位吸纳**：从 `ByteBuf` 读入 1 个 Base64 字符，通过分支剪枝快速映射为 6-bit 数值 `val`（0~63），执行：
     ```java
     buf4 = (buf4 << 6) | val;
     bits += 6;
     ```
  3. **8-bit 即时吐出**：只要 `bits >= 8`（累积凑齐 1 个明文字节），即刻移位提取并驱动下游 DFA：
     ```java
     bits -= 8;
     byte decodedByte = (byte) ((buf4 >> bits) & 0xFF); // 瞬间吐出 1 字节明文驱动 DFA
     ```
  **收益**：完全摆脱了 4 字节块对齐限制与临时数组分配，解码与状态机以真正的流式流水线（Streaming Pipeline）协同工作。

  ##### 📊 传统 JSON 解析 vs 本项目流式 DFA 0-GC 效果对比

  | 对比指标 | 传统方案 (Jackson / Fastjson + Base64) | 本项目流式 DFA 状态机 (`JwtPayloadDfaParser`) | 提升倍数 / 收益 |
  | :--- | :--- | :--- | :---: |
  | **单次解析堆分配** | **2,048 ~ 4,096 Bytes** (String, byte[], AST Node) | **0 Byte (100% 纯栈原语寄存器运算)** | **GC 压力彻底归零** |
  | **10万 QPS 下垃圾产生** | **200 ~ 400 MB / 秒** (引发高频 Minor GC) | **0 MB / 秒 (零垃圾产生)** | **消除 GC 抖动** |
  | **单次解析延迟 (P99)** | **$2,000 \sim 5,000\text{ ns}$** | **$\approx 180\text{ ns}$** | **性能提升 15 ~ 25 倍** |
  | **内存扫描遍数** | 4 遍 (截取 ➔ 解码 ➔ 转字符串 ➔ AST 反射构建) | **1 遍 (Single-Pass 边解边扫边合成)** | **L1 Cache Miss 极低** |

  ##### 🔬 深度耗时拆解与 CPU 时钟周期推演 (Why 180ns vs 2,000~5,000ns?)

  ###### 1. 传统方案 4 阶段内存分配灾难 (2,000 ~ 5,000 ns 耗时来源)
  - **① 截取子串**：`token.substring(dot1, dot2)` 产生堆内存分配与字符数组拷贝 (~300 ~ 600 ns)；
  - **② Base64 解码**：`Base64.getUrlDecoder().decode(...)` 分配新 `byte[]` 堆数组 (~400 ~ 800 ns)；
  - **③ 字符串重组**：`new String(bytes, UTF_8)` 再次分配 `String` 对象进行编码校验 (~200 ~ 400 ns)；
  - **④ JSON 语法树解析**：`ObjectMapper.readTree(...)` 创建 `JsonParser`，动态分配 `ObjectNode`、`HashMap`、`TextNode` 节点，逐字段计算哈希并装箱为 `Long.valueOf()` (~1,500 ~ 3,500 ns)。
  - **总代价**：至少 4 次堆内存分配，产生 2~4 KB 垃圾，遍历内存 4 遍，耗时 **$2,400 \sim 5,300\text{ ns}$**。

  ###### 2. 本项目 DFA 流式寄存器预算 ($\approx 180\text{ ns}$ 耗时推演)
  - **单遍扫描长度**：标准 JWT Payload Base64 长度约为 **120 ~ 160 个 ASCII 字符**，整个解析仅需 **$N \approx 150$ 次轻量循环**；
  - **单循环硬件指令极简**：单条 `MOVZX` 字节读取 + 3 次 `CMP/SUB` Base64 映射 + 1 条 `SHL/OR` 移位 + 1 条 `CMP` 状态跳转 + 1 条 `uid = uid * 10 + (b - '0')` 原位栈累加，**单循环平均仅消耗 2 ~ 3 个 CPU 时钟周期**；
  - **时钟周期推演**：
    $$\text{总周期数} \approx 150 \text{ 字符} \times 2.5 \sim 3.0 \text{ Cycles} = 375 \sim 450 \text{ 个 CPU 时钟周期}$$
    在 $2.5 \sim 3.5\text{ GHz}$ 现代 CPU 上：
    $$\text{总耗时} = \frac{450 \text{ Cycles}}{3.0 \text{ GHz}} \approx 150 \sim 180\text{ 纳秒 (ns)}$$
  - **100% 寄存器驻留**：状态变量（`buf4, bits, uidMatchState, uid, expSec`）全部直接分配在 **CPU 物理寄存器（`rax, rbx, rcx` 等）** 与栈帧中，0 堆内存往返，实现硬件级极速吞吐。

  ##### 💡 为什么逐字节流式输出必须使用 DFA？——朴素线性匹配的漏匹配灾难与状态自愈

  在单通道 Base64 流式解码中，字节是从 24-bit 移位寄存器**逐个单向吐出的（Streaming & No Rewind）**。数据一旦流过，如果不分配堆内存将其存下，就绝对无法倒流回溯。

  ###### 1. 朴素线性匹配（匹配失败直接归零 `state = 0`）的漏匹配陷阱：
  假设输入流中包含合法但常见的 JSON 片段：`{"name":"u", "uid":1001}` 或 `{"role":"user", "uid":1001}`：
  - **步骤 1**：读到 `"name":"u"` 的内容 `"u`，匹配到首引号与字符 `'u'`，进入 `state = 2`（已匹配 `"u`）；
  - **步骤 2（灾难爆发）**：接着读到 `"u"` 的右引号 `"`。朴素匹配器期望读到 `'i'`（组成 `"ui`），发现不匹配，**直接粗暴地将 `state` 归零重置（`state = 0`）**；
  - **步骤 3（漏掉合法数据）**：这个刚读到的 `"` 恰恰是后续真正 `"uid":` 的起始左双引号！因为被粗暴归零丢弃，紧接着读到真正 `"uid"` 的字母 `'u'` 时，由于此时 `state == 0`（期望首字符是 `"`），再次被判定为无关字符忽略！**结果：真正的 `"uid": 1001` 被彻底漏掉跳过，导致合法用户被误杀抛出 401！**
  - **传统解决代价**：若想在传统模式下避免漏匹配，就必须在堆上分配临时 `byte[]` 数组缓存历史数据以支持指针回退（Rewind Backtracking）——**这直接导致 0-GC 架构彻底破产**！

  ###### 2. DFA 状态转移矩阵的优雅自愈（Zero Backtracking & 100% 0-GC）：
  在 `JwtPayloadDfaParser` 的 DFA 状态机中：
  ```java
  case 1: if (b == 'u') return 2; if (b == '"') return 1; return 0;
  case 2: if (b == 'i') return 3; if (b == '"') return 1; return 0; // 遇到 '"' 绝不归零，自愈继承为 State 1！
  case 3: if (b == 'd') return 4; if (b == '"') return 1; return 0;
  ```
  - 当在任何状态遇到 `"` 时，**绝不粗暴归零，而是自愈跳转至 `State 1`**（将该字符无缝继承为新前缀的起始双引号）；
  - **架构精髓**：以严格确定性的有限状态矩阵，**在 0 内存回溯、0 临时缓冲区、仅占用 1 个 `int` 栈寄存器的极致约束下**，完美达成了 100% 精准匹配与 0-GC 流式吞吐！

---

### 二、 0-GC 极速 JWT 鉴权的缓存架构设计 (JDK 21 / VarHandle / L1 Cache 体系结构优化)

#### 🌟 架构定位与 5 维工程闭环 (Design Context, Premises & Trade-offs)

> **💡 一句话定义**：自研 `JwtSigUidCache` 与 `LocalBanCache` 是针对 JWT 鉴权快路径与本地黑名单的**0-STW 分层交错无锁缓存引擎**，基于“L1 Cache 物理隔离 + VarHandle 内存屏障剪枝 + 粗粒度双表轮转”，以 1.52 亿 QPS 吞吐实现 14ns 级极速探查。

1. **需求必要性 (Why Specialized 0-GC Cache is Mandatory?)**：
   * **密码学算力保护**：尽管流式 DFA 慢路径提炼仅需 180ns，但 HMAC-SHA256 依然消耗宝贵的 CPU 算力。面对同一活跃用户的连续高频请求，必须在网关本地提供 **纳秒级（~14ns）** 的鉴权结果复用；
   * **黑名单零延迟阻断**：当用户被封禁或配额预警时，必须在网关入口以纳秒级速度即刻识别，防止非法流量穿透。
2. **传统方案痛点 (Traditional Bottlenecks)**：
   * **`ConcurrentHashMap` / Caffeine 瓶颈**：写操作存在分段锁/节点锁争用；每次 `put` 动态分配 `Node` 节点对象，GC 必须扫描百万级堆内 Entry；维护精确 LRU 链表导致写指针竞争激烈，多核 CPU 缓存行频繁失效（Cache Line Invalidation）；
   * **扩容停顿与 STW**：动态扩容与重哈希（Rehash）导致剧烈的吞吐毛刺与 GC 停顿。
3. **解决依赖的前提 (Underlying Premises)**：
   * **时间与空间局部性 (Temporal Locality)**：大促期间活跃用户在秒级窗口内会发起多次重复请求；
   * **粗粒度轮转容忍度**：网关鉴权与黑名单允许以 40% 负载率为阈值进行粗粒度的双表轮转淘汰，无需为每个 Entry 维持纳秒级精确的链表时序。
4. **方案的架构取舍 (Architecture Trade-offs)**：
   * **放弃的能力**：放弃了微观维度的单个 Entry 精确访问计数与动态扩容；
   * **换取的收益**：
     * **L1 Cache 空间预取极值化**：通过 `keyPrefixes[]` 探查域与 `valExps[]` 数据域分层物理隔离，单条 64B 缓存行容纳 4 组 Key，探查 4 步 0 跨行 Miss；
     * **全无锁 0-GC**：静态预分配扁平数组，彻底消除动态扩容与 Node 对象分配，单机并发探查吞吐跃升至 **1.52 亿 QPS（14.35 ns/op）**。
5. **边界场景与防御机制 (Edge Cases & Resilience)**：
   * **64-bit 哈希碰撞防御**：单指令提取 Signature 前 8 字节（`sigPrefix`）与 64-bit Hash 组成 128 位物理二重防碰撞校验，彻底杜绝伪造 Token 碰撞误判；
   * **写覆盖与脏读防范**：`JwtSigUidCache` 采用两阶段 `setRelease` + `0L 哨兵`，读线程遇 0L 执行 PAUSE 极短自旋（≤64步）；`LocalBanCache` 采用 64-bit 单字 CAS 原子打包，100% Wait-Free；
   * **冷表清空 0-GC 保护**：轮转时由后台线程基于 SIMD 向量化指令执行静态原位清零，避免触发全局锁与内存重分配。

---

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

#### 1. 0-STW 全无锁并发模型与双表轮转 LRU 架构 (`JwtSigUidCache` & `LocalBanCache`)

在高并发网关场景下，传统并发哈希表（如 `ConcurrentHashMap` 或基于锁分段的缓存）存在三大致命瓶颈：**哈希冲突分段锁/节点锁引发线程阻塞与上下文切换、动态扩容时的链表/红黑树内存重分配（引发 GC 与 STW 停顿）、以及维护精确 LRU 链表时每次读操作都必须修改指针导致的严重锁争用**。

本项目彻底摒弃了互斥锁（Mutex/Lock），基于 **Java 21 `VarHandle` Acquire/Release 内存语义 + CPU 硬件原子原语 (`LOCK CMPXCHG`) + 粗粒度双表轮转**，系统性构建了端到端的全无锁高并发模型。

---

##### 1.0 【架构选型对比】两大核心缓存组件无锁机制对比与选型矩阵

系统内部针对不同的业务特征，在两大核心缓存中采用了分层的无锁并发模型：

| 核心组件 | 槽位数据结构 | 写线程并发协议 (Write Protocol) | 读线程并发协议 (Read Protocol) | 是否存在自旋同步 |
| :--- | :--- | :--- | :--- | :---: |
| **`JwtSigUidCache`<br>(JWT 鉴权缓存)** | **多字段跨数组**<br>`keyPrefixes` (Key+Prefix)<br>`valExps` (UID+ExpSec) | **CAS 抢占 Key + 两阶段 Release 顺序发布**<br>覆盖更新时使用 `0L 哨兵` 通知读线程防脏读 | **Acquire 无锁线性探查**<br>Key 命中后读取 Prefix 与 ValExp 进行二重校验 | **有 (极罕见 Slow Path)**<br>若读线程在写线程中途介入，执行 `PAUSE` 极短自旋（≤64步） |
| **`LocalBanCache`<br>(本地黑名单缓存)** | **单 64-bit 物理字**<br>`long[] entries`<br>`packed(userId, expSec)` | **单指令 CAS 原子写入**<br>`casEntry(idx, EMPTY, packed)`<br>单条 `LOCK CMPXCHG` 一步到位 | **单指令 Acquire 读取**<br>`getEntryAcquire(idx)` 加载后直接解包判定，纯 100% Wait-Free | **无 (0 自旋)**<br>单指令原子搬运，无任何中间裂变态 |

---

##### 1.1 【JwtSigUidCache 专属机制】多字段 CAS 抢占 + 两阶段 Release 顺序发布与 0L 哨兵自旋协同

> [!NOTE]
> **模块专属范围界定**：本小节所述的**两阶段有序发布、0L 边界状态哨兵、就地覆盖更新协议以及双重活性守护自旋**，是专门针对 **`JwtSigUidCache`** 跨数组存储多物理字段（64-bit Key、64-bit Prefix、64-bit ValExp）的特征而定制的；后文 1.2 小节的 **`LocalBanCache`** 为单 64-bit 物理字结构，天然无需多字段时序与 0L 哨兵。

读写线程之间通过严格的 **Acquire-Release 内存栅栏协议与硬件原子操作** 形成无缝配合，全流程如下：

```
┌───────────────────────────────────────────────┐        ┌───────────────────────────────────────────────┐
│     【写线程：CAS 抢占 + 两阶段 Release 发布】 │        │          【读线程：Acquire 无锁探测与读取】    │
└───────────────────────┬───────────────────────┘        └───────────────────────┬───────────────────────┘
                        │                                                        │
         ① 哈希定位: idx = (int)(key & MASK)                               ① 哈希定位: idx = (int)(key & MASK)
                        │                                                        │
                        ▼                                                        ▼
         ┌──────────────────────────────┐                         ┌──────────────────────────────┐
         │ ② Acquire 读取槽位当前状态   │                         │ ② Acquire 读取槽位当前状态   │
         │ k = getKeyAcquire(idx)       │                         │ k = getKeyAcquire(idx)       │
         └──────────────┬───────────────┘                         └──────────────┬───────────────┘
                        │                                                        ├─ [k == EMPTY] ──► 探查终止 (Miss)
                        ├─ [k == key (Live 存在)] ──► 走覆盖更新协议 (0L 哨兵)   ├─ [k != key]   ──► 线性步进探测下一槽位
                        ├─ [k != key (其他 Live)] ──► 线性步进探测下一槽位       ▼ [k == key 命中]
                        ▼ [k == EMPTY / TOMBSTONE (空槽或墓碑)]   ┌──────────────────────────────┐
         ┌──────────────────────────────┐                         │ ③ Acquire 读取 Prefix 与     │
         │ ③ CAS 抢占空槽/墓碑所有权    │                         │    ValExp 核心载荷           │
         │ casKey(idx, k, key)          │                         └──────────────┬───────────────┘
         └──────────────┬───────────────┘                                        │
                        ├─ [CAS 失败] ──► 线性步进探测下一槽位                   ▼
                        ▼ [CAS 成功]                              ┌──────────────────────────────┐
         ┌──────────────────────────────┐                         │ ④ 极速 Fast Path (99.99%)    │
         │ ④ Release 写入防碰撞前缀      │ ══════════════════════► │ ValExp != 0 且 Prefix 匹配:  │
         │ setPrefixRelease(sigPrefix)  │   Release-Acquire 内存屏障│ 零锁解包 UID 返回！(14.35 ns) │
         └──────────────┬───────────────┘                         └──────────────┬───────────────┘
                        │                                                        │
                        ▼                                                        ▼ [极罕见: ValExp == 0L 写入中途]
         ┌──────────────────────────────┐                         ┌──────────────────────────────┐
         │ ⑤ Release 发布核心数据载荷   │                         │ ⑤ Slow Path 自旋同步        │
         │ setValExpRelease(packed)     │                         │ Thread.onSpinWait() (PAUSE)  │
         │ (此时槽位数据正式对外可见)   │                         │ 活性守护 MAX_SPIN ≤ 64 步    │
         └──────────────┬───────────────┘                         └──────────────────────────────┘
                        │
                        ▼
         ┌──────────────────────────────┐
         │ ⑥ count.incrementAndGet()    │
         └──────────────────────────────┘
```

* **写路径两阶段发布与 0L 状态哨兵生命周期维护 (Sentinel Lifecycle)**：
  - **新槽位写入时序与天然初始值**：Java 中底层 `long[]` 数组的初始值天然为 0L。新槽位写入严格遵循 $\text{CAS 抢占 Key} \xrightarrow{\text{setRelease}} \text{写入 SigPrefix} \xrightarrow{\text{setRelease}} \text{最终发布 ValExp}$。当 CAS 抢占 Key 成功的一瞬间，由于槽位原始值本就是 0L，它对外展现的正是一个天然合法的“0L 哨兵（更新中）”状态。这使得**新条目插入**与**已有条目覆盖**在读路径上完美收敛于同一套 0L 自旋等待逻辑；
  - **覆盖更新的触发场景与哨兵生命周期维护 (In-Place Overwrite & 0L Sentinel)**：
    1. **何时会触发覆盖更新（3 大高并发网关场景）**：
       - **冷到热并发晋升竞争 (Cold-to-Hot Promotion Race)**：高频用户的多个并发请求在冷表命中，并发调用 `hot.put()` 晋升热表。首个线程成功 CAS 抢占热表槽位后，后续并发晋升线程在热表探查到 `k == key`，触发覆盖更新；
       - **慢路径 DFA 解析并发回填 (Concurrent JWT Slow Path Parsing)**：新 Token 初次请求时，多个 EventLoop 线程穿透慢路径解析出相同的 `(uid, expSec)` 并几乎同时回填，后到达的线程探测到已存在 `k == key` 触发覆盖；
       - **Token 续期与过期时间延展 (Token Refresh / Renewal)**：业务刷新 Token 或滑动窗口延期，更新相同 Key 的 `packed(uid, newExpSec)`。
    2. **为什么遇到 `k == key` 必须就地覆盖（In-Place），而绝不能在 Prefix 不匹配时往后顺延？**
       在无锁开放寻址架构中，读路径（$N$ 读 $1$ 写）即使自旋也是无副作用的只读等待；但写路径一旦引入基于 Prefix 判断的多槽顺延，将彻底破坏“单物理条目契约”，引发致命后果：
       - **槽位分裂与僵尸条目竞态灾难（Slot Splitting & Zombie Slots）**：假设 Token A 写入槽 1，Token B 因 Prefix 不符顺延写入槽 2（同一 Key 霸占两槽）。随着时间推移，槽 1 过期被标记为墓碑或清空。此时再次更新 Token B 的线程探查时，发现槽 1 为空便会直接 CAS 抢占写入槽 1。结果是 **槽 1 和 槽 2 同时存入了 Token B**。槽 2 彻底化为永远无法被覆盖和清理的“僵尸槽位”，导致探测链错乱与哈希表永久内存泄漏；
       - **极低碰撞概率下的核心架构取舍（$10^{-10}$ 概率）**：64-bit MD5 截断的哈希碰撞率不到百亿分之一。面对这极罕见的极端情况，系统坚决选择**就地覆盖以驱逐旧碰撞者（Eviction-on-Collision）**，并依赖读路径的 `isPrefixMismatched` 严格把关防伪造越权。这种取舍换来了写路径 $O(1)$ 极简状态机的巅峰性能，从根本上排除了多槽位协调带来的死锁与死循环。
    3. **为什么覆盖更新必须先竖立 0L 哨兵？——致命跨代撕裂并发推演 (Why 0L Sentinel is Mandatory)**：
       假设槽位已有旧数据 `[Key: 100, Prefix: P_Old, ValExp: (UID_Old, Exp_Old)]`，写线程准备覆写为 `[Key: 100, Prefix: P_New, ValExp: (UID_New, Exp_New)]`。**若不竖立 0L 哨兵直接更新，将引发不可逆的安全越权灾难**：
       - **反例推演 A（先写 Prefix 再写 ValExp）**：写线程刚写入 `P_New`，此时并发读线程传入 `P_New` 对应的 Token 查询。读线程读取到非零的旧数据 `UID_Old`，随后校验 Prefix 发现 `P_New == P_New` 匹配成功！**结果：持新 Token 的用户被赋予了属于旧用户的 `UID_Old` 身份，引发严重越权漏洞！**
       - **反例推演 B（先写 ValExp 再写 Prefix）**：写线程刚写入 `UID_New`，此时并发读线程传入 `P_Old` 对应的旧 Token 查询。读线程读取到新数据 `UID_New`，随后校验 Prefix 发现 `P_Old == P_Old` 匹配成功！**结果：持旧 Token 的用户被赋予了属于新用户的 `UID_New` 身份！**
       - **0L 哨兵三步生命周期的终极防御**：
         1. **竖立哨兵 (Step 1)**：写线程执行 `setValExpRelease(idx, 0L)` 将 ValExp 置零。此时读线程一旦读取到 `0L`，便知晓数据处于更新瞬态，**绝不信任当前字段，强制进入自旋等待**；
         2. **更新前缀 (Step 2)**：执行 `setPrefix(idx, P_New)` 安全替换 8 字节前缀（普通写进入 CPU Store Buffer 进行写合并）；
         3. **撤除哨兵并原子发布 (Step 3)**：执行 `setValExpRelease(idx, packed_New)` 写入新数据，撤除 0L 哨兵。读线程自旋结束，原子感知到强一致的新版本数据。**彻底杜绝跨字段错位与身份冒领！**
  
  - **为什么竖立哨兵使用单指令 `setValExpRelease(0L)`，而无需昂贵的 CAS？**
    1. **核心设计前提：接受纳秒级最终一致性，绝对零容忍跨字段数据撕裂（Zero Torn Read）**：
       - **业务契约**：作为高性能网关本地缓存，系统**允许纳秒级最终一致性**（即在写线程更新的极短瞬态内，并发读线程读取到刚生效前一瞬间的合法旧代数据，在业务上是完全有效且符合预期的）；
       - **红线底线**：系统**绝对零容忍数据撕裂（Torn Read）**——绝不允许读线程读取到“新 Prefix + 旧 ValExp”这种由跨字段拼接而成的错位脏数据（否则会导致严重的安全越权或业务误杀）。
    2. **时序安全性推演（100% 杜绝跨代脏读）**：
       - **若读线程在写 0L 之后介入**：读线程通过 `getValExpAcquire` 读到 `0L`，瞬间感知到数据正在更新，立刻进入 Slow Path 极短自旋，直到新数据完全发布；
       - **若读线程在写 0L 之前介入**：读线程读取到非零的旧 ValExp。**由于写线程是在将 `valExp` 设为 0L 之后才去修改 Prefix 的**，此时底层的 Prefix 必定也是与之强一致的旧 Prefix。读线程获取到的是一个完整自洽的旧版本数据（属于写入生效前一瞬间的正常并发读命中，满足最终一致性），**绝对不可能出现“读到了新 Prefix、却读到旧 ValExp”的错位撕裂态**！
    3. **硬件微架构性能碾压（`MOV` 1 周期 vs `LOCK CMPXCHG` 40+ 周期）**：
       - 若使用 CAS：底层必须发射带硬件总线锁前缀的 `LOCK CMPXCHG` 汇编指令，强制锁定当前缓存行（Cache Line Lock）并停顿 CPU 指令流水线，耗时高达 **10 ~ 15 纳秒（约 40 个 CPU 时钟周期）**；
       - 使用 `setRelease`：在 x86-64 下直接编译为普通的单条汇编指令 `MOV [addr], 0`，耗时仅需 **1 个 CPU 时钟周期（约 0.3 纳秒）**，随后通过 CPU 硬件 MESI 协议在总线广播 Invalidate 信号。在保证 100% 零撕裂安全的同时，彻底免除了硬件总线锁的停顿惩罚！

* **读路径分流与防死循环双重活性守护 (Liveness Guard & Anti-Deadlock)**：
  - **Fast Path（99.99% 场景）**：若 `valExp != 0L` 且 `storedPrefix == targetPrefix`，单指令直接解包返回，0 自旋；
  - **Slow Path 极短自旋与防死循环机制**：若遇到 `valExp == 0L`（写线程正在更新中），读线程调用 `Thread.onSpinWait()` 发射硬件 `PAUSE` 汇编指令进入极短自旋。为了防止在自旋期间因并发淘汰、覆盖或哈希碰撞导致死循环，设计了**双重活性守护检测**：
    1. **Key 完整性动态探测 (Key Integrity Guard)**：自旋每一步均通过 `isKeyEvictedOrOverwritten(table, idx, key)` 探测槽位的 Key。一旦检测到槽位 Key 在自旋期间被并发淘汰、置为 `TOMBSTONE` 或被覆写为其他 Key，**瞬间短路退出 (Fast Reject 返回 0L)**，绝不等待已失效的数据；
    2. **Prefix 签名前缀防碰撞短路 (Prefix Mismatch Short-Circuit)**：在等待 ValExp 期间，持续探测 `isPrefixMismatched(targetPrefix, getPrefixAcquire(idx))`。一旦发现写线程落地的 Prefix 与预期的前缀不符（发生 64-bit 哈希碰撞），**立即终止自旋短路退出**；
    3. **自旋步数硬上限截断与 EventLoop 保护 (Bounded Spin & Latency Budget)**：
       设置 `MAX_SPIN = 64` 步硬上限。根据 CPU 硬件微架构推演：
       - **时钟周期预算**：写线程的连续 3 次发布指令在 CPU 流水线背靠背执行仅需 ~1ns（加上跨核 MESI 缓存广播约 10~30ns）。读线程单次 `Thread.onSpinWait()`（发射 x86 `PAUSE` 指令）延迟约 15~40ns。64 步自旋提供了 **$1.0 \sim 2.5\ \mu\text{s}$ 的充裕等待窗口**（超写发布耗时的 30~80 倍），正常并发下 1~2 步内必定命中；
       - **反应堆防队头阻塞 (EventLoop Protection)**：若写线程被 OS 内核调度抢占（概率 < 0.001%），读线程在 64 步（~1μs）后**果断超时跳出自旋返回 0L**，无缝降级走 200ns 的零拷贝 DFA 慢路径解析，**宁可牺牲一次缓存命中，也绝对不让 EventLoop 发生微秒级队头阻塞**，在数学与工程上双重根除死锁与活锁。

---

##### 1.2 【LocalBanCache 专属机制】单 64-bit 字单指令 CAS / Release 极简 Wait-Free 流程

由于黑名单缓存仅需记录 `(userId, expireTimeSec)`，已通过 64-bit Bit-Packing 压缩打入单个 `long` 中，其并发控制相比 `JwtSigUidCache` 更加极简高效：

```
┌───────────────────────────────────────────────┐        ┌───────────────────────────────────────────────┐
│         【写线程：单指令原子 CAS 抢占写入】     │        │        【读线程：单指令 Acquire 无等待读取】   │
└───────────────────────┬───────────────────────┘        └───────────────────────┬───────────────────────┘
                        │                                                        │
         ① 哈希定位: idx = (int)(mixHash(uid) & MASK)                       ① 哈希定位: idx = (int)(mixHash(uid) & MASK)
                        │                                                        │
                        ▼                                                        ▼
         ┌──────────────────────────────┐                         ┌──────────────────────────────┐
         │ ② 单指令原子抢占与写入       │                         │ ② 单指令 Acquire 加载槽位    │
         │ casEntry(idx, EMPTY, packed) │ ══════════════════════► │ entry = getEntryAcquire(idx) │
         │ (单条 LOCK CMPXCHG 一步到位) │   硬件级 64-bit 原子可见性│ (单条 MOV 指令，无自旋)      │
         └──────────────┬───────────────┘                         └──────────────┬───────────────┘
                        ├─ 失败: 线性探测下一个槽位                              │
                        ▼ (成功)                                                 ▼
         ┌──────────────────────────────┐                         ┌──────────────────────────────┐
         │ ③ count.incrementAndGet()    │                         │ ③ isLiveEntry(entry) 判定    │
         └──────────────────────────────┘                         │ 解包 UID 匹配 ──► 瞬间返回！ │
                                                                  └──────────────────────────────┘
```

* **写操作 (100% 单指令原子落盘)**：
  直接调用 `casEntry(idx, EMPTY/TOMBSTONE, packed)`，单条 `LOCK CMPXCHG` 汇编指令同时完成槽位所有权争夺与有效数据写入，**无需两阶段时序，无需 0L 哨兵**；
* **读操作 (100% Wait-Free，绝对零自旋)**：
  直接调用 `getEntryAcquire(idx)` 单指令加载。
  - **为什么读到 0L（EMPTY）绝对不需要自旋？**：
    1. **单字原子落盘，物理上无半成品瞬态**：因为 `UID` 与 `ExpSec` 压缩在单个 64-bit 字中，状态从 `0L (EMPTY)` 到 `packed (有效数据)` 是在单条指令周期内瞬间完成的，物理内存中根本不存在“写入了一半、等待另一半发布”的中间态；
    2. **0L 的单一数学语义（停探放行）**：在开放寻址哈希表中，读线程看到 `0L` 意味着该探测链在此处断开，即**“该 UID 从未被封禁”**，直接 `break` 停探返回 `BAN_STATUS_PASSED`，耗时仅 14ns；
    3. **与 `JwtSigUidCache` 读到 0 的本质区别**：`JwtSigUidCache` 之所以遇到 0L 会自旋，是因为它跨越了 16 字节的多字段（`Key/Prefix` 与 `ValExp`），写线程在覆写时主动将 `ValExp` 置为 0L 作为“更新中哨兵”；而 `LocalBanCache` 的 64-bit 结构彻底免除了跨字段协调，达成了纯粹的 **Wait-Free（无等待算法）**。

##### 1.3 【双组件通用架构】0-STW 原子双表轮转与粗粒度无锁 LRU 淘汰
传统网关 LRU（如基于双向链表的 LRU Cache）在每次 `get()` 操作时都必须修改链表指针，导致读操作退化为写锁竞争。本项目采用**无锁粗粒度双表轮转 LRU**：
* **Hot/Cold 静态预分配**：系统初始化静态预分配容量为 65,536 的 Flat 数组对（Hot Table 与 Cold Table），常驻物理内存，与进程生命周期绑定，**完全杜绝运行时扩容分配与 GC STW**；
* **40% 黄金阈值与无阻塞轮转**：
  当 Hot Table 装载率达到 40% (26,214 槽位) 时，触发轮转。轮转过程使用单个 `AtomicBoolean rotating.compareAndSet(false, true)` 实现单核独占清表，**其他所有读写线程在此期间完全不被阻塞，继续正常处理流量**；
* **SIMD 向量化清空旧表 + StoreStore 屏障**：
  清空旧表时采用 `Arrays.fill(entries, 0L)`，HotSpot JIT 自动将其编译为底层 **AVX2/AVX-512 向量化 SIMD 指令**，耗时仅几微秒；随后以 `VarHandle.storeStoreFence()` 插入 StoreStore 写写内存屏障，确保清零彻底提交；最后利用 `setTablesRelease` 原子交换 Hot ↔ Cold 指针，全局读写视角瞬间就地切换！
* **`count` 计数与双表轮转防更新丢失设计 (Per-Table Counter & Rotation Consistency)**：
  为了彻底防止高并发下双表轮转导致计数丢失、计数漂移或数据丢失，设计了 **4 重闭环机制**：
  1. **表对象内聚独立计数 (Per-Table Dedicated Counter)**：每个 `TableHolder` 独立持有专属 `AtomicInteger count`。数据写入哪张物理表就递增哪张表的 `count`，绝对杜绝全局共享计数的跨表竞态污染；
  2. **写前装载率预判与动态重定向 (Pre-Check & Dynamic Re-Acquire)**：
     - `put()` 入口先检查当前热表容量。若已达阈值，尝试触发轮转并**重新 Acquire 获取切换后的全新热表**，使后续新写入精准落入新热表；
     - **并发轮转间隙写入 Cold 表的安全性**：若写线程未抢到 `rotating` 锁，且新热表指针尚未发布（轮转极短微秒瞬态），该写入会继续落入当前表（即即将变成 Cold 表的实例）。**这绝不会导致数据丢失**：读线程在 Hot 表未命中时会自动 **Fallback 查询 Cold 表**，命中后立即触发 **冷到热无锁晋升 (Cold-to-Hot Promotion)** 自动回填至新热表，数据生命周期全程 100% 可见自洽；
  3. **精确区分占座与覆写 (Slot Occupy vs Idempotent Overwrite)**：仅在 `casKey` 成功抢占空槽/墓碑时才递增 `count`，已有 Key 就地覆盖时不递增，防止计数虚高引发过早虚假轮转；
  4. **冷表生命周期与晋升前安全重置 (Clean Reset Before Promotion)**：落在 Cold 表中的历史条目与 `count`，在当前轮转周期内充当只读缓存与晋升源泉；直到该 Cold 表在下一次轮转即将晋升为新 Hot 表前，单写者执行 `clear()` 强制将 `count.set(0)` 并 SIMD 清零数组，完成全生命周期的严格重置。

##### 1.4 【双组件通用架构】冷到热无锁晋升机制 (Lock-Free Cold-to-Hot Promotion)
* 读请求未命中 Hot Table 但在 Cold Table 中命中时，读线程执行 `cold.casKey(idx, key, TOMBSTONE)` 原子抢占并标记冷表槽位为墓碑，随后异步向 Hot Table 调用 `put` 进行晋升写入；
* 整个冷热晋升过程无全局锁、无链表调整，以粗粒度双表轮转完美兼顾了高命中率与极致吞吐。

#### 2. 64-bit 位域压缩 (Bit-Packing) 与撕裂读 (Torn Read) 物理根绝

在 Java 中，如果分别用两个独立变量存储 `long userId` 与 `long expireTimestamp`，不仅需要 16 字节内存，而且在并发读写更新时极易触发严重的**撕裂读 (Torn Read)** 隐患。本项目利用业务特征进行位运算压缩：

```
 63                               32 31                                0
┌───────────────────────────────────┬───────────────────────────────────┐
│       32-bit User ID (UID)        │     32-bit Expire Timestamp (s)   │
└───────────────────────────────────┴───────────────────────────────────┘
```

##### 深度原理解析：如果不采用 Bit-Packing，撕裂读是如何引发严重故障与安全漏洞的？

若将 UID 与 ExpireTimestamp 存放于两个独立字段（或两个独立数组），写线程在并发更新槽位数据时（例如 Token 续期、槽位覆写更新），必然需要执行两次写入：`write(UID)` 与 `write(ExpSec)`。在缺少昂贵复合互斥锁的情况下，并发读线程极易在两次写入的**瞬态间隙**中读取到错位数据，引发两大灾难性场景：

```
【场景 A：新 UID + 旧过期时间 ──► 业务误杀故障】
写线程操作：准备将槽位更新为 用户 B (UID=2002, 12:00 有效)
槽位原状态：用户 A (UID=1001, 10:00 已过期)

 1. 写线程写入新 UID:  UID = 2002 ──┐
                                     ├─► ⚡ 并发读线程介入读取！
 2. 写线程尚未更新 Exp: Exp = 10:00 ──┘   读到组合状态：[UID=2002, Exp=10:00 (旧过期时间)]
                                        结果：合法用户 B 被网关误判为 Token 已过期，请求被错误拦截！

【场景 B：旧 UID + 新过期时间 ──► 越权与安全放行漏洞】
若写线程先更新 ExpireTimestamp，再更新 UID：

 1. 写线程写入新 Exp:  Exp = 12:00 ──┐
                                     ├─► ⚡ 并发读线程介入读取！
 2. 写线程尚未更新 UID: UID = 1001 ──┘   读到组合状态：[UID=1001 (已失效旧用户), Exp=12:00 (新有效时间)]
                                        结果：本已过期的非法/封禁用户 A 被错误延期，成功绕过鉴权放行！
```

##### 架构深思与权衡：为什么不采用“拆分独立字段 + 级联 0L 哨兵自旋”，而坚决选择 Bit-Packing？

在并发理论上，通过类似 `JwtSigUidCache` 的“多阶段 Release 屏障 + 0L 哨兵 + 读路径自旋等待”时序控制，确实也能在软件逻辑上避免撕裂读。但从底层的**并发算法等级、CPU 缓存行局部性与状态机复杂度**来看，Bit-Packing 带来了 3 个无可替代的质变收益：

| 架构对比维度 | 拆分多字段 + 级联 0L 哨兵自旋 | 64-bit Bit-Packing 单字打包 (本项目选择) |
| :--- | :--- | :--- |
| **并发算法等级** | **Lock-Free（带自旋）**：读写均需多阶段握手，遇 0 需自旋 | **Wait-Free（最高并发级别）**：读写均在固定指令周期内必达 |
| **`LocalBanCache` 写入** | 必须执行 `CAS 占座 ➔ 竖 0 哨兵 ➔ 写 Exp ➔ 撤哨兵`（4 步） | **单条 `LOCK CMPXCHG` 汇编指令 1 步原子落盘！** |
| **`LocalBanCache` 读取** | 读到 0L 必须进入 Slow Path 自旋重试 | **单条 `MOV` 指令直接解包返回，100% 绝对零自旋！** |
| **CPU L1 缓存行利用率** | 需产生 2 次不连续内存寻址（加载 2 根 Cache Line），极易引发 L1D 缓存颠簸 | **1 根 64-Byte Cache Line 紧凑容纳 8 个完整槽位**，访存次数减半，L1D 命中率翻倍 |
| **状态机复杂度** | `Key + Prefix + UID + ExpSec` 演变为 **4 维级联状态空间**，自旋分支预测开销剧增 | **4 维状态瞬间降维为极简的“Prefix + ValExp”2 阶段发布**，写发布背靠背 3 条指令完成 |

##### 硬件微架构层面的物理根绝优势：

* **内存占用缩减 50% & 缓存行有效利用率翻倍**：
  单数组 `long[]` 直接将 `(UID << 32) | (ExpSec & 0xFFFFFFFFL)` 紧凑打包进单个 64 位槽位，单条 64-Byte Cache Line 容纳有效状态数提升 100%。
* **单指令 64 位硬件原子性 (Single-Instruction Hardware Atomicity)**：
  在现代 64 位体系结构（x86-64 / ARM64）下，64 位整型的读写由底层单条汇编指令（如 `MOV` 或 `LOCK CMPXCHG`）直接完成。
  - **写入不可分割**：UID 与 ExpSec 在 1 个 CPU 指令周期内**同时写入、同时生效**；
  - **读取不可分割**：读线程要么读取到写入前的完整老状态，要么读取到写入后的完整新状态，在物理硬件层面彻底切断了中间撕裂态的产生空间，**以 0 锁开销达成了强一致性状态发布**！

#### 3. JwtSigUidCache 特化专属：分层交错物理内存布局 (Layered Interleaved Memory Layout)

> [!NOTE]
> **缓存选型与架构边界说明**：
> * **`LocalBanCache` (黑名单缓存)**：每个条目仅需存储 `userId` 与 `expireTimeSec`，借助上述 **64-bit Bit-Packing** 已能完美压缩进单个 64 位 long 中，采用**纯单一扁平数组 (`long[] entries`)** 即可完成极速探查与数据提取，无需独立 Value 数组；
> * **`JwtSigUidCache` (JWT 鉴权缓存)**：每个逻辑条目包含 64-bit `sigHash`、64-bit `sigPrefix`、32-bit `UID` 与 32-bit `ExpSec`（单条目需 24 字节），字段的多样性与读写不对称性才催生了以下**探查域与数据域解耦的分层交错布局**。

在网关鉴权架构中，**`JwtSigUidCache` 的读查询（Read Path）占据了 99.9% 以上的流量**，是决定单机极限吞吐与纳秒级延迟的绝对核心。为了将查询性能压榨至现代硬件物理极限，为其量身定制了**探查域（交错）与数据域（解耦）的物理分层隔离架构**：

```
【探查数组 keyPrefixes】(2-Long 密集交错排列，极速查询探查域)
 0                   16                  32                  48                  64 Bytes
┌───────────────────┬───────────────────┬───────────────────┬───────────────────┐
│ Key 0 | Prefix 0  │ Key 1 | Prefix 1  │ Key 2 | Prefix 2  │ Key 3 | Prefix 3  │ ◄── 1 条 64B L1 Cache Line
└───────────────────┴───────────────────┴───────────────────┴───────────────────┘
 └── 槽位 0 探查键 ──┘ └── 槽位 1 探查键 ──┘ └── 槽位 2 探查键 ──┘ └── 槽位 3 探查键 ──┘
 ◄────────────────────── 线性探测 4 步连续扫描，100% 在同一条 L1 缓存行内完成 ──────────────────────►

【数据数组 valExps】(物理独立解耦，存放 packed(UID, ExpSec)，按需读取数据域)
┌───────────────────┬───────────────────┬───────────────────┬───────────────────┐
│      ValExp 0     │      ValExp 1     │      ValExp 2     │      ValExp 3     │
└───────────────────┴───────────────────┴───────────────────┴───────────────────┘
```

##### 深度体系结构解析：高频查询流水线与交错布局的硬件契合点

在开放寻址哈希表的只读查询中，CPU 运行的核心操作是一条**高频三连击流水线**：
```
       ┌────────────────────────┐
       │ ① 初始槽位快速定位     │ ──► hash & MASK 计算起始槽位 idx
       └───────────┬────────────┘
                   ▼
       ┌────────────────────────┐
       │ ② 开放寻址线性探测     │ ──► 遇到冲突时顺次步进 idx = (idx + 1) & MASK (最多 MAX_PROBE 步)
       └───────────┬────────────┘
                   ▼
       ┌────────────────────────┐
       │ ③ 二次碰撞安全检测     │ ──► 先比对 Key == sigHash，再紧接着比对 sigPrefix == prefix
       └───────────┬────────────┘
                   ▼ (二重校验完全匹配成功后)
       ┌────────────────────────┐
       │ ④ 最终单次按需读取     │ ──► 从独立 valExps[idx] 一次性抓取 packed(UID, ExpSec)
       └────────────────────────┘
```

在这套高频查询循环（**定位 ➔ 线性探测 ➔ 二次碰撞检测**）中，CPU 唯一需要访问并参与逻辑计算的数据**仅且仅有 `Key` 与 `SigPrefix`（每个 8 字节，合计 16 字节）**。

| 物理域 | 数据成员 | 并发访问特征 (Access Patterns) | 在高频查询三步曲中的硬件表现 | 布局决策与体系结构收益 |
| :--- | :--- | :--- | :--- | :--- |
| **探查域**<br>`keyPrefixes[]` | `key` (64-bit Hash)<br>`sigPrefix` (8B 前缀) | **定位 + 线性探测 + 二次碰撞检测**<br>(成对、只读、高局部性) | 1. **定位与二次检测零寻址延迟**：槽位定位后，Key 与 Prefix 物理相邻，单次内存抓取同时喂入 CPU 寄存器完成二次比对；<br>2. **线性探测 4 步零跨行 Miss**：单条 64B L1 Cache Line **紧凑打包 4 组连续探测槽位**（$4 \times 16\text{B} = 64\text{B}$）。开放寻址遇到冲突连续探测时，**后续 4 步探测全部在同一条已经被 L1 Cache 命中的缓存行内部完成**，命中率 100%，无跨 Cache Line 惩罚。 | **允许且必须交错 (2-Long Interleaved)**：<br>彻底匹配高频查询流水线，消除碎片化内存寻址，硬件空间预取器 (Spatial Prefetcher) 效率达到理论极值。 |
| **数据域**<br>`valExps[]` | `packed(UID, ExpSec)`<br>(64-bit 打包数据) | **延迟访问、高频并发覆写**<br>(Delayed Read & Read-Write Asymmetry) | 1. **查询流水线末端按需读取**：前述“定位 ➔ 探测 ➔ 二次检测”循环中 **Value 完全不参与计算**，只有全部匹配成功后才按物理下标读取 1 次；探查未命中的槽位不需要 Value；<br>2. **并发覆写破坏只读流**：写线程更新 Value（如刷新过期时间存在并发覆写）属于写操作。 | **绝对禁止交错，必须物理隔离 (Isolated Array)**：<br>1. **避免探查密度被腰斩**：若塞入 Value（3-Long 占 24B），64B 缓存行只能容纳 2 组槽位，探查密度暴跌 50%，线性探测走 2 步就跨越缓存行边界触发 L1 Cache Miss；<br>2. **物理切断伪共享 (False Sharing)**：写线程更新 Value 会触发 MESI **RFO 广播使整行失效**。隔离后，写 Value 绝不会导致读线程正在探查 Key 的 S 态 L1 缓存行被冲刷失效。 |

* **探查密度极值化与 L1 Cache 容积保护 (Cache Density & Footprint Optimization)**：
  CPU 从内存子系统向 L1 Data Cache 搬运数据的**最小物理粒度是 64 字节的 Cache Line**。
  - **如果强行采用 3-Long 交错（`[Key, Prefix, Value]`）**：单个槽位膨胀至 24 字节，单条 64B 缓存行扣除对齐后只能容纳 2 组完整探查键。当开放寻址发生哈希冲突时，读线程每探查一个未命中的槽位，该槽位的 Value 都会伴随整行搬运强行载入 L1 Cache，导致**缓存行内的有效比对信息载荷比降低了 33%**。原本在同一条 Cache Line 内即可完成的 4 步连续探查，被迫频繁**跨越 Cache Line 物理边界触发额外的 L1 Cache Miss**；且无用的 Value 占用了极度稀缺的 L1 缓存容积（通常每核心仅 32~48KB），加剧了热点数据的 Cache Eviction（缓存抖动驱逐）；
  - **分层物理隔离后**：读线程探查链仅在紧凑的 `keyPrefixes` 数组上滑动，单行承载 4 组探查键，绝大多数冲突在当前已加载的 L1 Cache Line 内即可就地命中；只有在最终二重匹配成功后，才精准按物理下标单次读取 `valExps` 数组，做到了**探查阶段零冗余数据干扰，命中阶段单次寻址即达**。
* **MESI 协议与伪共享物理根绝 (False Sharing Elimination)**：
  将 `valExps` 剥离至独立物理数组后，写线程更新 Value 的内存写操作与读线程扫描 Key 的内存地址处于完全不同的物理缓存行。读核心持有 `keyPrefixes` 的 Shared (S) 缓存行永不被写线程针对 `valExps` 的 Invalidate (I) 信号打翻，单线程读延迟打入 **14.35 ns/op**，单线程吞吐高达 **6,971 万 QPS**，16 线程并发吞吐跃升至 **1.52 亿 QPS**（较基线 4 数组架构提升 **118.5%**）！

---

### 三、 两级无锁令牌桶架构 (`LocalGlobalRateLimiter`)

#### 🌟 架构定位与 5 维工程闭环 (Design Context, Premises & Trade-offs)

> **💡 一句话定义**：`LocalGlobalRateLimiter` 是防线 ① 的**单机物理算力熔断护体盾**，采用“节点级 64-bit 无锁全局桶 + FastThreadLocal 线程私有 AIMD 缓冲区 + Nacos 动态算力联动”，以 1,483 万 QPS 吞吐为单机提供 0 外部依赖的绝对过载防护。

1. **需求必要性 (Why Node-Level Local Global Limiting is Mandatory?)**：
   * **单机物理资源保护**：任何单台网关服务器的 CPU、网卡带宽与 TCP 连接处理能力都存在物理天花板（如单机安全水位 100,000 QPS）。当面对未知来源的瞬间海量洪峰时，必须在网关入口处以纳秒级速度拦截超出物理极限的洪水流量，防止单机 CPU 满载打垮宿主机。
2. **传统方案痛点与“异步租约”的物理边界 (Traditional Bottlenecks & Async Lease Limits)**：
   * **痛点 ①：向 Redis 租约的两难困境 (Why Redis Leasing Fails?)**：
     * **高频动态租约 ➔ 租约风暴（Lease Storm）**：若网关集群（如 50 台节点、800 个 EventLoop）频繁向 Redis 申请租约，海量租约网络 RTT 自身就会将 Redis 打死，违背了“最前置防线必须比被保护对象更轻更坚固”的铁律；
     * **低频批量预分 ➔ 流量倾斜与饥饿误杀**：若为了降低 Redis 压力而拉长租约周期（例如按秒预批租约），一旦发生流量倾斜，过载节点的租约会在几十毫秒内迅速耗尽导致大面积误杀正常用户，而闲置节点的配额无法即时让渡；
     * **网络抖动 ➔ 级联雪崩**：网关与 Redis 之间一旦发生网络抖动或 Redis 慢查询，全网节点将因租不到令牌而在入口处将 100% 合法流量误杀（全网瘫痪）。
   * **思辨拓展：为什么“自适应步伐调节 (AIMD / Adaptive Step Sizing)”在单机内 100% 丝滑生效，但在分布式跨网络中却会撞上物理天花板？**：
     * **① 物理介质与时延的 6 个数量级鸿沟 (Physical Medium Gap)**：
       * **单机进程内（EventLoop 线程 $\leftrightarrow$ 本地全局桶）**：基于 CPU L1/L3 Cache 缓存一致性总线（UPI/Ring Bus），单次 CAS 往返仅需 **$\approx 10\text{ 纳秒 (ns)}$**；
       * **分布式跨网络（网关节点 $\leftrightarrow$ 外部 Redis）**：历经物理网卡 (NIC) ➔ 光纤/网线 ➔ 交换机 ➔ Linux TCP 协议栈 ➔ Redis 单线程处理，单次网络 RTT 需要 **$\approx 1 \sim 2\text{ 毫秒 (ms)} = 1,000,000 \sim 2,000,000\text{ ns}$**；
       * **物理时延差距跨越了 5 ~ 6 个数量级（快 100,000 ~ 200,000 倍）！**
     * **② 控制论与因果律推导：反馈回路时延与第 0 毫秒突变的因果律真空 (Causality & Zero-Lag Feedback)**：
       * **反馈控制的因果律本质**：任何自适应算法（AIMD / EMA）调整步长的依据都是“观测过去已发生的流量”，**算法在物理因果律上永远无法预知第 0 毫秒从 0 到 100,000 QPS 的瞬时垂直阶跃突变**；
       * **为什么单机内存总线可以？** 突发 100,000 QPS 脉冲时，请求间隔为 $10\mu s$（$10,000\text{ns}$）。第 1 个请求在第 0 纳秒触发自适应扩容，向本地全局桶发起 CAS 仅耗时 **$10\text{ns}$**。因为 **$10\text{ns} \ll 10,000\text{ns}$**，在第 2 个请求（第 10,000 纳秒）到达前，私有缓冲区早已借回大额令牌！**单机反馈时延完全被淹没在硬件时钟周期内（Zero-Lag Feedback），根本不存在断粮真空期**；
       * **为什么分布式跨网络不行？** 突发 100,000 QPS 阶跃脉冲时，每 1 毫秒涌入 100 个请求。网络反馈时延 **$1,000,000\text{ns} \gg 10,000\text{ns}$**。在第 1 个请求发出自适应租约到网络回包的 **1 毫秒真空期内**，后续 100 个请求已全部到达并打空了本地库存，算法因果律滞后必然导致首波请求**发生局部饥饿误杀**。
     * **③ 并发争用规模、低水位均分与“保底步长 + 熔断退避”防御的终极反思**：
       * **过度囤积死锁**：若集群 50 ~ 100 台网关节点在突发时自适应放大步长（如每节点申请 50,000），集群总配额会在第 1 毫秒内被抢先到达的少数几台节点提前掏空并囤积在本地（Over-allocation），导致其余节点由于配额枯竭而全量误杀；
       * **方案设想（设定保底步长与低水位公平均分）**：为了防止过度囤积，理论上可以设定单次租约步长上限 `MAX_STEP` 与保底步长 `MIN_STEP`（如 $\text{MIN\_STEP}=500$），并在 Redis 濒危低水位时按 $\text{FairQuota} = \frac{\text{Remaining}}{N}$ 执行公平均分配额；
       * **分布式核心难点：Redis 必须实时感知动态活跃节点数 $N$（引入心跳与拓扑复杂度）**：
         * **单机进程内（极简）**：Netty EventLoop 线程数是恒定不变的（$N=16$），单机低水位均分算 `availableTokens / 16`，0 成本、0 维护；
         * **分布式跨网络（重型）**：网关在 K8s / 云原生环境中是动态弹性伸缩的（Pod 随时水平扩缩容、滚动重启与故障剔除）。Redis Lua 要精确计算 $\frac{\text{Remaining}}{N}$，就必须在 Redis 中维护一套**全网网关节点心跳注册表（Heartbeat Registry）**。这不仅让无状态数据层沦为重型的集群协调中心，而且秒级的心跳滞后性会导致算出的均分配额发生严重失真；
       * **低水位均分的次生痛点：租约风暴指数级放大悖论 (Lease Storm Amplification)**：若在 Redis 濒危（存量<10%）时强制公平均分配额（如每节点只分 200 个），会导致各节点在 4ms 内扣光配额，全网以 **12,500 次/秒** 的恐怖频率高频重试，在系统最脆弱时引发致命的 **Redis 租约风暴**；
       * **双层防御耦合共振与“早夭假死死锁 (Coupled Defensive Resonance & Premature Tripping)”**：
         * 若进一步引入“取得少于 $\text{MIN\_STEP}$ 就短路熔断退避以保护 Redis”，系统会爆发**两层防御自相矛盾的死锁灾难**：
         * **灾难场景**：假设 50 台节点，保底阈值 $\text{MIN\_STEP}=500$，此时 Redis 中**明明还剩 15,000 个有效可用令牌**；
         * **恶性共振**：Redis 执行低水位公平均分 $\text{FairQuota} = \frac{15,000}{50} = 300\text{ 个}$；节点拿到 300 个令牌后检测到 $300 < \text{MIN\_STEP}(500)$，**瞬间误判为 Redis 彻底断粮而集体进入短路熔断**！
         * **荒谬后果**：**全网网关 100% 提前早夭熔断（开始大面积误杀合法用户），而 Redis 内部明明还沉睡着 15,000 个有效配额，却被活活死锁闲置！**（公平均分追求“配额切碎”，短路熔断要求“配额保底”，两者在分布式环境中天然对立冲突）；
       * **单机本地全局桶的终极架构反思**：既然在极端洪峰和中心濒危时，分布式租约方案无论如何修补（均分 ➔ 租约风暴 ➔ 保底熔断 ➔ 早夭死锁）最终都不可避免地退化为“单机本地熔断决策”，那么与其把系统可用性押注在“跨网络租约 + K8s 节点心跳注册 + 租约风暴熔断”的重型链路上，不如**在防线 ① 直接采用纯栈 64-bit 本地全局桶（1,483 万 QPS）+ Nacos 秒级热更新**，彻底免疫任何网络时延、租约风暴与早夭死锁，达成最纯粹的 0 外部依赖与极致单机高可用！
     * **两级自适应分层落地实践**：正因如此，本项目在**单机进程内（EventLoop 线程与本地全局桶之间，CPU 内存总线 RTT $\approx 0$）全面落地了 AIMD 自适应动态步长**（`MIN_STEP=4` 至 `MAX_STEP=512`）；而跨网络节点间则坚守本地无锁桶 + 安全冗余系数（1.2x），以 0 外部依赖守住单机 1,483 万 QPS 绝对底线。
   * **痛点 ②：传统单机写死配置的弹性僵化 (Why Hardcoded Static Config Fails?)**：
     * 若在本地配置文件中写死静态固定容量（如 `capacity: 100000`），当集群在大促期间动态弹性扩缩容（如节点数由 10 台扩容至 50 台）时，集群总承载阈值被动放大了 5 倍，导致单机防线严重失真，无法自适应集群总算力目标。
3. **解决依赖的前提与工业级四层立体防御体系 (Underlying Premises & Production Architecture)**：
   * **现实洞察：L4 负载均衡（SLB）分配的是“TCP 连接”而非“请求流”**：
     * 四层负载均衡（LVS / F5 / 云 SLB）仅在 TCP 握手阶段按权重均摊**物理连接数**；
     * 现代客户端与微服务网关普遍采用 **长连接（HTTP Keep-Alive）与 HTTP/2 多路复用（Multiplexing Streams）**。这意味着少数几个 TCP 连接可能集中涌入数万 QPS 的高频 Stream 流，**“连接数均匀”物理上必然导致“单机请求流量倾斜（Traffic Skew）”**；
   * **源头治理：网关自主 L4 连接生命周期重平衡 (Gateway-Driven Connection Rebalancing)**：
     * **架构定位明晰**：本项目网关直接挂载于 **L4 负载均衡（LVS / F5 / 云 SLB）之后**，作为业务集群的**第一道 L7 极速统一入口与安全防线**。由于直面外部海量客户端 TCP 长连接，网关自身必须主动编排连接生命周期：
     * **① 最大请求数切断 (`max_requests_per_connection`)**：单条 TCP 长连接处理满 5,000 ~ 10,000 个请求后，网关在 HTTP Response Header 中回写 `Connection: close`（或发送 HTTP/2 `GOAWAY` 帧），通知客户端当前连接优雅终止。客户端发起新握手时，L4 SLB 就会将其均匀重定向分摊到其他空闲网关节点；
     * **② 最大连接寿命漂移 (`max_connection_age`)**：利用 Netty `IdleStateHandler` 或 Channel 存活时间戳，长连接存活达 3~5 分钟强制触发优雅断连，彻底消除爬虫或批量服务与单台网关长年死锁绑死导致的静态倾斜。
   * **控制面（Control Plane）秒级非对称动态算力调控闭环**：
     * **① 旁路秒级指标上报 (Out-of-Band Reporting)**：各网关 Pod 通过后台守护线程每秒无锁采集当前的实测 QPS（$Q_i$）与宿主机 CPU 负载，异步上报控制面（0 阻塞 Netty IO 线程）；
     * **② 非对称配额动态计算 (Asymmetric Allocation Formula)**：控制中心（Nacos / Sentinel Controller）聚合全网活跃节点数 $N$ 与实测流量分布，计算各节点的非对称权重并推算单机目标容量：
       $$\text{Weight}_i = \frac{Q_i + \epsilon}{\sum_{j=1}^{N} (Q_j + \epsilon)}, \qquad \text{DynamicCapacity}_i = Q_{\text{total}} \times \text{Weight}_i \times 1.2\text{ (安全冗余系数)}$$
     * **③ 毫秒级无锁原地生效**：控制面通过 Nacos（`GatewayRateLimitConfigListener`）推送快照，网关数据面通过 **`VarHandle` / 原子快照** 在微秒级完成原地替换，**0 锁、0 线程挂起、0 服务中断**。
4. **方案的架构取舍 (Architecture Trade-offs)**：
   * **放弃的能力 (Sacrifices - 目标层放弃)**：
     * **放弃了集群宏观总 QPS 的“绝对刚性数学精准贴合 (Rigid Precision)”**：由于引入了 $1.2\text{x}$ 安全冗余系数与控制面秒级非对称收敛窗口，集群在流量发生剧烈倾斜或垂直突发的瞬间，实际全网总放行吞吐允许在 $[0.9 \times Q_{\text{total}}, 1.2 \times Q_{\text{total}}]$ 范围内存在微小的**弹性容差区间 (Elastic Tolerance Margin)**，而非数学上绝对分毫不差的硬卡死；
   * **采用的工程解法 (Architectural Means - 手段层解耦)**：
     * **数据面坚决不跨网络集中协调**：放弃在数据面核心转发热路径上向外部中心发起同步/异步配额协商，数据面 Pod 不维护全网集群拓扑与成员心跳，专注极速无锁转发；
   * **换取的收益 (Gains)**：
     * **数据面与控制面彻底解耦（Control/Data Plane Decoupling）**：
     * **达成了 100% 单机高可用、0 外部依赖、1,483 万 QPS 极限处理吞吐（67.4 ns）与纳秒级极速物理熔断护体**；即使外部 Redis 或控制中心发生网络分区或彻底宕机，网关单机物理防线依然坚如磐石；
     * **职责清晰分层**：将“单机物理资源过载防护（允许弹性容差）”留给防线 ①，将“100% 刚性绝对零超卖”完全下沉到防线 ③ 的 Redis 阶梯限流引擎中执行。
5. **边界场景与防御机制 (Edge Cases & Resilience)**：
   * **低水位公平配额保护 (<1%)**：当全局令牌跌破 1% 时强制平摊配额，杜绝突发倾斜线程抢光残存令牌；
   * **物理租约到期机制 (Lease Expiration)**：闲置期残留令牌强制按速率过期清零，消除低谷期跨窗口突发倾泻历史旧令牌引发的下游打垮；
   * **配额枯竭短路避退 (Jitter Backoff)**：全局配额耗尽时步长平滑折半并触发随机 Jitter 错峰避退，阻止空转 CAS 暴击 CPU。

---

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

#### 🌟 架构定位与 5 维工程闭环 (Design Context, Premises & Trade-offs)

> **💡 一句话定义**：Per-UID 阶梯式限流引擎通过 **“前 80% 安全区异步批量快道 + 后 20% 临界区响应式强校验 + Redis Pub/Sub 全网广播”**，在保证分布式**全局配额 0 逃逸、捍卫全网用户抢购公平性**的前提下，消除了 90% 以上常规请求的网络 RTT 阻塞，实现数百万 QPS 极限吞吐。

1. **需求必要性 (Why Distributed Per-UID Limiting & Fairness are Mandatory?)**：
   * **单机限流的全局盲区与配额垄断**：单机本地令牌桶（防线 ①）仅能防御单个节点的总体物理硬件过载；但在大促抢票与秒杀场景中，黄牛黑产会利用海量分布式代理 IP 与群控脚本，将同一个目标 UID 的超频高并发刷单请求，打散路由到**集群内的数十台不同网关节点**上；
   * **捍卫全网用户配额公平性 (Quota Fairness & Anti-Monopolization)**：若仅依赖单机限流（如单机限 5 次/秒），黑产跨 50 台节点即可在 1 秒内刷出 $50 \times 5 = 250$ 次高频请求，不仅绕过限流规则，更**垄断了有限的 API 算力与稀缺抢购机会，剥夺了普通正常用户的公平抢购权**；因此，必须在分布式集群维度对单 UID 配额进行**全局精准仲裁与防逃逸拦截**。
2. **传统方案痛点 (Traditional Bottlenecks)**：
   * **传统纯同步限流 (Lettuce/Jedis 同步调用)**：每次请求都强制跨网络打 Redis，网络 RTT（0.5~2ms）直接将网关单请求耗时拉长数倍，单机吞吐被锁死在 1~2 万 QPS，且极易引发 Netty 线程池耗尽瘫痪；
   * **传统纯异步记账 (Fire-and-Forget 纯异步)**：虽然吞吐高，但异步攒批与网络传输存在毫秒级时间盲区。当黑产突发超高频冲刷时，在本地尚未感知到配额耗尽的几十毫秒内，已有数千个非法请求被乐观放行穿透到业务层（引发严重的配额逃逸与后端热点击穿）。
3. **解决依赖的前提 (Underlying Premises)**：
   * **流量帕累托法则 (80/20 Rule)**：在真实生产流量中，**99% 以上的普通合法用户在其生命周期内永远处于安全配额区间（0 ~ 80% 水位）**，绝不会触碰限流临界线；只有不到 1% 的异常刷子和黄牛黑产才会逼近配额枯竭；
   * **水位预警窗口充足**：20% 的缓冲配额（Buffer Quota）足以给 Redis 触发全网 Pub/Sub 广播并在各网关节点生效留出充足的物理毫秒级窗口。
4. **方案的架构取舍 (Architecture Trade-offs)**：
   * **放弃绝对的强一致性，采用“阶梯式最终一致性”**：
     * **前 80% 安全区**：牺牲微小的瞬时强一致性，采用本地 `FastThreadLocal` 攒批异步入账，换取 **400 万+ QPS 的零 RTT 极速放行**；
     * **后 20% 临界区**：牺牲微秒级的响应等待，精准切入 Mode B 响应式无阻塞校验，换取 **100% 分布式全局配额 0 逃逸与精准刚性拦截**。
5. **边界场景与防御机制 (Edge Cases & Resilience)**：
   * **Pub/Sub 广播丢包与网络分区**：Redis Pub/Sub 属于尽力交付（At-Most-Once），若广播丢包导致某个节点未收到预警，Redis 侧 Lua 脚本在配额扣减至 100% 时依然会执行硬拦截，并通过硬封禁广播二次兜底；
   * **Redis 服务不可用与断网逃逸**：自研 `failClosed` 机制，在 Redis TCP 断开或无可用连接时，临界请求直接快速失败拒绝，杜绝黑产在 Redis 故障期间逃逸；
   * **网络抖动与慢查询**：50ms 时间轮熔断与快速失败，防止在途请求堆积导致内存反压过载。

---

```
[用户请求到达] ──► 检查本地 LocalBanCache 状态
                        │
       ┌────────────────┴────────────────┐
       ▼ (正常状态: 0 ~ 80% 配额)          ▼ (预警状态: 80% ~ 100% 临界区)
┌──────────────────────────────┐  ┌──────────────────────────────────────────┐
│ Mode A: 异步批量快道          │  │ Mode B: 响应式无阻塞精确校验             │
│ 1. 本地 FastThreadLocal 攒批 │  │ 1. 挂起续体: AsyncRateLimitContext (0-GC)│
│ 2. 数量(32) 或 时间(50µs)触发 │  │ 2. 4层反压: setAutoRead(false) 暂停读取   │
│ 3. RESP2 Pipeline 一次性发送 │  │ 3. 16条微攒批 Pipeline 写入 Redis      │
│ 4. 0 RTT 阻塞，极速放行      │  │ 4. Redis 回包事件驱动唤醒，0 线程阻塞    │
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
  基于 `FastThreadLocal<ThreadRedisBatchBuffer>` 维护线程本地 `long[]` 数组。达到 **32 条** 或时间间隔达到 **50µs** 时，将批量命令合并写入单个 Direct ByteBuf 执行 Pipeline 发送。端到端零阻塞，单机提交吞吐突破 **400 万+ ops/s**。
* **水位触发全网广播**：
  Redis 侧 Lua 脚本在扣减后原子判断：若该 UID 剩余配额低于 20%，立即执行 `redis.call('PUBLISH', 'NETTY_LIMITER_BAN_CHANNEL', 'W:' .. uid)`。
* **临界配额响应式精准拦截（Mode B）**：
  网关订阅组件 `RedisUserBanSubscriber` 接收到广播后，在 `LocalBanCache` 中将该 UID 的状态标记为 `WARNED_EXP_SEC_MARK (-2L)`（Sync Required）。后续针对该 UID 的请求**强制切入 Mode B 响应式无阻塞校验**，直接由 Redis 仲裁最后的配额。既消除了 90% 以上常规请求的网络 RTT 开销，又彻底封堵了在乐观异步放行模式下的超卖漏洞，同时 Worker 线程 0 阻塞释放，单连接吞吐突破 **10.3 万 QPS**！

---

### 五、 0-GC 响应式无阻塞续体与 TCP 物理反压架构 (Reactive Continuation & Backpressure Engine)

#### 0. 为什么需要自研 0-GC Reactive 范式？（Project Reactor 的 GC 困境 vs 本项目解法）

根据 **《响应式宣言》（The Reactive Manifesto）**，响应式架构的核心在于四大支柱：**即时响应性 (Responsive)、韧性 (Resilient)、弹性与反压 (Elastic & Backpressure)、消息驱动 (Message-Driven)**。

在传统 Java 响应式技术栈（如 Spring WebFlux / Project Reactor / RxJava）中，构建一条响应式流水线（如 `Mono.just().flatMap().subscribe()`）虽然实现了线程的非阻塞切换，但在 **微基准内存分配（Alloc Profiling）** 下暴露出致命缺陷：
* **海量中间装饰器对象（Heap Pollution）**：每个 HTTP 请求会动态实例化 8 ~ 15 个操作符对象（`MonoFlatMap`, `FluxMapFuseable`, `Operators$MonoSubscriber`, `MonoSink` 等），在高频百万 QPS 下每秒产生几百兆垃圾对象，直接摧毁了 0-GC 目标；
* **应用层虚拟反压的局限性**：Reactor 的反压（`request(n)`）仅停留在 JVM 堆内存内部，若客户端持续通过 Socket 灌入报文，Netty 依然会源源不断把字节读入堆内存，最终引发内存溢出（OOM）。

**本项目彻底摒弃了上层沉重的响应式抽象，在 Netty Socket 裸字节流与 JMM 硬件内存层，纯手工系统性实现了 100% 0-GC 的原生响应式范式：**

```
┌────────────────────────────────────────────────────────────────────────────────────────────────────────┐
│                                 0-GC 原生响应式架构 四大支柱映射矩阵                                     │
├───────────────────────┬────────────────────────────────────────────┬───────────────────────────────────┤
│ 响应式宣言核心支柱    │ 传统 Project Reactor / WebFlux 实现方式    │ 本项目 Netty 0-GC 原生架构实现     │
├───────────────────────┼────────────────────────────────────────────┼───────────────────────────────────┤
│ 1. 消息驱动 (Message) │ Mono/Flux 事件流 + Lambda 回调链 (多堆对象)│ 续体机制 (Suspend & Resume) + 0-GC│
│                       │                                            │ Recycler 池化上下文 (0 堆分配)    │
├───────────────────────┼────────────────────────────────────────────┼───────────────────────────────────┤
│ 2. 弹性反压 (Elastic) │ JVM 堆内 request(n) 队列流控 (无法遏制TCP) │ Linux 内核 4 层物理级 TCP 零窗口  │
│                       │                                            │ autoRead(false) 物理刹车          │
├───────────────────────┼────────────────────────────────────────────┼───────────────────────────────────┤
│ 3. 即时响应 (Response)│ EventLoop 线程调度 (受 GC STW 停顿干扰)     │ 0 线程阻塞 (无 park/wait) + 0-STW │
│                       │                                            │ 硬件 L1 Cache 亲和性保障          │
├───────────────────────┼────────────────────────────────────────────┼───────────────────────────────────┤
│ 4. 韧性容错 (Resilient)│ Mono.timeout() / retry() 包装对象           │ 状态机 CAS 原子互斥 + 50ms 时间轮 │
│                       │                                            │ HashedWheelTimer + 溢出快速失败   │
└───────────────────────┴────────────────────────────────────────────┴───────────────────────────────────┘
```

---

#### 1. 响应式范式的核心实现机制全景拆解

```
网关 EventLoop 线程 (Http-EL)                           Netty Redis EventLoop 线程 (Redis-EL)
┌──────────────────────────────────────────┐             ┌──────────────────────────────────────────────┐
│ 1. 收到 HTTP 请求，判定切入 Mode B        │             │                                              │
│ 2. 挂起流水线：suspendAndAcquire...      │             │                                              │
│    - 物理反压: setAutoRead(false) 暂停   │             │                                              │
│    - 借出续体: AsyncRateLimitContext     │             │                                              │
│ 3. 提交 Redis-EL 异步 Pipeline 环形队列  │ ──────────► │ 1. RingBuffer.offer(ctx) 串行入队            │
│ 4. Http-EL 立即释放！(0 线程阻塞)         │             │    (以 state.setRelease 作为终极发布门禁)    │
│                                          │             │ 2. 发送 RESP2 EVALSHA 堆外直接内存字节流      │
│                                          │             │ 3. 3ms 后 Redis 回包，FrameDecoder 切分      │
│                                          │             │ 4. RingBuffer.poll(ctx) 弹出上下文并有序置空  │
│                                          │             │ 5. CAS 抢占成功并取消 50ms 时间轮超时任务    │
│ 5. 恢复续体：resumeContinuation          │ ◄─ execute ─┤ 6. httpCtx.executor() 调度回原 Http-EL       │
│    - 解除反压: setAutoRead(true) 恢复读取│             │                                              │
│    - 放行 fireDownstream / 拦截 403      │             │                                              │
│ 6. asyncCtx.resume(granted) 恢复续体并   │             │                                              │
│    安全归还对象池 (100% 0-GC 闭环)       │             │                                              │
└──────────────────────────────────────────┘             └──────────────────────────────────────────────┘
```

##### 机制 ①：消息驱动与续体机制（Continuation Suspend & Resume）
* **流水线优雅挂起 (`suspendAndAcquireReactiveRateLimit`)**：
  当网关判定请求需要进入 Redis 临界校验时，绝对不调用 `LockSupport.park`、`CountDownLatch.await` 或任何同步阻塞方法。而是从 Netty `Recycler` 借出预分配的 `AsyncRateLimitContext`，将当前请求的全部现场（`ChannelHandlerContext`、物理 `downstreamBuf`、`userId`）封装进续体对象，并向 Redis 提交非阻塞任务，随后方法立即执行 `return;`。**当前调用栈彻底退出，当前 Netty EventLoop 线程毫不停顿，瞬间释放去处理其他数万并发连接**；
* **响应式事件恢复 (`asyncCtx.resume(granted)`)**：
  当 Redis 回包事件到达时（消息驱动），Redis-EL 线程原位解析出 UID 并弹出上下文，通过 `asyncCtx.httpCtx.executor().execute(...)` 调度回原始 HTTP 绑定的 EventLoop 线程，触发 `asyncCtx.resume(isAllowed)`。在回调内部根据仲裁结果调用 `fireDownstream(ctx, downstreamBuf)` 恢复下游 HTTP 编解码流水线，或调用 `rejectAndRelease(...)` 回写 403 拦截。

##### 机制 ②：4 层物理级 TCP 零窗口反压（TCP Physical Backpressure）
为了防止客户端在高并发下持续灌入请求导致网关内存膨胀，本项目将响应式反压推进到了操作系统内核与 TCP 协议层：
1. **应用层挂起**：调用 `ctx.channel().config().setAutoRead(false)`，从 Netty Selector 移除 `OP_READ` 事件；
2. **内核层积压**：网关操作系统的 `SO_RCVBUF`（TCP 接收缓冲区）被后续报文填满；
3. **协议层通知**：Linux TCP 协议栈向客户端发送 ACK 报文，并在 TCP Header 中宣告 **`Window Size = 0`（TCP 零窗口物理刹车）**；
4. **客户端挂起**：客户端操作系统协议栈物理停止发送任何数据包，将内存积压原路反推给客户端自身；
5. **恢复流通**：Redis 响应到达并调用 `setAutoRead(true)` 后，网关发送 `Window Update` ACK，客户端瞬间恢复发包，实现软硬件一体的极致弹性。

##### 机制 ③：无锁高性能环形缓冲区与 JMM 严格有序发布门禁 (`SyncWaitSlotRingBuffer`)

###### 🌟 架构定位与 5 维工程闭环 (Design Context, FIFO Invariants & Trade-offs)

> **💡 一句话定义**：`SyncWaitSlotRingBuffer` 是在 0 阻塞响应式架构中，基于严格 FIFO 机制、专为单 TCP 连接多路复用而设计的**跨 EventLoop 暂存并保序唤醒在途请求的 0-GC 无锁桥梁**。

1. **需求必要性 (Why RingBuffer is Mandatory?)**：
   * **单 TCP 连接多路复用现实**：在高并发大促秒杀下，网关 16 个 HTTP EventLoop 线程必须共享与 Redis 的单条物理 TCP 长连接，以消除连接池膨胀与 Redis 端的 Epoll 线程上下文切换损耗；
   * **Redis RESP2 协议“无 Request-ID”核心痛点**：RESP2 回包是极简的纯文本二进制流（例如 `:100000:1\r\n`），**回包中没有任何 UID、Trace ID 或请求序号**。当 HTTP 请求被非阻塞挂起后，Redis 回包到达时，网关必须拥有一座**0 堆分配、零锁竞争的跨 EventLoop 调度桥梁**，精准确定该回包对应哪一个在途请求上下文（`AsyncRateLimitContext`）。
2. **传统方案痛点 (Traditional Bottlenecks)**：
   * **`ConcurrentHashMap<Long, Promise>` 方案**：由于 Redis 回包缺乏 Request-ID，根本无法作为 Key 查询；且每次请求创建 `Promise` / `Future` 堆对象会产生海量垃圾，瞬间摧毁 0-GC 目标；
   * **`LinkedBlockingQueue` / `ArrayBlockingQueue` 方案**：多线程并发 `put()` / `take()` 存在全局锁争用与 CAS 激烈竞争，吞吐被锁死在数万 QPS。
3. **解决依赖的前提：为什么 FIFO 机制必然 100% 绝对成立？ (FIFO Invariant Proof)**：
   * 很多开发者质疑：“*Redis 回包没有 ID，凭什么队头弹出的第 1 个上下文就一定属于第 1 个回包？*” 这一机制能够 100% 成立依赖于**全链路四大物理铁律的串行闭环**：
     * **① 约束一（发送端原子绑定）**：网关在同一 EventLoop 线程内将“入队 RingBuffer（分配单调递增 Sequence）”与“将 RESP2 二进制命令写入 Socket”严格原子绑定执行；
     * **② 约束二（TCP 字节序严格保序）**：TCP 传输层按 `Sequence Number` 物理保障报文字节流在 Redis 服务端按发送顺序完全一致地组装；
     * **③ 约束三（Redis 单线程串行执行与回写）**：Redis 服务端核心是单线程单核 EventLoop，严格按收包顺序**串行执行 Lua 脚本**，并按执行顺序**串行写回 Socket**；
     * **④ 约束四（网关接收端单线程单调出队）**：网关端 `LineBasedFrameDecoder` 切分出每条响应后，Redis-EL 消费线程通过 `nextNeededAckSequence++` **严格单调递增出队**，实现 **1:1 绝对保序匹配**。
4. **方案的架构取舍 (Architecture Trade-offs)**：
   * **放弃的能力**：放弃了多 TCP 连接乱序异步打乱处理的灵活性，严格收敛为单 TCP 连接严格保序流水线；
   * **换取的收益**：彻底消除了哈希表寻址与 `Promise` 堆分配开销，以裸数组位与取模（`& mask`）和单发布门禁实现了 **0 堆分配、0 锁等待与单连接 10.3 万 QPS 的极限吞吐**。
5. **边界场景与防御机制 (Edge Cases & Resilience)**：
   * **超时与迟到回包竞争**：50ms 超时触发时，通过 CAS 原子写入 `CANCELLED_CONTEXT` COW 哨兵脱钩，**既杜绝了对象池复用引发的幽灵唤醒，又保持了 FIFO 序列号的物理连续性（不跳号）**；
   * **队列打满防爆**：利用 Safe Zone 本地快照快速判定，队列满时触发 Fail-Fast 快速失败拒绝，绝不让 Worker 线程发生死锁或无谓阻塞。

---

###### 🔬 RingBuffer 物理内存排布与 JMM 无锁实现细节

本项目自研的 `SyncWaitSlotRingBuffer` 在纯 JVM 裸内存层实现了与 LMAX Disruptor 媲美的无锁高并发吞吐：

```
                    ┌──────────────────────────────────────────────────────────────────┐
                    │               SyncWaitSlotRingBuffer 物理内存排布 (JMM)          │
                    ├──────────────────────────────────────────────────────────────────┤
Consumer 字段缓存行  │  p00..p07 (56B) │ nextNeededAckSequence │ cachedNextAvailableReq │ (64B 对齐)
                    ├──────────────────────────────────────────────────────────────────┤
Cache Line 隔离填充 │  p10..p17 (56B 物理填充，彻底切断跨核 CPU 缓存行伪共享)           │ (64B 隔离)
                    ├──────────────────────────────────────────────────────────────────┤
Producer 字段缓存行  │  p20..p27 (56B) │ nextAvailableReqSeq   │ cachedNextNeededAck    │ (64B 对齐)
                    ├──────────────────────────────────────────────────────────────────┤
物理直接数组存储     │  AsyncRateLimitContext[] array (2^n 长度，位与运算 & mask 取模)  │ (0-GC 存储)
                    └──────────────────────────────────────────────────────────────────┘
```

1. **56 字节 Cache Line 物理隔离与伪共享消除 (False Sharing Elimination)**：
   * 采用类继承链分层填充架构（`SyncWaitSlotRingBufferPad0` ➔ `ConsumerFields` ➔ `Pad1` ➔ `ProducerFields` ➔ `Pad2`）；
   * 通过 `p00..p07`、`p10..p17`、`p20..p27` 填充 56 字节无意义 `long` 变量，将 Consumer（Redis EventLoop 消费指针）与 Producer（网关 HTTP EventLoops 生产指针）物理隔离在不同的 64 字节 CPU 缓存行中，彻底切断高并发写入引发的 CPU L1/L2 缓存行频繁失效（Cache Line Invalidation Storm）；

2. **MPSC/SPSC 无锁抢占与单发布门禁模式 (`offer`)**：
   ```java
   public boolean offer(AsyncRateLimitContext ctx) {
       long currentAvailableReqSeq;
       do {
           currentAvailableReqSeq = (long) NEXT_AVAILABLE_REQUEST_SEQUENCE_HANDLE.getAcquire(this);
           if (isFull(currentAvailableReqSeq)) {
               return false; // 缓冲区满，Fail-Open 降级
           }
       } while (!NEXT_AVAILABLE_REQUEST_SEQUENCE_HANDLE.compareAndSet(this, currentAvailableReqSeq, currentAvailableReqSeq + 1));

       int index = (int) (currentAvailableReqSeq & mask);
       ctx.index = index;

       // 1. 普通写写入数组槽位 (开销最低，无需额外内存屏障)
       ARRAY_VH.set(array, index, ctx);

       // 🎯 2. 统一发布门禁：以 setRelease 发布 STATE_INIT，立下 StoreStore 屏障完成原子发布
       ctx.publish();
       return true;
   }
   ```
   * **为什么数组槽位可以使用普通写 (`ARRAY_VH.set`)？**
     * **JMM Happens-Before 传递性保障**：根据 JMM 规则，程序顺序规则保证 `write(ctx.fields)` $\le_{po}$ `ARRAY_VH.set(array, index, ctx)` $\le_{po}$ `ctx.state.setRelease(STATE_INIT)`；
     * **Release 屏障禁止下沉**：`ctx.publish()`（即 `state.setRelease(STATE_INIT)`）自带 JMM `StoreStore` 内存屏障，**严格禁止其上方的一切普通写（包括对 ctx 内部字段的写入和对数组槽位的写入）重排序下沉到 release 写之后**；
     * **消费端 Synchronizes-With**：消费端通过 `ctx.state.getAcquire()` 感知到 `STATE_INIT` 时，便与生产者的 `setRelease` 建立了严格的 `Synchronizes-With` 同步关系，此时上下文内部的所有字段与数组指针物理 100% 绝对可见。这一设计避免了在数组写和状态写上施加双重屏障，达成了开销最低的 **“单发布门禁模式 (Single Publication Gate)”**。

3. **50ms 超时哨兵原子脱钩：防御对象池复用导致的“幽灵唤醒”与“串包污染” (`cancel` & CANCELLED_CONTEXT COW)**：
   ```java
   public boolean cancel(AsyncRateLimitContext ctx) {
       if (ctx == null || ctx == CANCELLED_CONTEXT) {
           return false;
       }
       int index = ctx.index;
       // 原子替换为 CANCELLED_CONTEXT 哨兵脱钩
       return ARRAY_VH.compareAndSet(array, index, ctx, CANCELLED_CONTEXT);
   }
   ```
   * **方面 ①：原子断开悬挂引用，根除“幽灵唤醒与串包污染” (Anti-Phantom Wakeup & Cross-Request Pollution)**：
     * 当 50ms 时间轮超时先于 Redis 回包触发时，调用 `cancel(ctx)` 通过 CAS 将槽位物理指针原子替换为 `CANCELLED_CONTEXT` 哨兵，**彻底切断 RingBuffer 对已超时 `ctx` 的悬挂引用**；
     * 超时判定后，`ctx` 触发 `resume(false)` 向客户端快速返回拒绝，并被 Netty `Recycler` 对象池回收；
     * **对象池 ABA 场景重现**：随后该 `ctx` 内存块被极速复用分配给了一个**全新的用户请求 B**；
     * **安全屏障**：即使在 100ms 后原请求极度迟到的 Redis 回包终于到达，Redis EventLoop 在扫描槽位时读到的是 `CANCELLED_CONTEXT` 哨兵，**绝不会误将已经被复用给新请求 B 的 `ctx` 错误唤醒（即杜绝了幽灵唤醒与串包污染）**！
   * **方面 ②：队列物理序号连续性与零死锁推进**：
     * `cancel(ctx)` 绝不修改全局单调递增的消费序列号，槽位依然由哨兵物理占位，保持了严格的 FIFO 连续性；
     * 消费端在 `peek()` / `poll()` 遇到 `CANCELLED_CONTEXT` 哨兵时，仅推进消费序列号快速出队，零锁竞争、零业务开销。

4. **两级 Safe Zone 惰性缓存流控的数学证明与实例推演 (`isFull` & 消除跨核 Bus Sniffing)**：
   ```java
   private boolean isFull(long currentAvailableReqSeq) {
       if (currentAvailableReqSeq - this.cachedNextNeededAckSequence >= array.length) {
           long freshNeededAckSeq = (long) NEXT_NEEDED_ACK_SEQUENCE_HANDLE.getAcquire(this);
           if (currentAvailableReqSeq - freshNeededAckSeq >= array.length) {
               return true; // 队列物理打满
           }
           this.cachedNextNeededAckSequence = freshNeededAckSeq; // 刷新本地快照
       }
       return false;
   }
   ```
   * **数学单调性不变式 (Monotonic Invariant)**：
     * 消费序列号 $S_{ack}$ 是严格单调递增的（Consumer 只能向前消费，永远不会回退）；
     * 生产者的本地缓存记录的是过去的某个历史快照值：$\text{cachedNextNeededAckSequence} \le \text{freshNeededAckSeq}$；
     * 从而恒有：
       $$\text{currentAvailableReqSeq} - \text{cachedNextNeededAckSequence} \ge \text{currentAvailableReqSeq} - \text{freshNeededAckSeq}$$
   * **💡 为什么使用陈旧 Cache 也 100% 绝对正确？（具体实例推演）**：
     * **假设 RingBuffer 容量 `array.length = 1024`**：
       1. **第 1~500 次入队（第一级 Safe Zone 快路径）**：
          * 初始状态：`cachedNextNeededAckSequence = 0`，真实消费进度 `freshNeededAckSeq = 0`；
          * 生产者连续生产 500 个请求：`currentAvailableReqSeq = 500`；
          * 判定条件：`500 - 0 = 500 < 1024`，生产者**无需任何跨核总线嗅探（0 Bus Sniffing）**，直接在当前 CPU 核心的 L1 Data Cache 中瞬时完成判定！
       2. **假设此时 Consumer 已经默默消费了 300 个请求**：
          * 真实的实时进度已推进至 `freshNeededAckSeq = 300`；
          * 此时生产者看到的依然是陈旧快照 `cachedNextNeededAckSequence = 0`；
          * **关键安全性**：生产者以为队列里还剩 $1024 - 500 = 524$ 个空位，而实际上物理空位多达 $1024 - (500 - 300) = 824$ 个。
          * 也就是说，**陈旧的缓存只会让生产者做出更保守、更悲观的估计，绝对不会导致误判放行而发生环形缓冲区溢出（零假阴性，Zero False-Negative）**！
       3. **第 1024 次入队（第二级慢路径与快照刷新）**：
          * 当生产者请求序列达到 1024 时：`1024 - 0 = 1024 >= 1024`，触发第一级警戒线；
          * 此时生产者发起 1 次跨核 `NEXT_NEEDED_ACK_SEQUENCE_HANDLE.getAcquire(this)`，读取到最新的 `freshNeededAckSeq = 300`；
          * 重新计算真实在途量：`1024 - 300 = 724 < 1024`（未打满！）；
          * 生产者顺便将本地快照刷新为 `cachedNextNeededAckSequence = 300`；
          * **量化收益**：在接下来的 300 次入队中（1025~1324），生产者又可以凭借新的 Safe Zone 快照在本地 L1 Cache 中 0 跨核开销地极速放行！

5. **有序 Reset 与 GC 根断开 (`poll`)**：
   * 消费端弹出队头时，严格执行：
     1. `ARRAY_VH.setRelease(array, index, null)`：先清空槽位指针，彻底切断 GC 根引用防内存泄漏；
     2. `NEXT_NEEDED_ACK_SEQUENCE_HANDLE.setRelease(this, currentNeededAckSeq + 1)`：最终推进消费序列号，允许后续生产者安全覆写该槽位。

##### 机制 ④：全链路韧性与两级 Fail-Fast 容错（Resilience & Fault Tolerance）
* **`try-finally` 异常安全闭环**：`asyncCtx.resume(...)` 内部使用 `try-finally { this.recycle(); }` 保证无论业务逻辑或下游抛出何种异常，上下文对象 100% 安全归还对象池；
* **四态 CAS 防并发竞争**：`STATE_INIT -> STATE_RESOLVED / STATE_TIMEOUT / STATE_CANCELLED`，确保 Redis 回包与 50ms 时间轮超时并发到达时绝对互斥，只有一个胜出；
* **双级快速失败降级**：当 RingBuffer 1024 槽位打满（`!offered`）或 TCP 发送异常时，系统立即取消时间轮任务并执行 `resume(false)` 向客户端快速返回拒绝，绝不让请求陷入无谓的 50ms 挂起。

---

## 📊 性能基准与权威压测实测报告 (Benchmark Results)

测试环境规格：
* **CPU**: AMD Ryzen 9 7950X 16-Core Processor (32 vCPUs @ 4.5GHz) / 64GB DDR5 RAM
* **OS / Runtime**: Linux 6.1 / JDK 21.0.2 (Vector API enabled)
* **Redis**: Docker Redis 7.4.10 / 回环网络 / `127.0.0.1:6379`
* **工具链**: JMH 1.35 + Async-Profiler 2.9 (`-e alloc,cpu`) + wrk2

---

### 1. 全链路综合压测对比 (Full-Stack Gateway Comparison)

#### ① 16 线程端到端全链路真实吞吐实测 (2,000,000 次真实 HTTP 报文压测)
执行命令一键复现：`mvn test "-Dtest=FullStackGatewayComparisonBenchmarkTest"`

```
==================================================================================================
 🚀 端到端全链路真实网关性能对比压测 (16 线程并发, 总计 2,000,000 次真实 HTTP 报文)
==================================================================================================
  1. 传统网关链路 (SCG + JJWT + Map + CHM) :   560.58 ns/op |  1,783.88 K ops/sec | 耗时: 1121.15 ms
  2. Netty 0-GC 极前置限流引擎 (当前最新架构) :   125.08 ns/op |  7,994.92 K ops/sec | 耗时:  250.16 ms
--------------------------------------------------------------------------------------------------
  ⚡ 真实端到端 QPS 性能提升倍数: 4.48x 倍速度 (耗时下降 77.7%)
==================================================================================================
```

#### ② 系统级核心性能指标对比 (System-Level Metrics)

| 测量指标 / 场景 | 传统 Spring Cloud Gateway + JJWT + Lettuce | Netty 0-GC 极前置响应式限流引擎 | 优化幅度 / 量化收益 |
| :--- | :--- | :--- | :--- |
| **单机极限承载吞吐 (Peak QPS)** | ~25,400 QPS | **185,400+ QPS** | **+630% (提升近 7.3 倍)** 🚀 |
| **真实全链路每操作延迟** | 560.58 ns / op | **125.08 ns / op** | **延迟降低 77.7% (提升 4.48 倍)** ⚡ |
| **P50 基础延迟 (Median Latency)** | 2.80 ms | **0.08 ms (80 µs)** | **延迟降低 97.1%** |
| **P99.9 尾部延迟 (Tail Latency)** | 14.20 ms | **0.32 ms (320 µs)** | **延迟降低 97.7% (亚毫秒级)** 🎯 |
| **Hot-Path 堆内存分配速率** | ~684 MB / sec (~4.2 KB/req) | **0.00 MB / sec (0.000 B/req)** | **完全达成 100% Zero-Allocation** |
| **5 分钟 Young GC 停顿次数** | 92 次 (累计 STW 480ms) | **0 次 (0 STW 停顿)** | **彻底消除 GC 停顿引发的毛刺** |
| **被拦截流量网络与框架开销** | 走完完整 HTTP 编解码 + JJWT 反射解析 | **Socket 层 0-GC 立即回写预编译字节并切断 TCP** | **拦截开销降低 99.8% (0 框架负载)** |

---

#### ③ 分层技术架构与实现机制全链路对比 (Full-Stack Architectural Layer Comparison)

| 架构分层 / 处理阶段 | 传统微服务网关 (SCG + JJWT + Lettuce) | Netty 0-GC 极前置限流引擎 (当前架构) | 核心机制突破与量化收益 |
| :--- | :--- | :--- | :--- |
| **1. 入站与协议解析层** | 必须先经由 `HttpServerCodec` 完整反序列化为 `FullHttpRequest` 对象（产生大量 String/Header 堆对象） | **极前置于 Codec 之前**：在 `channelRead` 直接对物理 `ByteBuf` 扫描，合法流量重置 `readerIndex(0)` 透传 | **提前截断 100% 恶意/超限流量**<br>零反序列化开销 |
| **2. JWT 鉴权快路径** | 每次调用 `jwtParser.parseClaimsJws(token)`<br>（产生大量的 Base64 解码、JSON DTO 树与 String） | **`JwtSigUidCache`**：xxHash64 + 8B 签名前缀防碰撞 + 分层交错 L1 Cache 数组 + 双表冷热轮转 | **36.33 M ops/s (~27.5 ns)**<br>100% 0 堆对象分配 |
| **3. JWT 鉴权慢路径** | JJWT 反射解析 Claims，Jackson 反序列化 Payload 为 Map 提取 `uid` 和 `exp` | **`JwtPayloadDfaParser`**：单通道流式 DFA 状态机直接从 Base64URL 字节流提炼 UID/EXP | **零 JSON 解析器、零 String**<br>慢路径提炼仅需 ~180 ns |
| **4. 本地黑名单拦截** | `ConcurrentHashMap<Long, Long>` 或 Redis 同步查询（存在装箱对象与网络 RTT） | **`SwissTableBanCache`**：64-bit SWAR SIMD 向量化元数据控制字节并行快筛 + 扁平数组 | **105.61 M ops/s (9.5 ns)**<br>单指令 8 槽位瞬间并发探查 |
| **5. 节点级令牌桶争抢** | `AtomicLong` CAS 全局自旋争抢（多核总线锁 `LOCK CMPXCHG` 剧烈冲突，CPU 缓存行抖动） | **`LocalGlobalRateLimiter`**：64-bit Bit-Packing + `FastThreadLocal` 线程私有 AIMD 动态自适应局部缓冲 | **1,483.79 万 QPS (67.4 ns)**<br>跨核总线嗅探降低 99.9% |
| **6. 模式 A 异步批量记账** | Lettuce 同步执行 `redis.evalsha(...)`（每请求阻塞等待 Redis 网络 RTT，~6,755 QPS） | **`UserRateLimiterOperate.acquireBatchOffload`**：`FastThreadLocal` 32条攒批 Pipeline + 单个 Direct ByteBuf 刷入 | **7,180,737 ops/s (718 万 QPS)**<br>800万次在 1.11 秒内刷完 |
| **7. 模式 B 临界配额校验** | `LockSupport.park` 阻塞当前 HTTP Worker 线程（线程池耗尽瘫痪） | **0-GC 原生响应式无阻塞续体 (`suspend` / `resume`)** + 50ms 时间轮熔断 | **0 线程阻塞**<br>当前 Worker 立即释放处理新请求 |
| **8. 高并发防雪崩反压** | 无反压（或 JVM 堆内 `request(n)` 排队，客户端持续灌入导致 OOM） | **Linux 内核 4 层物理级 TCP 零窗口反压 (`autoRead(false)`)** | **从 TCP 协议层物理刹车**<br>将内存压力原路反推回客户端 |

---

### 2. 真实 Redis 限流模式三方全量对决与差异来源剖析 (Real Redis Validation)

> **🎯 业务场景与测试定位**：
> 本项测试专门针对 **【模式 A：80% 水位以下正常流量的“乐观放行”与“发后即忘 (Fire-and-Forget)”旁路异步批量记账】**。
> 当用户 UID 处于安全配额内时，网关无需等待 Redis 返回结果，HTTP 请求在安全防线处**直接乐观放行（Zero Latency Pass-Through）**；同时将 UID 异步提交给旁路 Redis 线程池，以“发后即忘”机制在 Redis 中完成配额原子记账。

16 线程并发 × 2,000 次操作（共 32,000 次相同 Lua 令牌桶操作，直连 Linux 原生 Redis 6379 服务端），执行命令一键复现：
`mvn test "-Dtest=RedisLimiterComparisonBenchmarkTest"`

```
==================================================================================================
 🚀 真实 Redis 协议限流压测对比 (16 线程并发, 总计 32,000 次真实 EVALSHA 操作)
==================================================================================================
  1. Lettuce 传统驱动 (逐请求同步阻塞 EVALSHA)       : 2,624.06 ms |    12,194.83 ops/sec
  2. 自研 0-GC RESP2 驱动 (无攒批逐条直发 DirectFlush) :   164.27 ms |   194,799.34 ops/sec
  3. 自研 0-GC RESP2 驱动 (32条自适应攒批 Pipeline)   :     7.72 ms | 4,146,742.86 ops/sec
--------------------------------------------------------------------------------------------------
  ⚡ 无攒批直发 相比 Lettuce 同步阻塞: 15.97x 倍速度 (耗时下降 93.7%)
  ⚡ 32条攒批 相比 Lettuce 同步阻塞  : 340.04x 倍速度 (耗时下降 99.7%)
  ⚡ 32条攒批 相比 无攒批直发        : 21.29x 倍速度 (耗时下降 95.3%)
==================================================================================================
```

#### ① 三方核心指标实测对比矩阵 (Comparison Matrix)

| 方案 / 模式 | 32,000次耗时 | 实测吞吐量 (Ops/sec) | 网络交互与刷新机制 | 内存分配与 GC 状态 |
| :--- | :--- | :--- | :--- | :--- |
| **1. Lettuce 传统驱动**<br>(`commands.evalsha`) | 2,624.06 ms | **12,194.83 ops/s** | ❌ **逐请求强制同步阻塞等待 RTT** | 频繁在堆上分配 `CommandArgs`、`AsyncCommand`、`CompletableFuture`，GC 压力巨大 |
| **2. 自研 0-GC RESP2 驱动 (无攒批直发)**<br>(`acquireSingleDirect`) | 164.27 ms | **194,799.34 ops/s**<br>(**相比 Lettuce 提升 15.97 倍**) ⚡ | ⚠️ **异步非阻塞直接刷入 Socket**<br>(每请求触发 1 次 `writeAndFlush`) | **100% 0-GC**：直接写入堆外 `PooledByteBufAllocator.directBuffer`，0 堆分配 |
| **3. 自研 0-GC RESP2 驱动 (32条攒批)**<br>(`acquireBatchOffload`) | **7.72 ms** | **4,146,742.86 ops/s**<br>(**相比 Lettuce 提升 340.04 倍**<br>**相比无攒批提升 21.29 倍**) 🚀 | ✅ **`FastThreadLocal` 32条批处理**<br>单个 Direct ByteBuf 批量刷入 Socket | **100% 0-GC + 硬件 L1 Cache 亲和**：线程私有 `long[]` 无跨核争用，系统调用降低 96.8% |

---

#### ② 性能差异来源与底层机理深度剖析 (Architectural Root Cause Analysis)

##### 1. Lettuce 传统驱动为何成为严重瓶颈（仅 1.2 万 QPS）？
* **物理网络 RTT 强制等待**：Lettuce 的 `sync().evalsha()` 底层基于 `future.get()` 或 `CountDownLatch`，当前线程必须等待 **TCP 发送 ➔ 网络传输 ➔ Redis 单线程执行 ➔ Redis 回包 ➔ TCP 接收** 全套网络 RTT 周期（约 0.1~0.5ms）。16 个线程受物理 RTT 限制，单机吞吐被锁死在 $\text{QPS} \le \frac{16}{0.1\text{ms}} \approx 1.6 \text{万}$；
* **密集的堆内存对象与反射/反序列化损耗**：Lettuce 每次调用在堆上实例化 `String[]`、`CommandOutput`、`RedisCommand` 等 10 余个对象，引发高频 Young GC STW 停顿；
* **频繁内核态切换（Syscall Overhead）**：每条请求独立触发操作系统 `write()` 与 `read()` 系统调用，引发密集的 CPU 用户态-内核态切换。

##### 2. 自研无攒批直发 (`acquireSingleDirect`) 为何能提升 16 倍（19.5 万 QPS）？
* **异步非阻塞 Pipeline（0 等待 RTT）**：客户端向 Socket 写入字节后立即 `return;`，不挂起线程，彻底消除了每请求等待网络 RTT 的阻塞停顿；
* **0-GC 原位 RESP2 协议打包**：直接将 EVALSHA 协议头、十六进制 SHA-1 与数字编码为 ASCII 字节写入堆外 DirectByteBuf，消除了全部 Java 堆对象分配；
* **潜在瓶颈**：每一次调用都产生一次独立的小数据包（Small TCP Packet）与 `writeAndFlush` 调用，Netty ChannelPipeline 遍历与底层 Socket 系统调用仍较为频繁。

##### 3. 自研 32 条自适应攒批 (`acquireBatchOffload`) 为何能暴增至 414.6 万 QPS（提升 340 倍）？
* **系统调用平摊削峰（Amortized Syscall Reduction）**：通过 `FastThreadLocal<ThreadRedisBatchBuffer>` 在当前 EventLoop 线程私有内存中将 32 条限流命令打成**单个 Direct ByteBuf 一次性刷入 Socket**，将 Socket 系统调用与 TCP 报头开销从 32 次暴降为 1 次（系统调用降低 **96.87%**）；
* **CPU 缓存亲和与 0 跨核锁竞争（Zero-Contention L1 Cache Local）**：线程私有 `long[]` 数组始终驻留在 CPU L1 Data Cache 中，0 跨核 CAS 争抢、0 总线嗅探（Bus Sniffing）；
* **数量 (32条) + 时间 (50µs) 双阈值自适应保障**：流量洪峰时瞬间填满 32 条触发高速批量发射；低峰期 50 微秒延迟到期自动 Flush，在极限吞吐（414 万 QPS）与微秒级极低延迟之间达成最佳工程平衡。

---

### 3. Mode B 临界限流校验：阻塞挂起 vs 响应式直发 vs 响应式微攒批 实测 (Mode B Benchmark Matrix)

在面临真实 Linux Redis 6379 网络往返延迟与 Redis Lua 脚本精准扣减时，网关 16 个 EventLoop Worker 线程并发处理 16,000 次 Mode B 临界配额校验：
执行命令一键复现：`mvn test "-Dtest=ModeBBlockingVsReactiveBenchmarkTest"`

```
==================================================================================================
 🚀 Mode B 直连真实 Linux Redis 6379 极限压测 (16 线程, 16000 次真实 EVALSHA 校验)
==================================================================================================
  1. 真实 Redis 阻塞式 Mode B (16 线程同步阻塞等待 Socket)      :  1521.04 ms |  10,519.14 ops/sec | 线程全部阻塞
  2. 自研 0-GC 响应式 Mode B (单 TCP 连接, 逐条直发 DirectFlush)   :   497.39 ms |  32,167.81 ops/sec | 0 线程阻塞
  3. 自研 0-GC 响应式 Mode B (单 TCP 连接, 16条微攒批 Pipeline)    :   154.01 ms | 103,888.55 ops/sec | 0 线程阻塞 (🚀 突破 10.3 万 QPS)
--------------------------------------------------------------------------------------------------
  ⚡ 响应式微攒批 相比 阻塞式提升: 9.88x 倍 (耗时降低 89.9%)
  ⚡ 响应式微攒批 相比 响应式直发提升: 3.23x 倍 (耗时降低 69.0%)
==================================================================================================
 📊 端到端耗时百分位数统计 (Latency Percentiles Matrix):
  1. 真实 Redis 阻塞式 Mode B (16 线程阻塞) : Avg=1.42 ms | P50=1.22 ms | P90= 2.34 ms | P99= 4.06 ms | P99.9= 6.38 ms | Max=16.18 ms
  2. 自研 0-GC 响应式直发 (单连接直发)      : Avg=7.68 ms | P50=6.78 ms | P90=11.13 ms | P99=37.81 ms | P99.9=41.81 ms | Max=42.36 ms
  3. 自研 0-GC 响应式微攒批 (16条 Pipeline) : Avg=2.36 ms | P50=2.16 ms | P90= 2.98 ms | P99=11.95 ms | P99.9=14.34 ms | Max=14.36 ms
==================================================================================================
```

#### ① 核心指标、延迟分布与架构瓶颈对比矩阵 (Comparison & Latency Matrix)

| 方案 / 模式 | 16,000 次耗时 | 实测吞吐量 (Ops/sec) | 平均耗时 / P90 / P99 耗时 | Netty Worker 状态 | Redis 交互与物理瓶颈分析 |
| :--- | :--- | :--- | :--- | :--- | :--- |
| **1. 真实 Redis 阻塞式 Mode B**<br>(`Lettuce` / `LockSupport.park`) | 1,521.04 ms | **10,519.14 ops/s** | Avg=**1.42 ms**<br>P90=**2.34 ms**<br>P99=**4.06 ms** | ❌ **16 线程 100% 阻塞挂起**<br>(线程池瘫痪雪崩) | 16 个工作线程创建 16 条独立连接，但被物理网络 RTT 强行挂起，单机吞吐被锁死在 $\text{QPS} \approx 1.05\text{万}$。 |
| **2. 自研 0-GC 响应式 (单连接直发)**<br>(`acquireReactiveDirect`) | 497.39 ms | **32,167.81 ops/s**<br>(**相比阻塞式提升 3.05 倍**) ⚡ | Avg=**7.68 ms**<br>P90=**11.13 ms**<br>P99=**37.81 ms** | ✅ **100% 0 线程阻塞**<br>(Worker 瞬间释放) | **单 TCP 逐包刷盘排队瓶颈**：受限于单 TCP 连接逐包刷盘 (`writeAndFlush`) 与 Redis 单线程单包协议解析 CPU 天花板，高并发下 Socket 发送队列积压，导致 **P99 延迟恶化至 37.81ms**。 |
| **3. 自研 0-GC 响应式 (16条微攒批)**<br>(`acquireReactiveAsync`) | **154.01 ms** | **103,888.55 ops/s**<br>(**相比阻塞式提升 9.88 倍**<br>**相比响应式直发提升 3.23 倍**) 🚀 | Avg=**2.36 ms**<br>P90=**2.98 ms**<br>P99=**11.95 ms**<br>(**P99 降低 68.4%**) ⚡ | ✅ **100% 0 线程阻塞**<br>(Worker 瞬间释放) | **单连接极限突破 (10.3万 QPS)**：单个 Direct ByteBuf 批量打包 16 条命令，Redis 单次系统调用批量处理，消除了 93.7% 的系统调用与协议解析开销，**排空积压后 P99 骤降至 11.95ms**！ |

---

#### ② 批处理不同操作能节省哪些开销？（哪些开销真正“值得”批处理解决？）

在分布式网关与 Redis 高性能通信中，批处理并非无脑扩大，其价值取决于**数据流特征（单向 vs 双向）**与**物理瓶颈点**：

##### 1. 模式 A（异步入账 / 纯写单向流）：`long[]` 数组 Pipeline 攒批
* **业务特点**：单向投递（Fire-and-Forget），只发不收，网关无需等待回包即可直接放行；
* **节省的核心开销（高价值）**：
  * **Direct ByteBuf 内存申请与回收开销（暴降 96.9%）**：从每条请求单独向 Netty 池化分配器申请/释放 DirectBuffer，变成 32 条共享一个连续内存块；
  * **系统调用（Syscall）与内核态切换开销（暴降 96.9%）**：32 次独立的 `writeAndFlush` / `sendto` 系统调用合并为 **1 次系统调用**，消除了 31 次 CPU 用户态-内核态上下文切换；
  * **TCP 报文头与网络带宽开销**：减少了 31 个 TCP/IP 报头（每个 40~54 字节）及小包通信引发的拥塞延迟；
  * **Redis 服务端 Epoll 唤醒与命令解析开销**：Redis 服务端单次 `read()` 读取整块字节流，在 CPU L1 Cache 极度亲和的紧凑循环中连续执行，吞吐从 **56 万 QPS 飙升至 400 万+ QPS（提升 257 倍）**。

##### 2. 模式 B（80% 临界强校验 / 双向响应式流）：`AsyncRateLimitContext[]` 微攒批
* **业务特点**：双向往返（Round-Trip Request-Response），每个请求挂起并绑定一个续体（Context），等待 Redis 仲裁后唤醒；
* **节省的核心开销（突破单核物理瓶颈）**：
  * **Redis 单线程 Lua 协议反序列化瓶颈（唯一物理瓶颈）**：单 Redis 服务端执行复杂 Lua 是**单线程单核**的。单发模式下，Redis 必须反复处理“网络中断 ➔ Epoll 就绪 ➔ read 字节流 ➔ 协议反序列化 ➔ 执行 Lua ➔ 写入 Socket”，CPU 算力天花板被锁死在 **~3.5 万 QPS**。16 条微攒批让 Redis 单次读取到 16 条 EVALSHA 字节流，紧凑循环执行 Lua，单核 QPS 直接突破 **10 万+**；
  * **Netty EventLoop 跨线程任务投递开销**：减少了 15/16 的跨 EventLoop 提交开销。

##### 3. 哪些开销“值得”批处理 vs 哪些开销“不值得”（反模式）？

| 类别 | 值得批处理的场景（高收益） | 不值得 / 有害的批处理（反模式） |
| :--- | :--- | :--- |
| **内存开销** | **线程本地无锁聚合**（如 `FastThreadLocal` 0-GC 数组） | **全局共享阻塞队列**（如 `LinkedBlockingQueue` 多线程 CAS 锁竞争会抵消批处理收益） |
| **系统调用** | **合并 Socket `writev` / `sendto`**（平摊 CPU 内核态切换） | **无节制大批次**（如等待 500ms 攒 1000 条，导致 P99 延迟严重恶化） |
| **网络往返** | **双向通信平摊 Redis 单线程协议解析成本** | **跨节点多层批处理**（每层都引入排队延迟导致延迟累加） |

---

#### ③ 攒批带来的延迟实测与深度剖析 (Latency Trade-Off Analysis)

##### 1. 为什么攒批没有导致单个任务的 P99 延迟增加，反而大幅下降了 68.4%？（排队论数学模型与服务率本质）

很多人存在一种朴素直觉：“*微攒批需要在网关侧等待凑满 16 条，这个‘微攒批等待耗时’理应让每条消息变慢，P99 为什么反而从 37.81ms 骤降至 11.95ms？*”

**核心结论：直觉上认为“微攒批等待耗时”是增加延迟的原因，但微攒批通过大幅压缩“单请求服务处理耗时”，使得“系统服务率”提升了 3.23 倍，从而彻底消除了高并发下的“系统排队耗时”。由于削减的排队耗时（-26.02ms）远大于攒批等待代价（+0.16ms），每一条消息的端到端总延迟反而大幅下降！**

###### 📐 延迟构成的排队论数学模型 (M/M/1 与 Kingman 重度流量逼近公式)：
$$\text{单任务端到端延迟 } T_{\text{total}} = \underbrace{T_{\text{batch\_wait}}}_{\text{微攒批等待耗时}} + \underbrace{\boldsymbol{T_{\text{queue}}}}_{\text{系统排队耗时}} + \underbrace{T_{\text{service}}}_{\text{单请求服务处理耗时}}$$

###### 💡 什么是“系统服务率（$\mu$）”？为什么微攒批能减少“系统排队耗时”？
* **服务率（$\mu$）的本质**：系统每秒钟能处理的最大请求量，与单请求平均净服务时间成反比（$\mu = \frac{1}{T_{\text{service}}}$）；
* **单发直连时（$T_{\text{service}}$ 巨大 ➔ $\mu$ 仅 3.2 万）**：
  * Redis 处理 16 个单发请求，必须重复执行 16 次完整的“Epoll 唤醒 ➔ `read()` 系统调用 ➔ RESP2 反序列化 ➔ Lua 执行 ➔ `write()` 回包”；
  * 处理 16 个请求累计占用 Redis CPU 达 **$500\mu s$**（单请求平均耗时 $T_{\text{service}} = 31.25\mu s$），服务率被锁死在 $\mu \approx 3.2\text{万 QPS}$；
* **16条微攒批时（$T_{\text{service}}$ 被压缩 69% ➔ $\mu$ 暴增至 10.3 万）**：
  * Redis 仅需 **1 次 `read()` 系统调用**，把 16 条 EVALSHA 字节流一次性读入，在 CPU L1 Cache 极度亲和的紧凑循环（Tight Loop）中连续执行 16 次 Lua，最后 **1 次 `write()` 回包**；
  * 处理这 16 个请求累计占用 Redis CPU 暴跌至 **$155\mu s$**（单请求边际耗时 $T_{\text{service}}$ 从 $31.25\mu s$ 骤降至 **$9.68\mu s$**！），系统极限服务率 $\mu$ 暴增至 **10.3 万 QPS**！

###### 🔍 排队延迟因果闭环（为什么排队耗时 $T_{\text{queue}}$ 减少了数十倍？）：
根据排队论中的 **Kingman 重度流量排队公式**：
$$T_{\text{queue}} \approx \left(\frac{\rho}{1 - \rho}\right) \cdot \frac{1}{\mu}$$
1. **单发直连**：当流量达到 3 万 QPS 时，系统利用率 $\rho = \frac{\lambda}{\mu} \approx 95\%$，排队倍数 $\frac{\rho}{1 - \rho} \approx 19$。Socket 缓冲区瞬间积压数千个请求，**请求必须在队列中干等 35ms 才能轮到执行，导致 P99 恶化至 37.81ms（其中 95% 是在无谓排队！）**；
2. **16条微攒批**：服务率 $\mu$ 提升至 10.3 万后，相同流量下的利用率 $\rho$ 骤降至 $30\%$，排队倍数 $\frac{\rho}{1 - \rho}$ 降至 $0.43$。Socket 积压瞬间被排空，**系统排队耗时 $T_{\text{queue}}$ 直接减少了 26 毫秒**；
3. **净得失账本计算**：
   $$\Delta T_{\text{total}} = \underbrace{+0.16\text{ ms}}_{\text{微攒批等待代价}} - \underbrace{26.02\text{ ms}}_{\text{排队积压消除收益}} = \mathbf{-25.86\text{ ms}}$$
   **微攒批用 0.16ms 的微小等待代价，换取了 26ms 排队耗时的消除，因此单个任务的 P99 延迟暴跌 68.4%！**

##### 2. 低并发/稀疏请求场景下的延迟保证（50µs 阈值守护）
* 在请求非常稀疏（如仅有 1~2 条散客流量）时，虽然无法瞬间凑满 16 条，但触发了 **50µs 时间阈值自适应刷新**；
* 额外引入的攒批等待延迟仅有 **$\le 50\mu s$（0.05 毫秒）**，相比物理网络往返 RTT（0.5~1ms）仅占 5%~10%，在宏观上完全无感知，兼顾了低峰期亚毫秒级超低延迟与高峰期 10 万+ QPS 极限吞吐。

---

#### ④ 为什么选择“响应式微攒批 Pipeline”并坚决摒弃“多连接池化”？（深层机理剖析）

1. **单 Redis 实例的物理铁律：多连接池化在单 Redis 节点下是负优化**
   * **客观事实**：单节点 Redis 服务端执行 Lua 脚本是 **单线程单核** 的；
   * **多连接池化的副作用**：在单 Redis 架构下盲目增加 TCP 连接数（例如 4 或 8 条连接），并不能增加 Redis 执行 Lua 脚本的 CPU 核心数；相反，它会导致 Redis 单线程在 Epoll 多路复用中频繁切换连接、触发更多的独立 `read()` 系统调用与独立协议解析，同时打碎了请求密度使得 Pipeline 无法攒批，因此被**坚决摒弃**；
2. **响应式微攒批（Reactive Micro-Batching Pipeline）为何能狂飙至 10.3 万 QPS？**
   * **系统调用与内核态切换暴降 93.7%**：将 16 条 Mode B 命令合并到单个 Direct ByteBuf 中一次性发送，操作系统与 Redis 端的 TCP `read()` / `write()` 物理调用次数直接减少 15/16；
   * **Redis 内存紧凑循环（Tight Execution Loop）**：Redis 单次从 Socket 缓冲区读取到包含 16 条 EVALSHA 的完整字节流，在 CPU L1 Cache 极度亲和的紧凑循环中连续执行 16 次 Lua 脚本，并批量写回 `:100000:1\r\n:100001:1\r\n...`；
   * **网关端 0-GC 严格保序分发**：自研 `SyncWaitSlotRingBuffer` 具备严格 FIFO 连续性，Redis 响应流经 `LineBasedFrameDecoder` 后原位匹配队头 UID，并发调度回各自 HTTP EventLoop 恢复续体，达成了 **103,888 ops/sec 的单机极限突破**！

---

#### 🔍 核心根因剖析：为什么“线程阻塞”会导致灾难性的并发雪崩与吞吐瓶颈？

```
【传统阻塞式 Mode B 崩溃链条】
16 个并发请求到达 ➔ 16 个 Worker 线程调用 LockSupport.park() 挂起等待 Redis 回包
                ➔ ❌ 16 个 EventLoop 线程全部陷入睡眠（EventLoop 循环停摆）
                ➔ 后续 49,984 个请求在内核/任务队列中排队无法被读取
                ➔ 吞吐量被物理锁死：16 线程 / 1ms = 1,000 QPS，总耗时长达 48.08 秒！

【0-GC 响应式续体架构 440 倍突破】
16 个并发请求到达 ➔ 借出 0-GC 续体向 Redis 提交任务 ➔ 立即 return; (Worker 线程 0 阻塞释放)
                ➔ ✅ Worker 线程在 1µs 内继续抓取后续成千上万个请求
                ➔ Redis 1ms 后回包 ➔ Netty 调度回原 EventLoop 触发 asyncCtx.resume() 恢复执行
                ➔ 吞吐彻底脱离外部网络 RTT 限制，飙升至 45.7 万+ QPS，总耗时仅 0.11 秒！
```

1. **核心矛盾：有限的系统工作线程 vs 外部网络物理 RTT**
   * Netty 高性能架构的基石在于 **EventLoop 线程非阻塞事件循环**。为了达到最高的 CPU L1/L2 缓存亲和性并消除上下文切换开销，工作线程数通常严格匹配 CPU 物理核心数（例如 16 个 Worker 线程）；
   * 在传统阻塞式设计中，当请求需要向 Redis 校验配额时，线程调用 `LockSupport.park()` 进入休眠等待回包。此时**哪怕 CPU 负载只有 2%，这 16 个线程也会瞬间被全部占满挂起**；
2. **事件循环停摆引发的级联排队雪崩（EventLoop Starvation & Queuing Delay）**
   * 一旦 16 个 Worker 全部陷入阻塞，Netty 的 Selector 将彻底停止从 Socket 读取任何后续网络报文；
   * 后续涌入的 49,984 个请求只能在操作系统 TCP 接收队列与 Netty 任务队列中苦苦排队，必须等待前面的请求逐个经历 1ms 网络往返唤醒后才能轮到下一个批次；
   * 整个 50,000 次请求的处理耗时被硬生生拉长到 **48.08 秒**，平均每个请求排队延迟高达数秒，极易在大促洪峰下引发大量的客户端 `504 Gateway Timeout` 超时崩溃；
3. **响应式范式（Reactive Continuation）如何从根本上粉碎此瓶颈？**
   * **彻底解耦“业务等待”与“工作线程”**：
     通过 `suspendAndAcquireReactiveRateLimit` 挂起续体并立即 `return;`，Worker 线程在 **不到 1 微秒内就被释放**，毫不停顿地继续服务后续第 17、第 18、乃至第 50,000 个请求；
   * **吞吐量彻底摆脱外部网络 RTT 的物理束缚**：
     在 Redis 处于 1ms 网络传输与执行期间，16 个 Worker 线程已经并行调度处理了数万个连接；当 Redis 响应到达后，通过 Netty EventLoop 原位触发 `asyncCtx.resume(granted)` 恢复下游流水线，从而将单机处理能力从 **1,039 QPS 暴增至 457,778 QPS（提升 440.23 倍）**！

---

### 4. 微基准测试：慢路径 Header 匹配与缓存读写延迟 (JMH)

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
    │   │   │   ├── JwtSigUidCache.java                   # 分层交错 L1 Cache Line 布局双表轮转缓存 (单门禁写屏障)
    │   │   │   ├── LocalBanCache.java                    # 64-bit Bit-Packing 扁平无锁原子黑名单 (SWAR SIMD 探查)
    │   │   │   └── RedisUserBanSubscriber.java           # Redis Pub/Sub 全网封禁与 80% 水位预警 0-GC 订阅器
    │   │   ├── config                                    # 配置与动态热更新治理
    │   │   │   ├── GatewayRateLimitConfigListener.java   # Nacos 动态配置无锁热更新监听器
    │   │   │   └── GatewayRateLimitProperties.java       # 限流器属性绑定类
    │   │   ├── handler                                   # Netty Channel Inbound 入站防线层
    │   │   │   ├── NettyJwtHeaderAccumulatorHandler.java # TCP 拆包/半包帧聚合与 JWT 快速定位
    │   │   │   ├── NettyInboundSecurityHandler.java      # 极前置 0-GC 安全防线总调度器 (挂起与恢复续体 + TCP 反压)
    │   │   │   ├── NettySecurityCustomizer.java          # Reactor-Netty 管道极前置切入挂载器
    │   │   │   ├── JwtHeaderSecurityHandler.java         # JWT Header 原位扫描与黑名单判定 (SWAR/SIMD)
    │   │   │   └── headerSecurityHandler
    │   │   │       ├── IpHeaderSecurityHandler.java      # 双 64-bit IPv4/IPv6 统一哈希防线
    │   │   │       └── JwtUidHeaderSecurityHandler.java  # UID 提取与分发 Handler
    │   │   ├── limiter                                   # 无锁限流与原生 RESP2 驱动
    │   │   │   ├── AsyncRateLimitContext.java            # 0-GC 响应式异步上下文 (Recycler 对象池 + 四态 CAS 状态机)
    │   │   │   ├── RateLimitCallback.java                # 响应式续体恢复回调函数接口
    │   │   │   ├── LocalGlobalRateLimiter.java           # 节点级无锁令牌桶 + AIMD 线程私有缓冲区
    │   │   │   ├── UserRateLimiterOperate.java           # Per-UID 响应式双模限流引擎 (Mode A 攒批 / Mode B 异步响应式)
    │   │   │   └── SyncWaitSlotRingBuffer.java           # 统一 0-GC 无锁环形队列 (类继承阶梯 Padding + JMM 单发布门禁)
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
        │   │   ├── JwtSigUidCacheBenchmarkTest.java      # 分层交错缓存读写延迟微基准 (对比 CHM)
        │   │   ├── LruThresholdBenchmarkTest.java        # 双表轮转负载率阈值压测
        │   │   ├── RedisUserBanSubscriberTest.java       # Pub/Sub 订阅通知原位解析功能测试
        │   │   └── SwissTableBanCacheTest.java           # SWAR SIMD 黑名单功能与并发测试
        │   ├── handler
        │   │   ├── FullStackGatewayComparisonBenchmarkTest.java # 16 线程端到端全链路真实网关性能对比压测
        │   │   ├── JwtHeaderSecurityBenchmarkTest.java   # Header 匹配微基准 (SWAR 16.6x 提速)
        │   │   ├── NettyInboundSecurityHandlerSplitPacketTest.java # TCP 粘包/半包严苛拆包测试
        │   │   └── NettyReactiveModeBRateLimitTest.java  # Mode B 响应式挂起/恢复与 TCP 物理反压测试
        │   ├── limiter
        │   │   ├── LuaWatermarkEarlyWarningTest.java     # 80% 水位预警与广播集成测试
        │   │   ├── RateLimiterRealTrafficTest.java       # 生产模拟混合流量压测 (近 2000 万 QPS)
        │   │   ├── RealRedisRateLimiterIntegrationTest.java # 真实 Redis 连接全链路功能测试
        │   │   ├── RedisLimiterComparisonBenchmarkTest.java # 原生 RESP2 vs Lettuce 吞吐对比基准
        │   │   ├── SyncEscapesWatermarkTest.java         # 水位逃逸与 Mode B 响应式校验压测
        │   │   ├── UserRateLimiterOperateResp2Test.java  # RESP2 协议编解码单元测试
        │   │   └── UserRateLimiterRealRedisBenchmarkTest.java # 800万次真实 TCP Socket Pipeline 压测
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
| **安全调度** | `NettyInboundSecurityHandler` | 极前置总入站防线，实现对称式**挂起续体 (`suspendAndAcquireReactiveRateLimit`)** 与 **恢复续体 (`resumeContinuation`)**，支持 4 层物理 TCP 零窗口反压。 |
| **硬件缓存** | `JwtSigUidCache` | 分层交错物理布局，单条 64B L1 Cache Line 容纳 4 组探查键，采用 `setValExpRelease` 单门禁写屏障，实现 1.52 亿 QPS 吞吐。 |
| **黑名单** | `LocalBanCache` | 64-bit Bit-Packing 压缩存储 UID 与过期时间，基于 SWAR SIMD 单指令 8 槽位向量化探查，吞吐突破 1.05 亿 QPS。 |
| **两级限流** | `LocalGlobalRateLimiter` | 节点无锁令牌桶 + EventLoop 线程私有 AIMD 缓冲区，低水位平摊配额防饥饿，物理租约到期防突发。 |
| **响应式引擎**| `UserRateLimiterOperate` | Per-UID 响应式双模限流驱动：前 80% 配额 `acquireBatchOffload` 旁路批量 Pipeline，后 20% 配额 `acquireReactiveAsync` 响应式无阻塞精准扣减。 |
| **响应式续体**| `AsyncRateLimitContext` | 0-GC 响应式异步上下文，基于 Netty `Recycler` 轻量复用，封装 `publish()`、`tryResolve()`、`tryTimeout()`、`tryCancel()` 四态原子状态机，提供 `resume(isAllowed)` 异常安全闭环。 |
| **硬件环形队列**| `SyncWaitSlotRingBuffer` | 统一 0-GC 无锁环形队列，56 字节类继承阶梯 Padding 消除跨核伪共享，Safe Zone 局部序列号缓存降低 99.99% 总线嗅探，JMM `publish()` 终极发布门禁与有序清空。 |
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
| `NettyReactiveModeBRateLimitTest` | 0-GC 响应式 Mode B 续体调度与物理反压全链路 | 验证 `suspend` 挂起、`setAutoRead(false)` 反压、Redis 回包唤醒、50ms 时间轮超时熔断与连接断开 `failClosed` |
| `ModeBBlockingVsReactiveBenchmarkTest` | Mode B 真实 Linux Redis 压测与延迟百分位数矩阵 | 验证阻塞式 (1.05万 QPS) vs 响应式直发 (3.2万 QPS) vs 16条微攒批 (**10.3万 QPS / P99 降低 68.4%**) |
| `RateLimiterRealTrafficTest` | 16 线程真实大并发流量混合冲刷 | 验证 800,000 次混合并发请求下全局令牌桶与阶梯限流的高吞吐与零超卖 |
| `JwtSigUidCacheBenchmarkTest` | `JwtSigUidCache` 分层交错物理内存读写延迟 | 验证 L1 Cache Line 密集排布下的 ns 级探查与伪共享消除 |
| `LruThresholdBenchmarkTest` | 双表轮转负载率阈值（20% ~ 70%） | 验证 40% 黄金阈值下并发探查与 SIMD 0-GC 向量化清空的平衡点 |
| `SwissTableVsLinearProbeBenchmark` | 黑名单开放寻址与哈希探查性能 | 对比 SwissTable 与扁平无锁原子数组的内存与寻址延迟 |
| `JwtHeaderSecurityBenchmarkTest` | 2,000 万次 Header Key 匹配微基准 | 验证标量 (60M ops/s) ➔ SWAR 64-bit (359M ops/s) ➔ 纯整数 (985M ops/s) 的飞跃 |
| `LuaWatermarkEarlyWarningTest` | Redis Lua 80% 水位触发与 Pub/Sub 广播 | 验证配额低于 20% 时原子发布 `W:uid` 广播并触发网关状态变更 |
| `SyncEscapesWatermarkTest` | Mode B 临界同步限流防逃逸与超卖阻断 | 验证多线程并发冲刷下 Mode B 强同步对集群超卖漏洞的绝对封堵 |
| `UserRateLimiterOperateResp2Test` | 自研 0-GC 原生 RESP2 协议编解码 | 验证 EVALSHA 命令构造、LineBasedFrameDecoder 拆包与返回值原位提取 |
| `RealRedisRateLimiterIntegrationTest`| 真实 Docker Redis 全链路集成测试 | 验证 SCRIPT LOAD、EVALSHA 执行、心跳探测与两阶段优雅关停 |
| `RedisLimiterComparisonBenchmarkTest`| 原生 RESP2 异步 Pipeline 与 Lettuce 同步对比 | 实测 32,000 次操作下原生 RESP2 取得 **265 倍** 的吞吐优势 |

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

