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
         2. **更新前缀 (Step 2)**：执行 `setPrefixRelease(idx, P_New)` 安全替换 8 字节前缀；
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

