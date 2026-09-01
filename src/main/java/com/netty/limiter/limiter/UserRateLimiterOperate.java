package com.netty.limiter.limiter;

import com.netty.limiter.cache.LocalBanCache;
import com.netty.limiter.config.GatewayRateLimitProperties;
import com.netty.limiter.util.ZeroGcNumberUtil;
import io.netty.bootstrap.Bootstrap;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.PooledByteBufAllocator;
import io.netty.buffer.Unpooled;
import io.netty.channel.Channel;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelOption;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.SimpleChannelInboundHandler;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioSocketChannel;
import io.netty.handler.timeout.IdleStateEvent;
import io.netty.handler.timeout.IdleStateHandler;
import io.netty.util.HashedWheelTimer;
import io.netty.util.concurrent.FastThreadLocal;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import javax.annotation.PostConstruct;
import javax.annotation.PreDestroy;
import java.lang.invoke.MethodHandles;
import java.lang.invoke.VarHandle;
import java.nio.charset.StandardCharsets;
import java.util.ArrayDeque;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * @description: Per-UID 0-GC 异步 Predixy/Redis RESP2 原生驱动组件 (高可用健壮重连与心跳探测版)
 **/
@Slf4j
@Component
public class UserRateLimiterOperate {

    @Autowired
    private GatewayRateLimitProperties properties;

    @Autowired
    private LocalBanCache localBanCache;

    private static final byte[] PING_CMD_BYTES = "*1\r\n$4\r\nPING\r\n".getBytes(StandardCharsets.US_ASCII);
    private static final byte[] EVALSHA_CMD_PREFIX = "*9\r\n$7\r\nEVALSHA\r\n".getBytes(StandardCharsets.US_ASCII);
    private static final byte[] SCRIPT_LOAD_PREFIX = "*3\r\n$6\r\nSCRIPT\r\n$4\r\nLOAD\r\n".getBytes(StandardCharsets.US_ASCII);
    private static final byte[] CRLF = {'\r', '\n'};

    private final byte[] luaShaBytes = com.netty.limiter.util.LuaSha1Util.DEFAULT_LUA_SHA1_BYTES;

    public byte[] getLuaShaBytes() {
        return luaShaBytes;
    }

    public EventLoopGroup getEventLoopGroup() {
        return eventLoopGroup;
    }

    private volatile Channel redisChannel;
    private EventLoopGroup eventLoopGroup;
    private final AtomicBoolean reconnecting = new AtomicBoolean(false);
    private volatile int reconnectAttempts = 0;

    // =========================================================================================
    // =========================================================================================
    // 🚀 模式 A：FastThreadLocal 线程本地 0-GC long[] 攒批缓冲区 (0 跨核、0 CAS 锁开销)
    // =========================================================================================
    private static final int BATCH_SIZE = 32;
    private static final long BATCH_TIMEOUT_NANOS = 50_000L; // 50微秒自适应刷新

    private static class ThreadRedisBatchBuffer {
        final long[] uids = new long[64]; // 容量 64 long 数组
        int count = 0;
        long lastFlushNanos = System.nanoTime();
    }

    private final FastThreadLocal<ThreadRedisBatchBuffer> THREAD_REDIS_BATCH_BUFFER = new FastThreadLocal<ThreadRedisBatchBuffer>() {
        @Override
        protected ThreadRedisBatchBuffer initialValue() {
            return new ThreadRedisBatchBuffer();
        }

        @Override
        protected void onRemoval(ThreadRedisBatchBuffer buffer) throws Exception {
            if (buffer != null && buffer.count > 0) {
                flushLongArrayPipeline(buffer.uids, buffer.count, luaShaBytes);
                buffer.count = 0;
            }
            FastThreadLocal.removeAll();
        }
    };

    // =========================================================================================
    // 🚀 模式 B：FastThreadLocal 线程本地 AsyncRateLimitContext[] 响应式双阈值微攒批缓冲区
    // (数量 16条 + 时间 50µs 双阈值自适应触发，0 跨核锁竞争、0 堆对象分配)
    // =========================================================================================
    private static final int REACTIVE_BATCH_SIZE = 16;
    private static final long REACTIVE_BATCH_TIMEOUT_NANOS = 50_000L; // 50微秒自适应刷新

    private static class ThreadReactiveBatchBuffer {
        final AsyncRateLimitContext[] contexts = new AsyncRateLimitContext[32];
        int count = 0;
        long lastFlushNanos = System.nanoTime();
    }

    private final FastThreadLocal<ThreadReactiveBatchBuffer> THREAD_REACTIVE_BATCH_BUFFER = new FastThreadLocal<ThreadReactiveBatchBuffer>() {
        @Override
        protected ThreadReactiveBatchBuffer initialValue() {
            return new ThreadReactiveBatchBuffer();
        }

        @Override
        protected void onRemoval(ThreadReactiveBatchBuffer buffer) throws Exception {
            if (buffer != null && buffer.count > 0) {
                acquireReactiveBatchAsync(buffer.contexts, buffer.count, luaShaBytes);
                buffer.count = 0;
            }
            FastThreadLocal.removeAll();
        }
    };

    private static final HashedWheelTimer HASHED_WHEEL_TIMER = new HashedWheelTimer(10, TimeUnit.MILLISECONDS);

    @PostConstruct
    public void init() {
        eventLoopGroup = new NioEventLoopGroup(1);
        connectToRedisAsync();
    }

    @PreDestroy
    public void destroy() {
        try {
            // 0. 停止时间轮
            HASHED_WHEEL_TIMER.stop();
            // 1. 优先优雅关闭 Redis 驱动 EventLoop 线程池
            if (eventLoopGroup != null) {
                eventLoopGroup.shutdownGracefully().syncUninterruptibly();
            }
            // 2. 两阶段关闭 (Two-Phase Close)：先清空在途 Socket 发送缓冲区，再发送 TCP FIN 包关闭通道
            if (redisChannel != null && redisChannel.isActive()) {
                redisChannel.writeAndFlush(io.netty.buffer.Unpooled.EMPTY_BUFFER).syncUninterruptibly();
                redisChannel.close().syncUninterruptibly();
            }
            log.info("Successfully shutdown 0-GC RESP2 driver event loop.");
        } catch (Exception e) {
            log.error("Error shutting down 0-GC RESP2 driver", e);
        }
    }

    private synchronized void connectToRedisAsync() {
        if (redisChannel != null && redisChannel.isActive()) {
            return;
        }

        try {
            Bootstrap bootstrap = new Bootstrap();
            bootstrap.group(eventLoopGroup)
                    .channel(NioSocketChannel.class)
                    .option(ChannelOption.TCP_NODELAY, true)
                    .option(ChannelOption.SO_KEEPALIVE, true)
                    .handler(new ChannelInitializer<SocketChannel>() {
                        @Override
                        protected void initChannel(SocketChannel ch) {
                            // 1. 30秒未写数据自动触发心跳保活检测，防止 TCP 半开假死 (Half-Open Connection)
                            ch.pipeline().addLast(new IdleStateHandler(0, 30, 0, TimeUnit.SECONDS));
                            // 2. 0-GC 粘包/半包帧聚合拆包器 (LineBasedFrameDecoder 按 \r\n 拆分完备 RESP2 响应)
                            ch.pipeline().addLast(new io.netty.handler.codec.LineBasedFrameDecoder(1024));
                            ch.pipeline().addLast(new SimpleChannelInboundHandler<ByteBuf>() {
                                @Override
                                protected void channelRead0(ChannelHandlerContext ctx, ByteBuf msg) {
                                    int start = msg.readerIndex();
                                    int end = msg.writerIndex();
                                    if (start >= end) return;

                                    // 🛡️ Redis 错误报文快速失败处理 (如 -NOSCRIPT, -ERR 等)
                                    if (msg.getByte(start) == '-') {
                                        log.error("Redis returned error in Mode B: {}", msg.toString(java.nio.charset.StandardCharsets.UTF_8));
                                        AsyncRateLimitContext errCtx = syncWaitSlotRingBuffer.poll();
                                        if (errCtx != null && errCtx != SyncWaitSlotRingBuffer.CANCELLED_CONTEXT) {
                                            if (errCtx.tryCancel()) {
                                                if (errCtx.timeoutHandle != null) errCtx.timeoutHandle.cancel();
                                                errCtx.httpCtx.executor().execute(() -> errCtx.resume(false));
                                            }
                                        }
                                        return;
                                    }

                                    int colonIdx = msg.indexOf(start, end, (byte) ':');
                                    if (colonIdx < 0 || colonIdx >= end - 1) {
                                        return;
                                    }
                                    long uidFromRedis = ZeroGcNumberUtil.parseLongFromByteBuf(msg, start, colonIdx);
                                    byte statusByte = msg.getByte(colonIdx + 1);
                                    int allowedFlag = (statusByte == '1') ? 1 : 0;

                                    AsyncRateLimitContext expectedCtx;
                                    while ((expectedCtx = syncWaitSlotRingBuffer.peek()) != null) {
                                        if (expectedCtx == SyncWaitSlotRingBuffer.CANCELLED_CONTEXT) {
                                            // 🎯 遇到 50ms 超时已取消的 Context，出队推进队列
                                            syncWaitSlotRingBuffer.poll();
                                            continue;
                                        }
                                        if (expectedCtx.userId == uidFromRedis) {
                                            // 🎯 返回的 UID 与队头精确匹配！
                                            final AsyncRateLimitContext matchedCtx = expectedCtx;
                                            syncWaitSlotRingBuffer.poll();
                                            if (matchedCtx.tryResolve()) {
                                                if (matchedCtx.timeoutHandle != null) {
                                                    matchedCtx.timeoutHandle.cancel();
                                                }
                                                matchedCtx.httpCtx.executor().execute(() -> matchedCtx.resume(allowedFlag == 1));
                                            }
                                            break;
                                        } else {
                                            // 队头不匹配时快速出队并熔断，防止队头阻塞
                                            final AsyncRateLimitContext mismatchedCtx = expectedCtx;
                                            syncWaitSlotRingBuffer.poll();
                                            if (mismatchedCtx.tryCancel()) {
                                                if (mismatchedCtx.timeoutHandle != null) mismatchedCtx.timeoutHandle.cancel();
                                                mismatchedCtx.httpCtx.executor().execute(() -> mismatchedCtx.resume(false));
                                            }
                                        }
                                    }
                                }

                                @Override
                                public void userEventTriggered(ChannelHandlerContext ctx, Object evt) throws Exception {
                                    if (evt instanceof IdleStateEvent) {
                                        // 5秒空闲，发送 PING 心跳探测
                                        ctx.writeAndFlush(Unpooled.wrappedBuffer(PING_CMD_BYTES));
                                    } else {
                                        super.userEventTriggered(ctx, evt);
                                    }
                                }

                                @Override
                                public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) {
                                    log.error("Redis RESP2 channel IO exception, closing channel to trigger auto-reconnect", cause);
                                    ctx.close();
                                }

                                @Override
                                public void channelInactive(ChannelHandlerContext ctx) {
                                    log.warn("Redis RESP2 channel disconnected/inactive! Triggering exponential backoff reconnect...");
                                    scheduleReconnect();
                                }
                            });
                        }
                    });

            String host = properties.getRedisHost() != null ? properties.getRedisHost() : "127.0.0.1";
            int port = properties.getRedisPort() != null ? properties.getRedisPort() : 6379;

            // 非阻塞发起 TCP 三次握手
            bootstrap.connect(host, port).addListener(future -> {
                reconnecting.set(false);
                if (future.isSuccess()) {
                    redisChannel = ((io.netty.channel.ChannelFuture) future).channel();
                    reconnectAttempts = 0; // 重置重连计数
                    loadDefaultScript();
                    log.info("Successfully connected/reconnected 0-GC RESP2 driver to Redis/Predixy [{}:{}]", host, port);
                } else {
                    log.error("Failed to connect 0-GC RESP2 driver to Redis [{}:{}], attempt: {}", host, port, reconnectAttempts);
                    scheduleReconnect();
                }
            });
        } catch (Exception e) {
            reconnecting.set(false);
            log.error("Error initiating Redis connection", e);
            scheduleReconnect();
        }
    }

    /**
     * 指数退避重连机制 (Exponential Backoff Reconnect): 1s, 2s, 4s, 8s, 16s, max 30s
     */
    private void scheduleReconnect() {
        if (reconnecting.compareAndSet(false, true)) {
            reconnectAttempts++;
            int delaySeconds = Math.min(30, 1 << Math.min(reconnectAttempts, 5));
            log.info("Scheduling Redis RESP2 driver reconnect in {} seconds...", delaySeconds);
            if (eventLoopGroup != null && !eventLoopGroup.isShutdown()) {
                eventLoopGroup.schedule(this::connectToRedisAsync, delaySeconds, TimeUnit.SECONDS);
            }
        }
    }

    // =========================================================================================
    // 🚀 0-GC 响应式校验基础设施: 无 GC 预分配 RingBuffer (0 堆内存分配、0 Node 开销)
    // =========================================================================================
    private final SyncWaitSlotRingBuffer syncWaitSlotRingBuffer = new SyncWaitSlotRingBuffer(1024);

    /**
     * 🚀 模式 B 原生响应式无阻塞限流入口：
     * 基于 FastThreadLocal 数量 (16条) + 时间 (50µs) 双阈值自适应微攒批 Pipeline，
     * 在单 TCP 连接下兼具 10 万+ QPS 极限吞吐与微秒级极低延迟。
     */
    public void acquireReactiveAsync(AsyncRateLimitContext asyncCtx, byte[] luaShaBytes) {
        if (redisChannel == null || !redisChannel.isActive()) {
            failClosed(asyncCtx);
            return;
        }

        ThreadReactiveBatchBuffer batchBuffer = THREAD_REACTIVE_BATCH_BUFFER.get();
        batchBuffer.contexts[batchBuffer.count++] = asyncCtx;
        long nowNanos = System.nanoTime();

        // 🎯 数量 (16条) + 时间 (50µs) 双阈值自适应触发
        if (batchBuffer.count >= REACTIVE_BATCH_SIZE || (nowNanos - batchBuffer.lastFlushNanos) >= REACTIVE_BATCH_TIMEOUT_NANOS) {
            acquireReactiveBatchAsync(batchBuffer.contexts, batchBuffer.count, luaShaBytes);
            batchBuffer.count = 0;
            batchBuffer.lastFlushNanos = nowNanos;
        }
    }

    public void acquireReactiveAsync(AsyncRateLimitContext asyncCtx) {
        acquireReactiveAsync(asyncCtx, luaShaBytes);
    }

    /**
     * 🚀 模式 B 强制刷新当前线程未满批次 (Flush Thread Buffer)
     */
    public void flushReactiveBatch(byte[] luaShaBytes) {
        ThreadReactiveBatchBuffer batchBuffer = THREAD_REACTIVE_BATCH_BUFFER.get();
        if (batchBuffer != null && batchBuffer.count > 0) {
            acquireReactiveBatchAsync(batchBuffer.contexts, batchBuffer.count, luaShaBytes);
            batchBuffer.count = 0;
            batchBuffer.lastFlushNanos = System.nanoTime();
        }
    }

    public void flushReactiveBatch() {
        flushReactiveBatch(luaShaBytes);
    }

    /**
     * 🚀 模式 B 单发直连限流入口：逐条单独构建 DirectByteBuf 刷入 Socket (无攒批 DirectFlush，用于极致单发对比)
     */
    public void acquireReactiveDirect(AsyncRateLimitContext asyncCtx, byte[] luaShaBytes) {
        if (redisChannel == null || !redisChannel.isActive()) {
            failClosed(asyncCtx);
            return;
        }
        registerTimeoutTask(asyncCtx);
        redisChannel.eventLoop().execute(() -> {
            boolean offered = syncWaitSlotRingBuffer.offer(asyncCtx);
            if (offered) {
                ByteBuf buf = PooledByteBufAllocator.DEFAULT.directBuffer(256);
                try {
                    encodeResp2EvalSha(buf, luaShaBytes, asyncCtx.userId,
                            System.currentTimeMillis(), maxTokens(), refillRate(), ttlSeconds(), 1);
                    redisChannel.writeAndFlush(buf);
                } catch (Exception e) {
                    buf.release();
                    log.error("Error sending 0-GC async EVALSHA for userId: {}", asyncCtx.userId, e);
                    syncWaitSlotRingBuffer.cancel(asyncCtx);
                    if (asyncCtx.timeoutHandle != null) asyncCtx.timeoutHandle.cancel();
                    asyncCtx.httpCtx.executor().execute(() -> {
                        if (asyncCtx.tryCancel()) asyncCtx.resume(false);
                    });
                }
            } else {
                if (asyncCtx.timeoutHandle != null) asyncCtx.timeoutHandle.cancel();
                asyncCtx.httpCtx.executor().execute(() -> {
                    if (asyncCtx.tryCancel()) asyncCtx.resume(false);
                });
            }
        });
    }

    public void acquireReactiveDirect(AsyncRateLimitContext asyncCtx) {
        acquireReactiveDirect(asyncCtx, luaShaBytes);
    }

    /**
     * 🚀 Mode B 响应式自适应微攒批 (Reactive Micro-Batching Pipeline)：
     * 将多个 AsyncRateLimitContext 一起打包进单个 Direct ByteBuf 一次性刷入 Socket，
     * 大幅平摊系统调用开销与 Redis 单线程协议反序列化成本。
     */
    public void acquireReactiveBatchAsync(AsyncRateLimitContext[] batch, int count, byte[] luaShaBytes) {
        if (count <= 0) return;
        if (redisChannel == null || !redisChannel.isActive()) {
            for (int i = 0; i < count; i++) {
                failClosed(batch[i]);
            }
            return;
        }

        // 1. 为批次内每个 context 注册 50ms 超时时间轮
        for (int i = 0; i < count; i++) {
            registerTimeoutTask(batch[i]);
        }

        // 2. 统一提交到 Redis EventLoop 批量入队并以单个 Direct ByteBuf 刷入 Socket
        redisChannel.eventLoop().execute(() -> {
            long nowMs = System.currentTimeMillis();
            int maxTokens = maxTokens();
            int refillRate = refillRate();
            int ttlSec = ttlSeconds();

            ByteBuf buf = PooledByteBufAllocator.DEFAULT.directBuffer(256 * count);
            try {
                for (int i = 0; i < count; i++) {
                    AsyncRateLimitContext asyncCtx = batch[i];
                    if (asyncCtx != null) {
                        boolean offered = syncWaitSlotRingBuffer.offer(asyncCtx);
                        if (offered) {
                            encodeResp2EvalSha(buf, luaShaBytes, asyncCtx.userId, nowMs, maxTokens, refillRate, ttlSec, 1);
                        } else {
                            if (asyncCtx.timeoutHandle != null) asyncCtx.timeoutHandle.cancel();
                            asyncCtx.httpCtx.executor().execute(() -> {
                                if (asyncCtx.tryCancel()) {
                                    asyncCtx.resume(false);
                                }
                            });
                        }
                    }
                }
                if (buf.isReadable()) {
                    redisChannel.writeAndFlush(buf);
                } else {
                    buf.release();
                }
            } catch (Exception e) {
                buf.release();
                log.error("Error batch sending 0-GC async EVALSHA commands", e);
            }
        });
    }

    public void acquireReactiveBatchAsync(AsyncRateLimitContext[] batch, int count) {
        acquireReactiveBatchAsync(batch, count, luaShaBytes);
    }

    /**
     * 🛡️ 注册 Mode B 临界限流 50ms 超时熔断保护任务 (Fail-Fast & Timeout Circuit Breaker)
     *
     * 业务背景与设计目标：
     * 1. 【80% 水位临界防悬挂】：
     *    当用户进入 80% 水位临界预警（-2L）时，网关切入 Mode B 等待 Redis 强校验仲裁。
     *    若因物理网络抖动、丢包或 Redis 服务端慢查询导致未在 50ms 内返回，请求绝不能无休止挂起占用连接与内存。
     * 2. 【50ms HashedWheelTimer 时间轮轻量调度】：
     *    采用高效的 Netty 时间轮以 O(1) 复杂度管理在途超时任务，杜绝为每个请求创建独立线程或 JDK ScheduledFuture 堆对象。
     * 3. 【CAS 原子状态互斥与防双重唤醒】：
     *    超时触发时通过 {@code asyncCtx.tryTimeout()}（CAS 将 STATE_INIT 原子置为 STATE_TIMEOUT），
     *    与迟到的 Redis 正常回包（{@code tryResolve()}）严格物理互斥，确保仲裁结果仅能生效一次。
     * 4. 【跨 EventLoop 线程安全调度与 0-GC 对象池闭环】：
     *    通过 {@code asyncCtx.httpCtx.executor().execute(...)} 将执行权限无锁调度回原始 HTTP 绑定的 EventLoop 线程，
     *    执行 {@code asyncCtx.resume(false)} 快速返回 403 拦截并安全回收续体对象至 Recycler 池，彻底杜绝孤儿请求与内存泄漏。
     */
    private void registerTimeoutTask(AsyncRateLimitContext asyncCtx) {
        if (asyncCtx != null) {
            final io.netty.channel.ChannelHandlerContext httpCtx = asyncCtx.httpCtx;
            asyncCtx.timeoutHandle = HASHED_WHEEL_TIMER.newTimeout(timeout -> {
                if (asyncCtx.tryTimeout()) {
                    if (httpCtx != null && httpCtx.executor() != null) {
                        httpCtx.executor().execute(() -> asyncCtx.resume(false));
                    } else {
                        asyncCtx.resume(false);
                    }
                }
            }, 50, TimeUnit.MILLISECONDS);
        }
    }

    /**
     * 🛡️ Redis 连接断开时的 Fail-Closed 异步安全拒绝 (Fail-Closed Safety Barrier)
     *
     * 业务背景：
     * 处于 Mode B（80% 临界高危）的用户可能为突发恶意爬虫或黑产流量，当 Redis TCP 通道不可用时，
     * 必须采取 Fail-Closed 策略严格拦截拒绝（403），严防攻击者利用 Redis 网络空窗期渗透击穿下游业务；
     * 同时调度回原 HTTP EventLoop 触发 resume(false) 确保 0-GC 上下文闭环归还对象池。
     */
    private static void failClosed(AsyncRateLimitContext asyncCtx) {
        if (asyncCtx != null) {
            asyncCtx.httpCtx.executor().execute(() -> {
                if (asyncCtx.tryResolve()) {
                    asyncCtx.resume(false);
                }
            });
        }
    }

    /**
     * 0-GC 直连限流入口：直接构建 RESP2 EVALSHA 命令字节流异步发送给 Redis (带 Fail-Open 降级保护)
     */
    public void acquireSingleDirect(long userId, byte[] luaShaBytes) {
        if (redisChannel == null || !redisChannel.isActive()) {
            // 🛡️ 降级保障 (Fail-Open)：连接异常空窗期内静默放行，确保网关核心 HTTP 流程不受影响
            return;
        }
        ByteBuf buf = PooledByteBufAllocator.DEFAULT.directBuffer(256);
        try {
            encodeResp2EvalSha(buf, luaShaBytes, userId,
                    System.currentTimeMillis(), maxTokens(), refillRate(), ttlSeconds(), 1);
            redisChannel.writeAndFlush(buf);
        } catch (Exception e) {
            buf.release();
            log.error("Failed to send RESP2 EVALSHA command for userId: {}", userId, e);
        }
    }

    public void acquireSingleDirect(long userId) {
        acquireSingleDirect(userId, luaShaBytes);
    }

    /**
     * 🚀 模式 A 限流入口：基于 FastThreadLocal + long[] 数组自适应攒批 RESP2 Pipeline 异步发送
     * (攒满 32 条或超时 50µs 自动打成单个 ByteBuf 批量发送，带 Fail-Open 降级保护)
     */
    public void acquireBatchOffload(long userId, byte[] luaShaBytes) {
        if (redisChannel == null || !redisChannel.isActive()) {
            // 🛡️ 降级保障 (Fail-Open)：连接不可用时快速通过
            return;
        }
        ThreadRedisBatchBuffer batchBuffer = THREAD_REDIS_BATCH_BUFFER.get();
        batchBuffer.uids[batchBuffer.count++] = userId;
        long now = System.nanoTime();
        // 🚀 数量 + 时间双阈值攒批触发：数量满 32 条 或 距上次 Flush 超过 50µs (50,000ns)
        if (batchBuffer.count >= 32 || (now - batchBuffer.lastFlushNanos) >= 50_000L) {
            flushLongArrayPipeline(batchBuffer.uids, batchBuffer.count, luaShaBytes);
            batchBuffer.count = 0;
            batchBuffer.lastFlushNanos = now;
        }
    }

    public void acquireBatchOffload(long userId) {
        acquireBatchOffload(userId, luaShaBytes);
    }

    /**
     * 强制刷出当前线程 FastThreadLocal 缓冲区中残留的 UID 命令
     */
    public void flushThreadBatch(byte[] luaShaBytes) {
        ThreadRedisBatchBuffer batchBuffer = THREAD_REDIS_BATCH_BUFFER.get();
        if (batchBuffer.count > 0) {
            flushLongArrayPipeline(batchBuffer.uids, batchBuffer.count, luaShaBytes);
            batchBuffer.count = 0;
            batchBuffer.lastFlushNanos = System.nanoTime();
        }
    }


    /**
     * 核心 Pipeline 批量刷出与网络发送方法：循环 long[] 数组追加 count 条 RESP2 命令至单个 Direct ByteBuf 一次性发送
     */
    public void flushLongArrayPipeline(long[] uids, int count, byte[] luaShaBytes) {
        if (redisChannel == null || !redisChannel.isActive() || count <= 0) {
            return;
        }

        // 分配 1 个 Direct ByteBuf 打包发送整个 Batch
        ByteBuf pipelineBuf = PooledByteBufAllocator.DEFAULT.directBuffer(count * 256);
        try {
            for (int i = 0; i < count; i++) {
                encodeResp2EvalSha(pipelineBuf, luaShaBytes, uids[i],
                        System.currentTimeMillis(), maxTokens(), refillRate(), ttlSeconds(), 1);
            }
            // 单次 Socket writeAndFlush 发送整个 Batch 命令
            redisChannel.writeAndFlush(pipelineBuf);
        } catch (Exception e) {
            pipelineBuf.release();
            log.error("Failed to flush RESP2 batch pipeline for {} uids", count, e);
        }
    }

    /**
     * 封装 RESP2 协议 EVALSHA 命令的 0-GC 快速序列化逻辑
     */
    private static void encodeResp2EvalSha(ByteBuf buf, byte[] luaShaBytes, long userId,
                                           long nowMs, int maxTokens, int refillRate,
                                           int ttlSec, int requested) {
        buf.writeBytes(EVALSHA_CMD_PREFIX);
        writeBulkBytes(buf, luaShaBytes);
        writeBulkLong(buf, 1);
        writeBulkLong(buf, userId);
        writeBulkLong(buf, nowMs);
        writeBulkLong(buf, maxTokens);
        writeBulkLong(buf, refillRate);
        writeBulkLong(buf, ttlSec);
        writeBulkLong(buf, requested);
    }

    private static void encodeResp2EvalSha(ByteBuf buf, byte[] luaShaBytes, long userId) {
        encodeResp2EvalSha(buf, luaShaBytes, userId, System.currentTimeMillis(), 20, 20, 2, 1);
    }

    private static void writeBulkBytes(ByteBuf buf, byte[] value) {
        buf.writeByte('$');
        com.netty.limiter.util.ZeroGcNumberUtil.writeLongToAsciiByteBuf(buf, value.length);
        buf.writeBytes(CRLF);
        buf.writeBytes(value);
        buf.writeBytes(CRLF);
    }

    private static void writeBulkLong(ByteBuf buf, long value) {
        buf.writeByte('$');
        com.netty.limiter.util.ZeroGcNumberUtil.writeLongToAsciiByteBuf(buf,
                com.netty.limiter.util.ZeroGcNumberUtil.getLongAsciiLength(value));
        buf.writeBytes(CRLF);
        com.netty.limiter.util.ZeroGcNumberUtil.writeLongToAsciiByteBuf(buf, value);
        buf.writeBytes(CRLF);
    }

    private int maxTokens() {
        return properties != null && properties.getUidMaxPerSec() != null ? properties.getUidMaxPerSec() : 20;
    }

    private int refillRate() {
        return maxTokens();
    }

    private int ttlSeconds() {
        return 2;
    }

    private void loadDefaultScript() {
        if (redisChannel == null || !redisChannel.isActive()) {
            return;
        }
        byte[] script = com.netty.limiter.util.LuaSha1Util.DEFAULT_LUA_SCRIPT.getBytes(StandardCharsets.UTF_8);
        ByteBuf buf = PooledByteBufAllocator.DEFAULT.directBuffer(SCRIPT_LOAD_PREFIX.length + script.length + 16);
        buf.writeBytes(SCRIPT_LOAD_PREFIX);
        writeBulkBytes(buf, script);
        redisChannel.writeAndFlush(buf);
    }
}
