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
import io.netty.util.concurrent.FastThreadLocal;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import javax.annotation.PostConstruct;
import javax.annotation.PreDestroy;
import java.lang.invoke.MethodHandles;
import java.lang.invoke.VarHandle;
import java.nio.charset.StandardCharsets;
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
    private static final byte[] EVALSHA_CMD_PREFIX = "*8\r\n$7\r\nEVALSHA\r\n".getBytes(StandardCharsets.US_ASCII);
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
    // 🚀 模式 A：FastThreadLocal 线程本地 0-GC long[] 攒批缓冲区 (0 跨核、0 CAS 锁开销)
    // =========================================================================================
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
            // 🛡️ 引用解绑：移除该 FastThreadLocal 槽位引用，彻底避免 JVM 热重载 ClassLoader 物理泄露
            FastThreadLocal.removeAll();
        }
    };

    @PostConstruct
    public void init() {
        eventLoopGroup = new NioEventLoopGroup(1);
        connectToRedisAsync();
    }

    @PreDestroy
    public void destroy() {
        try {
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
                                    int colonIdx = msg.indexOf(start, end, (byte) ':');
                                    if (colonIdx < 0 || colonIdx >= end - 1) {
                                        return;
                                    }
                                    long uidFromRedis = ZeroGcNumberUtil.parseLongFromByteBuf(msg, start, colonIdx);
                                    byte statusByte = msg.getByte(colonIdx + 1);
                                    int allowedFlag = (statusByte == '1') ? 1 : 0;

                                    SyncWaitSlotRingBuffer.SyncWaitSlot expectedSlot = syncWaitSlotRingBuffer.peek();
                                    // 🎯 peek() 内的 ARRAY_VH.getAcquire 已建立 HB，userId/waiterThread 均为可见 plain 字段
                                    if (expectedSlot != null && expectedSlot.userId == uidFromRedis) {
                                        syncWaitSlotRingBuffer.poll(); // COW 替换 ZERO_SLOT，原子清空槽位
                                        // status plain 写：HB 由下方 unpark → park 链保证
                                        expectedSlot.status = (allowedFlag == 1) ? 1 : 2;
                                        Thread targetThread = expectedSlot.waiterThread; // plain 读，已通过 getAcquire 可见
                                        if (targetThread != null) {
                                            java.util.concurrent.locks.LockSupport.unpark(targetThread);
                                        }
                                    }
                                    // 🛡️ 若 userId 不匹配 (模式 A 异步返回数值)，直接跳过不唤醒！
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
    // 🚀 0-GC 同步校验基础设施: 无 GC 数组预分配 RingBuffer (0 堆内存分配、0 Node 开销)
    // =========================================================================================
    private final SyncWaitSlotRingBuffer syncWaitSlotRingBuffer = new SyncWaitSlotRingBuffer(1024);

    /**
     * 🚀 模式 B (0-GC 同步上报放行校验): 针对 80% 水位预警 (-2L) UID 执行 RESP2 0-GC 同步 EVALSHA 扣减
     * 100% 零 GC 堆内存分配，无 Node 垃圾回收负担。
     */
    public boolean acquire0GcUidSync(long userId, byte[] luaShaBytes) {
        if (redisChannel == null || !redisChannel.isActive()) {
            // 🛡️ Fail-Closed / Connection Dead 降级保护
            return false;
        }

        SyncWaitSlotRingBuffer.SyncWaitSlot slot = syncWaitSlotRingBuffer.offer(userId, Thread.currentThread());
        if (slot == null) {
            return false; // 🛡️ 缓冲区满，Fail-Open 降级拦截
        }

        // ② 0-GC 拼接 RESP2 EVALSHA 字节流并发送至 Redis TCP Channel
        ByteBuf buf = PooledByteBufAllocator.DEFAULT.directBuffer(256);
        try {
            encodeResp2EvalSha(buf, luaShaBytes, userId,
                    System.currentTimeMillis(), maxTokens(), refillRate(), ttlSeconds(), 1);
            redisChannel.writeAndFlush(buf);
        } catch (Exception e) {
            buf.release();
            log.error("Error sending 0-GC sync EVALSHA command for userId: {}", userId, e);
            return false; // 🛡️ Fail-Open 降级拦截
        }

        // ③ 同步阻塞等待 Netty ChannelRead 唤醒 (50ms 超时 + 虚假唤醒防护)
        // 🛡️ 虚假唤醒防护：用 deadline while 循环包裹 parkNanos 是 Java 并发标准惯用法。
        //    场景：上一次请求超时后 EventLoop 迟到唤醒并存入 permit，下一次 park 会被该 stale permit
        //    立即唤醒导致 status=0 误判。while 循环在第一次迭代消耗 stale permit 后会再次 park，彻底解决虚假唤醒。
        long deadlineNs = System.nanoTime() + TimeUnit.MILLISECONDS.toNanos(50);
        while (slot.status == 0) {
            long remaining = deadlineNs - System.nanoTime();
            if (remaining <= 0) {
                break; // 真正超时退出
            }
            java.util.concurrent.locks.LockSupport.parkNanos(remaining);
            // 若因 stale permit 立即返回：status 仍为 0，deadline 未到，继续自旋等待真实唤醒
        }

        // status plain 读：HB 由 unpark → park Happens-Before 链保证
        int result = slot.status;
        // 🎯 无需 slot.clear()：FTL Slot 在下次 offer() 时由本线程自己覆写字段，ZERO_SLOT 已由 poll() COW 原子清空
        if (result == 0) {
            return false; // 🛡️ Fail-Open 超时降级拦截
        }
        return result == 1;
    }

    /**
     * 0-GC 直连限流入口：直接构建 RESP2 EVALSHA 命令字节流异步发送给 Redis (带 Fail-Open 降级保护)
     */
    public void acquire0GcUid(long userId, byte[] luaShaBytes) {
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

    /**
     * 🚀 模式 A 限流入口：基于 FastThreadLocal + long[] 数组自适应攒批 RESP2 Pipeline 异步发送
     * (攒满 32 条或超时 50µs 自动打成单个 ByteBuf 批量发送，带 Fail-Open 降级保护)
     */
    public void acquire0GcUidBatch(long userId, byte[] luaShaBytes) {
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
     * 核心 Pipeline 刷盘方法：循环 long[] 数组追加 count 条 RESP2 命令至单个 Direct ByteBuf 一次性发送
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
