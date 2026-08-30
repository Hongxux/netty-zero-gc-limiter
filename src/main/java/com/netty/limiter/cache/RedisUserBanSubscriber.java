package com.netty.limiter.cache;

import com.netty.limiter.config.GatewayRateLimitProperties;
import com.netty.limiter.limiter.UserRateLimiterOperate;
import com.netty.limiter.util.ZeroGcNumberUtil;
import io.netty.bootstrap.Bootstrap;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.*;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioSocketChannel;
import io.netty.handler.timeout.IdleStateEvent;
import io.netty.handler.timeout.IdleStateHandler;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.CommandLineRunner;
import org.springframework.stereotype.Component;

import javax.annotation.PreDestroy;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * @description: Redis Pub/Sub 全网 UID 黑名单 0-GC RESP2 原生 Socket 订阅器
 * 专为 UID 全网黑名单同步设计，绝对 0 堆内存分配
 **/
@Slf4j
@Component
public class RedisUserBanSubscriber implements CommandLineRunner {

    private static final String BAN_PUBSUB_CHANNEL = "NETTY_LIMITER_BAN_CHANNEL";
    private static final byte[] SUBSCRIBE_CMD_BYTES = ("*2\r\n$9\r\nSUBSCRIBE\r\n$" 
            + BAN_PUBSUB_CHANNEL.length() + "\r\n" + BAN_PUBSUB_CHANNEL + "\r\n").getBytes(StandardCharsets.US_ASCII);

    private static final byte[] PING_CMD_BYTES = "*1\r\n$4\r\nPING\r\n".getBytes(StandardCharsets.US_ASCII);

    @Autowired
    private GatewayRateLimitProperties properties;

    @Autowired
    private LocalBanCache localBanCache;

    @Autowired(required = false)
    private UserRateLimiterOperate userRateLimiterOperate;

    private volatile Channel subscriberChannel;
    private EventLoopGroup ownEventLoopGroup;
    private final AtomicBoolean reconnecting = new AtomicBoolean(false);

    @Override
    public void run(String... args) throws Exception {
        connectToRedisPubSubAsync();
    }

    private synchronized void connectToRedisPubSubAsync() {
        if (subscriberChannel != null && subscriberChannel.isActive()) {
            return;
        }

        EventLoopGroup group = (userRateLimiterOperate != null && userRateLimiterOperate.getEventLoopGroup() != null)
                ? userRateLimiterOperate.getEventLoopGroup()
                : (ownEventLoopGroup != null ? ownEventLoopGroup : (ownEventLoopGroup = new NioEventLoopGroup(1)));

        try {
            Bootstrap bootstrap = new Bootstrap();
            bootstrap.group(group)
                    .channel(NioSocketChannel.class)
                    .option(ChannelOption.TCP_NODELAY, true)
                    .option(ChannelOption.SO_KEEPALIVE, true)
                    .handler(new ChannelInitializer<SocketChannel>() {
                        @Override
                        protected void initChannel(SocketChannel ch) {
                            ch.pipeline().addLast(new IdleStateHandler(0, 30, 0, TimeUnit.SECONDS));
                            // 🚀 挂载 CRLF 行拆包/粘包解码器：解决 TCP 粘包/半包问题，按 CRLF (\r\n) 完整切割 RESP2 报文
                            ch.pipeline().addLast(new io.netty.handler.codec.LineBasedFrameDecoder(4096));
                            ch.pipeline().addLast(new SimpleChannelInboundHandler<ByteBuf>() {
                                @Override
                                public void channelActive(ChannelHandlerContext ctx) throws Exception {
                                    // 连上 Redis 后，发送 RESP2 SUBSCRIBE 命令
                                    ctx.writeAndFlush(Unpooled.wrappedBuffer(SUBSCRIBE_CMD_BYTES));
                                    super.channelActive(ctx);
                                }

                                @Override
                                protected void channelRead0(ChannelHandlerContext ctx, ByteBuf msg) {
                                    processByteBufLine0GC(msg);
                                }

                                @Override
                                public void userEventTriggered(ChannelHandlerContext ctx, Object evt) throws Exception {
                                    if (evt instanceof IdleStateEvent) {
                                        ctx.writeAndFlush(Unpooled.wrappedBuffer(PING_CMD_BYTES));
                                    } else {
                                        super.userEventTriggered(ctx, evt);
                                    }
                                }

                                @Override
                                public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) {
                                    log.error("Redis PubSub RESP2 channel error, closing to reconnect", cause);
                                    ctx.close();
                                }

                                @Override
                                public void channelInactive(ChannelHandlerContext ctx) {
                                    log.warn("Redis PubSub channel disconnected, scheduling reconnect...");
                                    scheduleReconnect();
                                }
                            });
                        }
                    });

            String host = properties.getRedisHost() != null ? properties.getRedisHost() : "127.0.0.1";
            int port = properties.getRedisPort() != null ? properties.getRedisPort() : 6379;

            bootstrap.connect(host, port).addListener(future -> {
                reconnecting.set(false);
                if (future.isSuccess()) {
                    subscriberChannel = ((ChannelFuture) future).channel();
                    log.info("Successfully connected RESP2 PubSub UID subscriber to Redis [{}:{}] on channel [{}]", host, port, BAN_PUBSUB_CHANNEL);
                } else {
                    log.error("Failed to connect RESP2 PubSub UID subscriber to Redis [{}:{}]", host, port);
                    scheduleReconnect();
                }
            });
        } catch (Exception e) {
            log.error("Error initiating RESP2 PubSub UID connection", e);
            scheduleReconnect();
        }
    }

    private void scheduleReconnect() {
        if (reconnecting.compareAndSet(false, true)) {
            EventLoopGroup group = (userRateLimiterOperate != null && userRateLimiterOperate.getEventLoopGroup() != null)
                    ? userRateLimiterOperate.getEventLoopGroup()
                    : ownEventLoopGroup;
            if (group != null && !group.isShutdown()) {
                group.schedule(this::connectToRedisPubSubAsync, 3, TimeUnit.SECONDS);
            }
        }
    }

    /**
     * 🚀 100% 0-GC 裸 ByteBuf 解析 UID 黑名单通知 (零堆内存分配)
     * 支持格式:
     * 1. "10001:300" 或 "U:10001:300" (UID 封禁, 时长 300 秒)
     * 2. "10001" (UID 封禁, 默认时长 60 秒)
     */
    private void processByteBufLine0GC(ByteBuf msg) {
        int start = msg.readerIndex();
        int end = msg.writerIndex();
        int len = end - start;
        if (len <= 0) return;

        byte firstByte = msg.getByte(start);
        // 1. 过滤 RESP2 协议控制标头: * (Array), $ (BulkString), : (Integer), + (SimpleString), - (Error)
        if (isResp2ProtocolHeader(firstByte)) {
            return;
        }

        // 2. 过滤 "message" / "subscribe" / BAN_PUBSUB_CHANNEL 频道名等 RESP2 PubSub 字符串元数据
        if (isMetadataHeader(msg, start, len)) {
            return;
        }

        // 3. 判断是否包含前缀 "U:" / "W:" (U: 封禁, W: 80% 水位预警)
        int curStart = start;
        boolean isWarning = false;
        if (len >= 2 && msg.getByte(start + 1) == ':') {
            byte prefix = (byte) Character.toUpperCase((char) firstByte);
            if (prefix == 'W') {
                isWarning = true;
                curStart = start + 2;
            } else if (prefix == 'U') {
                curStart = start + 2;
            }
        }

        int colonIdx = msg.indexOf(curStart, end, (byte) ':');
        long userId;
        long duration = 60L; // 默认 60 秒

        if (colonIdx >= 0) {
            userId = ZeroGcNumberUtil.parseLongFromByteBuf(msg, curStart, colonIdx);
            long dur = ZeroGcNumberUtil.parseLongFromByteBuf(msg, colonIdx + 1, end);
            if (dur > 0) duration = dur;
        } else {
            userId = ZeroGcNumberUtil.parseLongFromByteBuf(msg, curStart, end);
        }

        if (userId > 0) {
            if (isWarning) {
                // W: 80% 水位预警 -> 写入 -2L 特殊降级标记 (WARNED_EXP_SEC_MARK)，后续请求触发同步上报 Redis 校验
                localBanCache.putUserWarned(userId);
                log.warn("Received 0-GC RESP2 PubSub 80% Watermark Warning for userId: {}, set ExpSec=-2L (Sync Required)", userId);
            } else {
                // U: 100% 彻底耗尽 -> 写入硬封禁
                localBanCache.putUserBan(userId, duration);
                log.warn("Received 0-GC RESP2 PubSub UID Hard Ban message for userId: {}, duration: {}s", userId, duration);
            }
        }
    }

    private static final byte[] MESSAGE_BYTES = "message".getBytes(StandardCharsets.US_ASCII);
    private static final byte[] SUBSCRIBE_BYTES = "subscribe".getBytes(StandardCharsets.US_ASCII);
    private static final byte[] BAN_PUBSUB_CHANNEL_BYTES = BAN_PUBSUB_CHANNEL.getBytes(StandardCharsets.US_ASCII);

    private boolean isResp2ProtocolHeader(byte firstByte) {
        return firstByte == '*' || firstByte == '$' || firstByte == ':' || firstByte == '+' || firstByte == '-';
    }

    private boolean isMetadataHeader(ByteBuf buf, int start, int len) {
        if (len == 7 && ZeroGcNumberUtil.equalsBytesIgnoreCase(buf, start, MESSAGE_BYTES)) return true;
        if (len == 9 && ZeroGcNumberUtil.equalsBytesIgnoreCase(buf, start, SUBSCRIBE_BYTES)) return true;
        if (len == BAN_PUBSUB_CHANNEL_BYTES.length && ZeroGcNumberUtil.equalsBytesIgnoreCase(buf, start, BAN_PUBSUB_CHANNEL_BYTES)) return true;
        return false;
    }

    @PreDestroy
    public void destroy() {
        try {
            if (subscriberChannel != null && subscriberChannel.isActive()) {
                subscriberChannel.writeAndFlush(Unpooled.EMPTY_BUFFER).syncUninterruptibly();
                subscriberChannel.close().syncUninterruptibly();
            }
            if (ownEventLoopGroup != null) {
                ownEventLoopGroup.shutdownGracefully().syncUninterruptibly();
            }
            log.info("Successfully stopped RESP2 PubSub UID subscriber.");
        } catch (Exception e) {
            log.error("Error shutting down RESP2 PubSub UID subscriber", e);
        }
    }
}
