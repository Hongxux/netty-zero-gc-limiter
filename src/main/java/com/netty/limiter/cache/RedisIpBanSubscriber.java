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
 * @description: Redis Pub/Sub 全网黑名单 0-GC RESP2 原生 Socket 订阅器 (彻底抹平 Spring Data Reactive 依赖)
 **/
@Slf4j
@Component
public class RedisIpBanSubscriber implements CommandLineRunner {

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
                    log.info("Successfully connected RESP2 PubSub subscriber to Redis [{}:{}] on channel [{}]", host, port, BAN_PUBSUB_CHANNEL);
                } else {
                    log.error("Failed to connect RESP2 PubSub subscriber to Redis [{}:{}]", host, port);
                    scheduleReconnect();
                }
            });
        } catch (Exception e) {
            log.error("Error initiating RESP2 PubSub connection", e);
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
     * 🚀 100% 0-GC 裸 ByteBuf 解析黑名单通知 (零堆内存分配)
     * 仅支持 2 种高效格式:
     * 1. "10001:300" (uid:banTime - 封禁 userId=10001, 时长 300 秒)
     * 2. "10001"     (uid - 封禁 userId=10001, 默认时长 60 秒)
     */
    private void processByteBufLine0GC(ByteBuf msg) {
        int start = msg.readerIndex();
        int end = msg.writerIndex();
        int len = end - start;
        if (len <= 0) return;

        byte firstByte = msg.getByte(start);
        // 1. 过滤 RESP2 协议元数据标头: * (0x2A), $ (0x24)
        if (firstByte == '*' || firstByte == '$') {
            return;
        }

        // 2. 检查是否匹配 "message" / "subscribe" / BAN_PUBSUB_CHANNEL 频道名
        // 3. 0-GC 裸字节直接提取 userId 与 banTime
        int colonIdx = msg.indexOf(start, end, (byte) ':');
        long userId;
        long duration = 60L; // 默认 60 秒
        if (colonIdx >= 0) {
            userId = ZeroGcNumberUtil.parseLongFromByteBuf(msg, start, colonIdx);
            long dur = ZeroGcNumberUtil.parseLongFromByteBuf(msg, colonIdx + 1, end);
            if (dur > 0) duration = dur;
        } else {
            userId = ZeroGcNumberUtil.parseLongFromByteBuf(msg, start, end);
        }

        if (userId > 0) {
            localBanCache.putUserBan(userId, duration);
            log.warn("Received 0-GC RESP2 PubSub ban message for userId: {}, duration: {}s", userId, duration);
        }
    }

    private static final byte[] MESSAGE_BYTES = "message".getBytes(StandardCharsets.US_ASCII);
    private static final byte[] SUBSCRIBE_BYTES = "subscribe".getBytes(StandardCharsets.US_ASCII);
    private static final byte[] BAN_PUBSUB_CHANNEL_BYTES = BAN_PUBSUB_CHANNEL.getBytes(StandardCharsets.US_ASCII);

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
            log.info("Successfully stopped RESP2 PubSub subscriber.");
        } catch (Exception e) {
            log.error("Error shutting down RESP2 PubSub subscriber", e);
        }
    }
}
