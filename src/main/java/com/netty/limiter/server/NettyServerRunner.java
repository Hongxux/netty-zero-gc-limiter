package com.netty.limiter.server;

import com.netty.limiter.config.GatewayRateLimitProperties;
import com.netty.limiter.handler.NettyInboundSecurityHandler;
import com.netty.limiter.handler.NettyJwtHeaderAccumulatorHandler;
import io.netty.bootstrap.ServerBootstrap;
import io.netty.buffer.Unpooled;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelOption;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.SimpleChannelInboundHandler;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioServerSocketChannel;
import io.netty.handler.codec.http.DefaultFullHttpResponse;
import io.netty.handler.codec.http.FullHttpRequest;
import io.netty.handler.codec.http.HttpHeaderNames;
import io.netty.handler.codec.http.HttpHeaderValues;
import io.netty.handler.codec.http.HttpObjectAggregator;
import io.netty.handler.codec.http.HttpResponseStatus;
import io.netty.handler.codec.http.HttpVersion;
import io.netty.handler.codec.http.HttpServerCodec;
import lombok.extern.slf4j.Slf4j;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.CommandLineRunner;
import org.springframework.stereotype.Component;

import javax.annotation.PreDestroy;
import java.nio.charset.StandardCharsets;

/**
 * @description: 纯原生 Netty 独立运行安全网关 HTTP 服务端启动器 (仅在 netty.limiter.standalone=true 时生效)
 **/
@Slf4j
@Component
@ConditionalOnProperty(prefix = "netty.limiter", name = "standalone", havingValue = "true", matchIfMissing = false)
public class NettyServerRunner implements CommandLineRunner {

    @Autowired
    private GatewayRateLimitProperties properties;

    @Autowired
    private NettyJwtHeaderAccumulatorHandler jwtAccumulatorHandler;

    @Autowired
    private NettyInboundSecurityHandler securityHandler;

    private EventLoopGroup bossGroup;
    private EventLoopGroup workerGroup;

    @Override
    public void run(String... args) throws Exception {
        int port = properties.getServerPort() != null ? properties.getServerPort() : 8888;

        bossGroup = new NioEventLoopGroup(1);
        workerGroup = new NioEventLoopGroup();

        try {
            ServerBootstrap b = new ServerBootstrap();
            b.group(bossGroup, workerGroup)
                    .channel(NioServerSocketChannel.class)
                    .option(ChannelOption.SO_BACKLOG, 1024)
                    .childOption(ChannelOption.SO_KEEPALIVE, true)
                    .childOption(ChannelOption.TCP_NODELAY, true)
                    .childHandler(new ChannelInitializer<SocketChannel>() {
                        @Override
                        protected void initChannel(SocketChannel ch) {
                            // -------------------------------------------------------------------------
                            // 🚀 极前置防线 (Pre-Codec Security Gate)：在 HttpServerCodec 之前！
                            // 所有非法/超限/黑名单攻击在裸 ByteBuf 阶段即被拦截丢弃 (0-GC，不触发 HTTP 解析)
                            // -------------------------------------------------------------------------
                            ch.pipeline().addLast("jwtAccumulator", jwtAccumulatorHandler);
                            ch.pipeline().addLast("securityHandler", securityHandler);

                            // -------------------------------------------------------------------------
                            // 🛡️ 后置协议与代理层 (Post-Security HTTP Line)：放置在防线之后！
                            // 只有 100% 校验通过的合法流量，才会流向 HttpServerCodec 解析 HTTP 报文
                            // -------------------------------------------------------------------------
                            ch.pipeline().addLast("httpServerCodec", new HttpServerCodec());
                            ch.pipeline().addLast("httpAggregator", new HttpObjectAggregator(65536));
                            ch.pipeline().addLast("gatewayBackendHandler", new SimpleChannelInboundHandler<FullHttpRequest>() {
                                @Override
                                protected void channelRead0(ChannelHandlerContext ctx, FullHttpRequest req) {
                                    // 模拟透传到下游微服务响应
                                    DefaultFullHttpResponse resp = new DefaultFullHttpResponse(
                                            HttpVersion.HTTP_1_1,
                                            HttpResponseStatus.OK,
                                            Unpooled.copiedBuffer("{\"code\":200,\"message\":\"Gateway Pass & Forwarded Successfully\"}", StandardCharsets.UTF_8)
                                    );
                                    resp.headers().set(HttpHeaderNames.CONTENT_TYPE, "application/json;charset=UTF-8");
                                    resp.headers().set(HttpHeaderNames.CONTENT_LENGTH, resp.content().readableBytes());
                                    resp.headers().set(HttpHeaderNames.CONNECTION, HttpHeaderValues.KEEP_ALIVE);
                                    ctx.writeAndFlush(resp);
                                }
                            });
                        }
                    });

            ChannelFuture f = b.bind(port).sync();
            log.info("🚀 Standalone Netty 0-GC Security Gateway Server listening on port {}...", port);
        } catch (Exception e) {
            log.error("Failed to start Standalone Netty Security Gateway Server", e);
            shutdownGracefully();
        }
    }

    @PreDestroy
    public void destroy() {
        shutdownGracefully();
    }

    private void shutdownGracefully() {
        // 阶段 1：先关 BossGroup，停止监听 8888 端口，拒绝任何新的 Incoming TCP 连接
        stopBossGroup();
        // 阶段 2：再关 WorkerGroup，等待网关在途 HTTP 请求全部处理完毕并触发 FTL onRemoval
        stopWorkerGroup();
        log.info("Successfully stopped Standalone Netty Security Gateway Server.");
    }

    private void stopBossGroup() {
        if (bossGroup != null) {
            bossGroup.shutdownGracefully().syncUninterruptibly();
        }
    }

    private void stopWorkerGroup() {
        if (workerGroup != null) {
            workerGroup.shutdownGracefully().syncUninterruptibly();
        }
    }
}
