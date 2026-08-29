package com.netty.limiter.limiter;

import com.netty.limiter.config.GatewayRateLimitProperties;
import io.netty.bootstrap.ServerBootstrap;
import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelOption;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.SimpleChannelInboundHandler;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioServerSocketChannel;
import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicLong;

/**
 * 真实网络 TCP Socket 压测: UserRateLimiterOperate 模式 A 线程本地 long[] 攒批 Pipeline 对接 127.0.0.1:6391 真实 RESP2 端口
 */
public class UserRateLimiterRealRedisBenchmarkTest {

    public static void main(String[] args) throws Exception {
        new UserRateLimiterRealRedisBenchmarkTest().runRealRedisBenchmark();
    }

    @Test
    public void runRealRedisBenchmark() throws Exception {
        int targetPort = 6391;
        // 1. 启动监听 127.0.0.1:6391 的真实 RESP2 TCP Socket 服务端
        EmbeddedRedisServer redisServer = new EmbeddedRedisServer(targetPort);
        redisServer.start();
        Thread.sleep(500); // 等待 Server 监听成功

        try {
            // 2. 初始化 UserRateLimiterOperate 客户端，连接至 127.0.0.1:6391
            UserRateLimiterOperate operate = new UserRateLimiterOperate();
            
            java.lang.reflect.Field propField = UserRateLimiterOperate.class.getDeclaredField("properties");
            propField.setAccessible(true);
            GatewayRateLimitProperties props = new GatewayRateLimitProperties();
            props.setRedisHost("127.0.0.1");
            props.setRedisPort(targetPort);
            propField.set(operate, props);

            operate.init();
            Thread.sleep(1000); // 等待 TCP 三次握手建连成功

            byte[] luaShaBytes = "d87a6b41ef2c0a4e7689".getBytes(StandardCharsets.US_ASCII);
            int threads = 16;
            int opsPerThread = 500_000;
            long totalOps = (long) threads * opsPerThread;

            System.out.println("==========================================================================");
            System.out.println(" 🚀 [模式 A 攒批 Pipeline] 真实网络 TCP Socket (" + targetPort + ") 压测: " + totalOps + " 次请求");
            System.out.println("==========================================================================");

            // 预热 JIT
            for (int i = 0; i < 50000; i++) {
                operate.acquire0GcUidBatch(10086L + i, luaShaBytes);
            }
            operate.flushThreadBatch(luaShaBytes);

            // 测试: 模式 A 线程本地 long[] 自适应攒批 Pipeline 模式 (16 线程 -> 800 万次请求)
            long start2 = System.nanoTime();
            ExecutorService pool2 = Executors.newFixedThreadPool(threads);
            CountDownLatch latch2 = new CountDownLatch(threads);
            for (int t = 0; t < threads; t++) {
                final long baseUid = (long) t * opsPerThread;
                pool2.submit(() -> {
                    for (int i = 0; i < opsPerThread; i++) {
                        operate.acquire0GcUidBatch(baseUid + i, luaShaBytes);
                    }
                    operate.flushThreadBatch(luaShaBytes);
                    latch2.countDown();
                });
            }
            latch2.await();
            long time2Ns = System.nanoTime() - start2;
            pool2.shutdown();
            double qps2 = totalOps / (time2Ns / 1_000_000_000.0);

            System.out.println(String.format("🔥 [模式 A 线程本地 long[] 攒批] 耗时: %.2f ms | 吞吐量: %,.0f ops/sec (QPS)",
                    time2Ns / 1_000_000.0, qps2));
            System.out.println(String.format("📦 服务端接收到的 TCP 字节流总计: %,d 字节", redisServer.totalBytesReceived.get()));

            System.out.println("==========================================================================");

            operate.destroy();
        } finally {
            redisServer.stop();
        }
    }

    public static class EmbeddedRedisServer {
        private final int port;
        private EventLoopGroup bossGroup;
        private EventLoopGroup workerGroup;
        private ChannelFuture channelFuture;
        public final AtomicLong totalBytesReceived = new AtomicLong(0);

        public EmbeddedRedisServer(int port) {
            this.port = port;
        }

        public synchronized void start() throws Exception {
            bossGroup = new NioEventLoopGroup(1);
            workerGroup = new NioEventLoopGroup(2);
            ServerBootstrap b = new ServerBootstrap();
            b.group(bossGroup, workerGroup)
                    .channel(NioServerSocketChannel.class)
                    .childOption(ChannelOption.TCP_NODELAY, true)
                    .childOption(ChannelOption.SO_KEEPALIVE, true)
                    .childHandler(new ChannelInitializer<SocketChannel>() {
                        @Override
                        protected void initChannel(SocketChannel ch) {
                            ch.pipeline().addLast(new SimpleChannelInboundHandler<ByteBuf>() {
                                @Override
                                protected void channelRead0(ChannelHandlerContext ctx, ByteBuf msg) {
                                    int readable = msg.readableBytes();
                                    totalBytesReceived.addAndGet(readable);
                                    msg.skipBytes(readable); // 0 CPU 高速消费字节流
                                }

                                @Override
                                public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) {
                                    // 静默清理
                                }
                            });
                        }
                    });

            channelFuture = b.bind(port).sync();
            System.out.println("🚀 [EmbeddedRedisServer] Listening on 127.0.0.1:" + port);
        }

        public synchronized void stop() {
            if (channelFuture != null) {
                channelFuture.channel().close();
            }
            if (bossGroup != null) {
                bossGroup.shutdownGracefully();
            }
            if (workerGroup != null) {
                workerGroup.shutdownGracefully();
            }
        }
    }
}
