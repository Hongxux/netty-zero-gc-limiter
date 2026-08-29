package com.netty.limiter.handler;

import io.netty.channel.ChannelPipeline;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.web.embedded.netty.NettyServerCustomizer;
import reactor.netty.http.server.HttpServer;

/**
 * @description: 自动挂载自定义 Security Handler 到 Reactor-Netty Pipeline 的最前端
 **/
@Slf4j
public class NettySecurityCustomizer implements NettyServerCustomizer {

    private final NettyJwtHeaderAccumulatorHandler jwtAccumulatorHandler;
    private final NettyInboundSecurityHandler securityHandler;

    @Autowired
    public NettySecurityCustomizer(NettyJwtHeaderAccumulatorHandler jwtAccumulatorHandler,
                                   NettyInboundSecurityHandler securityHandler) {
        this.jwtAccumulatorHandler = jwtAccumulatorHandler;
        this.securityHandler = securityHandler;
    }

    @Override
    public HttpServer apply(HttpServer httpServer) {
        return httpServer.doOnConnection(connection -> {
            ChannelPipeline pipeline = connection.channel().pipeline();
            if (pipeline.get("zeroGcSecurityHandler") == null) {
                pipeline.addFirst("zeroGcSecurityHandler", securityHandler);
                pipeline.addFirst("zeroGcJwtAccumulatorHandler", jwtAccumulatorHandler);
                log.debug("Successfully added NettyJwtHeaderAccumulatorHandler and NettyInboundSecurityHandler to pipeline head.");
            }
        });
    }
}
