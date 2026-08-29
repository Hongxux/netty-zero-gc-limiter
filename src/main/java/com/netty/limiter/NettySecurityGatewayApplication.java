package com.netty.limiter;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

/**
 * @description: 高性能 0-GC 极前置安全网关 / RLS 独立服务启动类
 **/
@SpringBootApplication
public class NettySecurityGatewayApplication {

    public static void main(String[] args) {
        SpringApplication.run(NettySecurityGatewayApplication.class, args);
    }
}
