package com.netty.limiter.util;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;

import java.nio.charset.StandardCharsets;

/**
 * @description: 网关安全响应 0-GC 共享常量库 (全预构建为 Unpooled.unreleasableBuffer 线程安全共享对象)
 **/
public final class SecurityResponses {

    private SecurityResponses() {}

    public static final ByteBuf RESPONSE_400 = Unpooled.unreleasableBuffer(Unpooled.copiedBuffer(
            "HTTP/1.1 400 Bad Request\r\n" +
            "Content-Type: application/json;charset=UTF-8\r\n" +
            "Content-Length: 47\r\n" +
            "Connection: close\r\n\r\n" +
            "{\"code\":400,\"message\":\"Header Length Exceeded\"}",
            StandardCharsets.UTF_8
    ));

    public static final ByteBuf RESPONSE_401 = Unpooled.unreleasableBuffer(Unpooled.copiedBuffer(
            "HTTP/1.1 401 Unauthorized\r\n" +
            "Content-Type: application/json;charset=UTF-8\r\n" +
            "Content-Length: 64\r\n" +
            "Connection: close\r\n\r\n" +
            "{\"code\":401,\"message\":\"Unauthorized: Missing or Invalid Token\"}",
            StandardCharsets.UTF_8
    ));

    public static final ByteBuf RESPONSE_403 = Unpooled.unreleasableBuffer(Unpooled.copiedBuffer(
            "HTTP/1.1 403 Forbidden\r\n" +
            "Content-Type: application/json;charset=UTF-8\r\n" +
            "Content-Length: 54\r\n" +
            "Connection: close\r\n\r\n" +
            "{\"code\":403,\"message\":\"Access Denied! IP/UID Banned.\"}",
            StandardCharsets.UTF_8
    ));

    public static final ByteBuf RESPONSE_429 = Unpooled.unreleasableBuffer(Unpooled.copiedBuffer(
            "HTTP/1.1 429 Too Many Requests\r\n" +
            "Content-Type: application/json;charset=UTF-8\r\n" +
            "Content-Length: 60\r\n" +
            "Connection: close\r\n\r\n" +
            "{\"code\":429,\"message\":\"System is busy, rate limit exceeded!\"}",
            StandardCharsets.UTF_8
    ));
}
