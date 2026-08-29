package com.netty.limiter.util;

import io.netty.buffer.ByteBuf;
import io.netty.util.AttributeKey;

/**
 * @description: Netty TCP Channel 属性键集中管理
 **/
public class SecurityAttributeKeys {

    /**
     * 极速解析出的客户端 IP
     */
    public static final AttributeKey<String> CLIENT_IP = AttributeKey.valueOf("NETTY_LIMITER_CLIENT_IP");

    /**
     * 极速解析出的 User ID (primitive long)
     */
    public static final AttributeKey<Long> USER_ID = AttributeKey.valueOf("NETTY_LIMITER_USER_ID");

    /**
     * 0-GC TCP 拆包/半包 ByteBuf 聚合器 buffer
     */
    public static final AttributeKey<ByteBuf> CUMULATION = AttributeKey.valueOf("NETTY_LIMITER_CUMULATION");

    /**
     * 当前 HTTP 请求 Header 是否已完成解析与安全校验（标记是否进入 Body 直通阶段）
     */
    public static final AttributeKey<Boolean> HEADER_PASSED = AttributeKey.valueOf("NETTY_LIMITER_HEADER_PASSED");
}
