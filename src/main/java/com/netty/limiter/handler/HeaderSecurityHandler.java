package com.netty.limiter.handler;

import com.netty.limiter.cache.LocalBanCache;
import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;

/**
 * @description: 0-GC 极速 Header 解析处理器接口
 **/
public interface HeaderSecurityHandler {

    void processHeaderValue(ByteBuf buf, int valueStart, int maxLen, ChannelHandlerContext ctx, LocalBanCache localBanCache);

    int getHandlerFlag();
}
