package com.netty.limiter.handler.headerSecurityHandler;

import com.netty.limiter.cache.LocalBanCache;
import com.netty.limiter.handler.HeaderSecurityHandler;
import com.netty.limiter.util.SecurityAttributeKeys;
import com.netty.limiter.util.ZeroGcIpParser;
import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;

/**
 * @description: IP 字段解析与 IP 黑名单校验处理器单例
 **/
public enum IpHeaderSecurityHandler implements HeaderSecurityHandler {

    INSTANCE;

    @Override
    public LocalBanCache.BanInfo processHeaderValue(ByteBuf buf, int valueStart, int maxLen, ChannelHandlerContext ctx, LocalBanCache localBanCache) {
        long ipLong = ZeroGcIpParser.INSTANCE.parseIpToLong(buf, valueStart, maxLen);
        if (ipLong <= 0) {
            return null;
        }

        if (ctx != null) {
            String ipStr = parseIpLongToString(ipLong);
            ctx.channel().attr(SecurityAttributeKeys.CLIENT_IP).set(ipStr);
        }

        return null;
    }

    private String parseIpLongToString(long ipLong) {
        return ((ipLong >> 24) & 0xFF) + "." +
               ((ipLong >> 16) & 0xFF) + "." +
               ((ipLong >> 8) & 0xFF) + "." +
               (ipLong & 0xFF);
    }

    @Override
    public int getHandlerFlag() {
        return 1 << 0;
    }
}
