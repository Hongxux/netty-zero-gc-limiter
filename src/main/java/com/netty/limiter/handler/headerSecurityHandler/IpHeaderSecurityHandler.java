package com.netty.limiter.handler.headerSecurityHandler;

import com.netty.limiter.cache.LocalBanCache;
import com.netty.limiter.handler.HeaderSecurityHandler;
import com.netty.limiter.util.SecurityAttributeKeys;
import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;

/**
 * @description: IP 字段解析与 IP 黑名单校验处理器单例
 **/
public enum IpHeaderSecurityHandler implements HeaderSecurityHandler {

    INSTANCE;

    private static final io.netty.util.concurrent.FastThreadLocal<long[]> IPV4_HOLDER = new io.netty.util.concurrent.FastThreadLocal<long[]>() {
        @Override
        protected long[] initialValue() {
            return new long[1];
        }
    };

    private static final io.netty.util.concurrent.FastThreadLocal<long[]> IPV6_HOLDER = new io.netty.util.concurrent.FastThreadLocal<long[]>() {
        @Override
        protected long[] initialValue() {
            return new long[2];
        }
    };

    @Override
    public void processHeaderValue(ByteBuf buf, int valueStart, int maxLen, ChannelHandlerContext ctx, LocalBanCache localBanCache) {
        long[] ip4Out = IPV4_HOLDER.get();
        long[] ip6Out = IPV6_HOLDER.get();

        int ipType = com.netty.limiter.util.ZeroGcNumberUtil.parseIp(buf, valueStart, maxLen, ip4Out, ip6Out);

        if (ctx != null) {
            if (ipType == com.netty.limiter.util.ZeroGcNumberUtil.IP_TYPE_V4) {
                ctx.channel().attr(SecurityAttributeKeys.CLIENT_IPV4_LONG).set(ip4Out[0]);
            } else if (ipType == com.netty.limiter.util.ZeroGcNumberUtil.IP_TYPE_V6) {
                ctx.channel().attr(SecurityAttributeKeys.CLIENT_IPV6_HIGH).set(ip6Out[0]);
                ctx.channel().attr(SecurityAttributeKeys.CLIENT_IPV6_LOW).set(ip6Out[1]);
            }
        }
    }

    @Override
    public int getHandlerFlag() {
        return 1 << 0;
    }
}
