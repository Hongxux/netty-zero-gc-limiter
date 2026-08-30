package com.netty.limiter.handler.headerSecurityHandler;

import com.netty.limiter.cache.LocalBanCache;
import com.netty.limiter.handler.HeaderSecurityHandler;
import com.netty.limiter.util.SecurityAttributeKeys;
import com.netty.limiter.util.jwt.ZeroGcJwtParser;
import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;

/**
 * @description: JWT / Authorization Token 中的 UID 0-GC 提取与黑名单校验单例
 **/
public enum JwtUidHeaderSecurityHandler implements HeaderSecurityHandler {

    INSTANCE;

    @Override
    public void processHeaderValue(ByteBuf buf, int valueStart, int maxLen, ChannelHandlerContext ctx, LocalBanCache localBanCache) {
        long userId = ZeroGcJwtParser.INSTANCE.authenticateJwtAndExtractUid(buf, valueStart, maxLen);
        if (userId <= 0) {
            return;
        }

        if (ctx != null) {
            ctx.channel().attr(SecurityAttributeKeys.USER_ID).set(userId);
        }
    }

    @Override
    public int getHandlerFlag() {
        return 1 << 1;
    }
}
