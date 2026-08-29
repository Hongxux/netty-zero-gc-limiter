package com.netty.limiter.handler;

import com.netty.limiter.cache.LocalBanCache;
import com.netty.limiter.listener.RateLimitEventListener;
import com.netty.limiter.limiter.LocalGlobalRateLimiter;
import com.netty.limiter.limiter.UserRateLimiterOperate;
import com.netty.limiter.util.SecurityAttributeKeys;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.ChannelFutureListener;
import io.netty.channel.ChannelHandler;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.nio.charset.StandardCharsets;

import com.netty.limiter.util.SecurityResponses;

/**
 * @description: Netty 极前置 0-GC 限流与黑名单防线 Handler
 **/
@Slf4j
@Component
@ChannelHandler.Sharable
public class NettyInboundSecurityHandler extends ChannelInboundHandlerAdapter {

    @Autowired
    private LocalGlobalRateLimiter localGlobalRateLimiter;

    @Autowired
    private JwtHeaderSecurityHandler jwtHeaderSecurityHandler;

    @Autowired
    private LocalBanCache localBanCache;

    @Autowired
    private UserRateLimiterOperate userRateLimiterOperate;

    @Autowired(required = false)
    private RateLimitEventListener eventListener;

    @Override
    public void channelRead(ChannelHandlerContext ctx, Object msg) throws Exception {
        if (!(msg instanceof ByteBuf)) {
            ctx.fireChannelRead(msg);
            return;
        }

        ByteBuf downstreamBuf = (ByteBuf) msg;

        try {
            // 防线 1: 节点级 0-GC 无锁令牌桶限流
            if (!localGlobalRateLimiter.tryAcquire()) {
                rejectAndRelease(ctx, downstreamBuf, 429, SecurityResponses.RESPONSE_429, "Global Rate Limit Exceeded");
                return;
            }

            // 防线 2: JWT Header 0-GC 极速解析与 UID 黑名单检查 (针对 JWT，从当前 readerIndex 直接探查)
            LocalBanCache.BanInfo banInfo = jwtHeaderSecurityHandler.authenticateJwtAndCheckBan(downstreamBuf, ctx, localBanCache);
            if (banInfo != null) {
                boolean isInvalidJwt = (banInfo == JwtHeaderSecurityHandler.INVALID_JWT_BAN);
                int statusCode = isInvalidJwt ? 401 : 403;
                ByteBuf responseBuf = isInvalidJwt ? SecurityResponses.RESPONSE_401 : SecurityResponses.RESPONSE_403;

                if (isInvalidJwt) {
                    // 如果识别到非法/畸形/已过期的 JWT 攻击，自动将客户端 IP 加入本地黑名单并关闭连接
                    String clientIp = ctx.channel().attr(SecurityAttributeKeys.CLIENT_IP).get();
                    if (clientIp != null && !clientIp.isEmpty()) {
                        localBanCache.putBanInfo(clientIp, "Banned due to Invalid JWT Attack", 300);
                    }
                }

                rejectAndRelease(ctx, downstreamBuf, statusCode, responseBuf, banInfo.getMessage());
                return;
            }

            // 防线 3: Per-UID 0-GC RESP2 异步上报 (Async Offloading + Pipeline 攒批)
            Long userId = ctx.channel().attr(SecurityAttributeKeys.USER_ID).get();
            if (userId != null && userId > 0) {
                userRateLimiterOperate.acquire0GcUidBatch(userId, com.netty.limiter.util.LuaSha1Util.DEFAULT_LUA_SHA1_BYTES);
            }

            // 安全校验全部通过，重置 readerIndex 为 0 并透传给下游 Pipeline
            fireDownstream(ctx, downstreamBuf);
        } catch (Throwable t) {
            downstreamBuf.release();
            throw t;
        }
    }

    private void fireDownstream(ChannelHandlerContext ctx, ByteBuf buf) {
        ctx.channel().attr(SecurityAttributeKeys.HEADER_PASSED).set(true);
        buf.readerIndex(0); // 重置 readerIndex 为 0，准备透传给下游 Pipeline (如 HttpCodec)
        ctx.fireChannelRead(buf);
    }

    private void rejectAndRelease(ChannelHandlerContext ctx, ByteBuf buf, int code, ByteBuf responseBuf, String reason) {
        notifyListener(ctx, code, reason);
        sendResponseAndClose(ctx, responseBuf.retainedDuplicate());
        buf.release();
    }

    private void notifyListener(ChannelHandlerContext ctx, int code, String reason) {
        if (eventListener != null) {
            String clientIp = ctx.channel().attr(SecurityAttributeKeys.CLIENT_IP).get();
            Long userId = ctx.channel().attr(SecurityAttributeKeys.USER_ID).get();
            eventListener.onRateLimitTriggered(clientIp != null ? clientIp : "", userId != null ? userId : 0L, code, reason);
        }
    }

    private void sendResponseAndClose(ChannelHandlerContext ctx, ByteBuf responseBuf) {
        ctx.writeAndFlush(responseBuf).addListener(ChannelFutureListener.CLOSE);
    }
}
