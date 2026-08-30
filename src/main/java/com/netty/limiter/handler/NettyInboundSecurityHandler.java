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

import com.netty.limiter.listener.RateLimitReasonCodes;

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
                rejectAndRelease(ctx, downstreamBuf, 429, SecurityResponses.RESPONSE_429, RateLimitReasonCodes.REASON_GLOBAL_RATE_LIMIT);
                return;
            }

            // 防线 2: JWT Header 0-GC 极速解析与 UID 黑名单检查 (原生 int 状态码，0 堆内存分配)
            int banStatus = jwtHeaderSecurityHandler.authenticateJwtAndCheckBanStatus(downstreamBuf, ctx, localBanCache);
            if (banStatus != JwtHeaderSecurityHandler.STATUS_PASSED) {
                if (banStatus == JwtHeaderSecurityHandler.STATUS_USER_BANNED) {
                    rejectAndRelease(ctx, downstreamBuf, 403, SecurityResponses.RESPONSE_403, RateLimitReasonCodes.REASON_LOCAL_BAN);
                    return;
                }

                if (banStatus == JwtHeaderSecurityHandler.STATUS_EXPIRED_JWT) {
                    // Token 自然过期：仅返回 401 Unauthorized，引导客户端重新登录
                    rejectAndRelease(ctx, downstreamBuf, 401, SecurityResponses.RESPONSE_401, RateLimitReasonCodes.REASON_INVALID_JWT);
                    return;
                }

                if (banStatus == JwtHeaderSecurityHandler.STATUS_INVALID_JWT) {
                    // 伪造/篡改/非法签名：直接返回 401 Unauthorized 并关闭连接
                    rejectAndRelease(ctx, downstreamBuf, 401, SecurityResponses.RESPONSE_401, RateLimitReasonCodes.REASON_INVALID_JWT);
                    return;
                }
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

    private void rejectAndRelease(ChannelHandlerContext ctx, ByteBuf buf, int code, ByteBuf responseBuf, int reasonCode) {
        notifyListener(ctx, code, reasonCode);
        sendResponseAndClose(ctx, responseBuf.retainedDuplicate());
        buf.release();
    }

    private void notifyListener(ChannelHandlerContext ctx, int code, int reasonCode) {
        if (eventListener != null) {
            Long ipHighAttr = ctx.channel().attr(SecurityAttributeKeys.CLIENT_IPV6_HIGH).get();
            Long ipLowAttr = ctx.channel().attr(SecurityAttributeKeys.CLIENT_IPV6_LOW).get();
            long ipHigh = ipHighAttr != null ? ipHighAttr : 0L;
            long ipLow = ipLowAttr != null ? ipLowAttr : 0L;
            if (ipHigh == 0L && ipLow == 0L) {
                Long ip4 = ctx.channel().attr(SecurityAttributeKeys.CLIENT_IPV4_LONG).get();
                ipLow = ip4 != null ? ip4 : 0L;
            }
            Long userId = ctx.channel().attr(SecurityAttributeKeys.USER_ID).get();
            eventListener.onRateLimitTriggered(ipHigh, ipLow, userId != null ? userId : 0L, code, reasonCode);
        }
    }

    private void sendResponseAndClose(ChannelHandlerContext ctx, ByteBuf responseBuf) {
        ctx.writeAndFlush(responseBuf).addListener(ChannelFutureListener.CLOSE);
    }
}
