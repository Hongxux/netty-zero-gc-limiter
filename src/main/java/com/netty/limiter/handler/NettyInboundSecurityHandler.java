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

            // 防线 3: Per-UID 限流上报与降级校验 (双版本：普通 0-GC 异步 offload vs 80% 预警 -2L 同步等待 Ack)
            Long userId = ctx.channel().attr(SecurityAttributeKeys.USER_ID).get();
            if (userId != null && userId > 0 && userRateLimiterOperate != null) {
                int userBanStatus = localBanCache.getUserBanStatus(userId);
                if (userBanStatus == LocalBanCache.BAN_STATUS_HARD_BANNED) {
                    rejectAndRelease(ctx, downstreamBuf, 403, SecurityResponses.RESPONSE_403, RateLimitReasonCodes.REASON_LOCAL_BAN);
                    return;
                } else if (userBanStatus == LocalBanCache.BAN_STATUS_WARNED_SYNC_REQUIRED) {
                    // 🚨 80% 水位降级预警 (ExpSec == -2L)：执行【0-GC 响应式异步无阻塞校验 + TCP 物理反压】
                    suspendAndAcquireReactiveRateLimit(ctx, downstreamBuf, userId);
                    return; // 立即返回挂起当前 Pipeline，等待异步唤醒回调继续执行！
                } else {
                    // 🚀 普通未预警 UID：执行【异步非阻塞上报版本】(Async Offloading + Pipeline 攒批)
                    userRateLimiterOperate.acquireBatchOffload(userId, com.netty.limiter.util.LuaSha1Util.DEFAULT_LUA_SHA1_BYTES);
                }
            }

            // 安全校验全部通过，重置 readerIndex 为 0 并透传给下游 Pipeline
            fireDownstream(ctx, downstreamBuf);
        } catch (Throwable t) {
            downstreamBuf.release();
            throw t;
        }
    }

    /**
     * 🚀 核心架构：挂起当前 Pipeline 并进入【0-GC 响应式异步无阻塞校验 + TCP 物理反压】
     *
     * 机制拆解：
     * 1. 物理反压 (TCP Backpressure): 暂停当前 TCP Socket 读取，通过 TCP 零窗口将内存压力反推给客户端；
     * 2. 借出续体上下文 (Continuation Context): 从 Recycler 借出 0-GC 异步上下文保存 httpCtx、downstreamBuf 与 userId；
     * 3. 绑定续体恢复函数 (resumeContinuation): Redis 仲裁完毕后恢复 autoRead(true) 并裁决流水线走向；
     * 4. 异步发射 EVALSHA: 将上下文排入 RingBuffer 发送给 Redis，当前 EventLoop 线程立即解放。
     */
    private void suspendAndAcquireReactiveRateLimit(ChannelHandlerContext ctx, ByteBuf downstreamBuf, long userId) {
        // 1. 物理反压：暂停当前 TCP Socket 读取
        ctx.channel().config().setAutoRead(false);

        // 2. 从对象池借出 0-GC 异步上下文，绑定续体恢复函数
        com.netty.limiter.limiter.AsyncRateLimitContext asyncCtx =
                com.netty.limiter.limiter.AsyncRateLimitContext.acquire(ctx, downstreamBuf, userId,
                        (granted) -> resumeContinuation(ctx, downstreamBuf, userId, granted));

        // 3. 异步提交给 Redis 驱动，当前线程立即返回
        userRateLimiterOperate.acquireReactiveAsync(asyncCtx);
    }

    /**
     * 🎯 续体恢复与结果裁决 (Resume Pipeline Continuation):
     * 1. 解除 TCP 物理反压 (setAutoRead(true))；
     * 2. 若 Redis 仲裁放行：触发 fireDownstream 恢复下游 Pipeline 流水线；
     * 3. 若 Redis 仲裁拒绝/超时：升级本地硬封禁缓存，并回写 403 响应切断连接。
     */
    private void resumeContinuation(ChannelHandlerContext ctx, ByteBuf downstreamBuf, long userId, boolean isAllowed) {
        // 1. 恢复 TCP Socket 自动读取 (解除物理反压)
        ctx.channel().config().setAutoRead(true);

        if (isAllowed) {
            // 🎯 续体恢复：校验通过，继续推进下游 Pipeline
            fireDownstream(ctx, downstreamBuf);
        } else {
            // 🛡️ 拦截分支：升级为本地硬封禁并直接给客户端回写 403
            localBanCache.putUserBan(userId, 60);
            rejectAndRelease(ctx, downstreamBuf, 403, SecurityResponses.RESPONSE_403, RateLimitReasonCodes.REASON_LOCAL_BAN);
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
