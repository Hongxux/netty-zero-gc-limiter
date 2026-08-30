package com.netty.limiter.handler;

import com.netty.limiter.listener.RateLimitEventListener;
import com.netty.limiter.util.SecurityAttributeKeys;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.ChannelDuplexHandler;
import io.netty.channel.ChannelHandler;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import io.netty.channel.ChannelPromise;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.nio.charset.StandardCharsets;

import com.netty.limiter.util.SecurityResponses;

import com.netty.limiter.listener.RateLimitReasonCodes;

/**
 * @description: 0-GC 极速 JWT Header 拆包/半包与前置鉴权 ChannelDuplexHandler
 * 挂载在 Netty Pipeline 最前端，负责 TCP 拆包聚合，只有扫描定位到有效 JWT 行后再向后透传。
 **/
@Slf4j
@Component
@ChannelHandler.Sharable
public class NettyJwtHeaderAccumulatorHandler extends ChannelDuplexHandler {

    @Autowired
    private JwtHeaderSecurityHandler jwtHeaderSecurityHandler;

    @Autowired(required = false)
    private RateLimitEventListener eventListener;

    @Override
    public void channelRead(ChannelHandlerContext ctx, Object msg) throws Exception {
        if (Boolean.TRUE.equals(ctx.channel().attr(SecurityAttributeKeys.HEADER_PASSED).get())) {
            // 已进入 Body 阶段且已检验过 JWT 合法未被限流，直接透传后续 Body 数据给下游 HttpCodec
            ctx.fireChannelRead(msg);
            return;
        }

        if (!(msg instanceof ByteBuf)) {
            ctx.fireChannelRead(msg);
            return;
        }

        ByteBuf input = (ByteBuf) msg;
        ByteBuf cumulation = ctx.channel().attr(SecurityAttributeKeys.CUMULATION).get();

        ByteBuf downstreamBuf;

        if (cumulation == null) {
            // 没有先前的积压包
            if (jwtHeaderSecurityHandler.isHeaderCompleteOrJwtFound(input, ctx)) {
                // 99.9% 快路径聚合完成：
                if (isJwtFoundInHeader(input)) {
                    // 状态 A：首包找到有效 JWT 行 (readerIndex 停留在 JWT 行开头)，透传给后续限流 Handler
                    downstreamBuf = input;
                } else {
                    // 状态 B：首包 Header 已收齐 (\r\n\r\n)，但未找到有效 JWT -> 直接响应 401 Unauthorized 并释放 ByteBuf
                    rejectAndRelease(ctx, input, 401, SecurityResponses.RESPONSE_401, RateLimitReasonCodes.REASON_ANONYMOUS_UNAUTHORIZED);
                    return;
                }
            } else {
                // 半包/拆包：将首包输入写入 cumulation 并保留已扫描的水位线位置
                initCumulation(ctx, input);
                return;
            }
        } else {
            // 之前已有积压的半包数据，将后续 TCP 分包写入 cumulation 并释放 input
            appendCumulation(cumulation, input);

            if (!jwtHeaderSecurityHandler.isHeaderCompleteOrJwtFound(cumulation, ctx)) {
                // 仍未收齐 JWT 行或 Header 结束标志 \r\n\r\n
                // 仅在 Header 未收齐且未找到 JWT 时校验已积压 Header 长度是否超限（防范 Slowloris 慢速 Header 攻击）
                if (isHeaderSizeExceeded(cumulation)) {
                    rejectExceededHeader(ctx, cumulation);
                    return;
                }
                // 仍未收齐且未超限，继续等待后续 TCP 包到达（readerIndex 已自动保留水位线）
                return;
            }

            // 聚合完成，清除 CUMULATION Attribute
            ctx.channel().attr(SecurityAttributeKeys.CUMULATION).set(null);

            if (isJwtFoundInHeader(cumulation)) {
                // 状态 A：多包积压聚合后找到有效 JWT 行 (readerIndex 停留在 JWT 行开头)
                downstreamBuf = cumulation;
            } else {
                // 状态 B：多包积压聚合后 Header 已收齐 (\r\n\r\n)，但未找到有效 JWT -> 直接响应 401 Unauthorized 并释放 ByteBuf
                rejectAndRelease(ctx, cumulation, 401, SecurityResponses.RESPONSE_401, RateLimitReasonCodes.REASON_ANONYMOUS_UNAUTHORIZED);
                return;
            }
        }

        // 成功定位找到 JWT，向 Pipeline 后续 Handler 透传
        ctx.fireChannelRead(downstreamBuf);
    }

    @Override
    public void write(ChannelHandlerContext ctx, Object msg, ChannelPromise promise) throws Exception {
        // 当服务器响应数据开始写回客户端时，表明当前 HTTP 请求已结束，重置 HEADER_PASSED 为 false 以支持 HTTP Keep-Alive
        ctx.channel().attr(SecurityAttributeKeys.HEADER_PASSED).set(false);
        super.write(ctx, msg, promise);
    }

    private boolean isJwtFoundInHeader(ByteBuf buf) {
        return buf.readerIndex() < buf.writerIndex();
    }

    private void initCumulation(ChannelHandlerContext ctx, ByteBuf input) {
        int scannedPos = input.readerIndex();
        input.readerIndex(0); // 重置 input 以复制全部数据到 cumulation

        ByteBuf cumulation = ctx.alloc().buffer(input.readableBytes() + 1024);
        cumulation.writeBytes(input);
        input.release();

        // 恢复已扫描的水位线位置，避免后续 TCP 包到达时重复扫描已确认无 JWT 的 Header 行
        cumulation.readerIndex(scannedPos);
        ctx.channel().attr(SecurityAttributeKeys.CUMULATION).set(cumulation);
    }

    private void appendCumulation(ByteBuf cumulation, ByteBuf input) {
        cumulation.writeBytes(input);
        input.release();
    }

    private boolean isHeaderSizeExceeded(ByteBuf cumulation) {
        return cumulation.writerIndex() > JwtHeaderSecurityHandler.MAX_HEADER_SIZE;
    }

    private void rejectExceededHeader(ChannelHandlerContext ctx, ByteBuf cumulation) {
        ctx.channel().attr(SecurityAttributeKeys.CUMULATION).set(null);
        cumulation.release();
        notifyListener(ctx, 400, RateLimitReasonCodes.REASON_HEADER_ACCUMULATION_OVERFLOW);
        sendResponseAndClose(ctx, SecurityResponses.RESPONSE_400.retainedDuplicate());
    }

    private void rejectAndRelease(ChannelHandlerContext ctx, ByteBuf buf, int code, ByteBuf responseBuf, int reasonCode) {
        notifyListener(ctx, code, reasonCode);
        sendResponseAndClose(ctx, responseBuf.retainedDuplicate());
        buf.release();
    }

    private void sendResponseAndClose(ChannelHandlerContext ctx, ByteBuf responseBuf) {
        ctx.writeAndFlush(responseBuf).addListener(io.netty.channel.ChannelFutureListener.CLOSE);
    }

    private void notifyListener(ChannelHandlerContext ctx, int code, int reasonCode) {
        if (eventListener != null) {
            try {
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
            } catch (Exception e) {
                log.error("Failed to notify RateLimitEventListener", e);
            }
        }
    }

    @Override
    public void channelInactive(ChannelHandlerContext ctx) throws Exception {
        cleanupCumulation(ctx);
        super.channelInactive(ctx);
    }

    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) throws Exception {
        cleanupCumulation(ctx);
        super.exceptionCaught(ctx, cause);
    }

    private void cleanupCumulation(ChannelHandlerContext ctx) {
        ctx.channel().attr(SecurityAttributeKeys.HEADER_PASSED).set(null);
        ByteBuf cumulation = ctx.channel().attr(SecurityAttributeKeys.CUMULATION).getAndSet(null);
        if (cumulation != null) {
            cumulation.release();
        }
    }
}
