package com.netty.limiter.util.jwt;

import io.netty.buffer.ByteBuf;
import io.netty.util.concurrent.FastThreadLocal;

import javax.crypto.Mac;
import javax.crypto.ShortBufferException;
import javax.crypto.spec.SecretKeySpec;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;

/**
 * @description: 0-GC 极速 HMAC-SHA256 JWT 物理签名鉴权器 (SWAR 64-bit SIMD 字节比对 + FastThreadLocal)
 * 绝对 0 堆内存分配 (Zero Heap Allocation)：
 * 1. 利用 FastThreadLocal 数组索引 O(1) 访问重用 Mac 实例与 32 字节 / 64 字节摘要 / Base64URL 缓冲区。
 * 2. 通过 buf.nioBuffer() 零拷贝获取内存视图注入 CPU 哈希引擎。
 * 3. 采用 SWAR (SIMD) 64-bit Word 级位运算进行 Constant-Time 防侧信道攻击比对。
 **/
public class JwtAuthenticator {

    private static final byte[] DEFAULT_SECRET_KEY = "secret".getBytes(StandardCharsets.UTF_8);
    private static volatile SecretKeySpec CURRENT_SECRET_KEY_SPEC = new SecretKeySpec(DEFAULT_SECRET_KEY, "HmacSHA256");
    private static volatile long SECRET_KEY_VERSION = 1L;

    /**
     * 🚀 物理级 JWT 密钥无缝轮换 (支持运行时从 Spring / Nacos 配置中心拉取并无缝切换 Secret Key)
     */
    public static void updateSecretKey(String secretKey) {
        if (secretKey != null && !secretKey.trim().isEmpty()) {
            byte[] keyBytes = secretKey.trim().getBytes(StandardCharsets.UTF_8);
            CURRENT_SECRET_KEY_SPEC = new SecretKeySpec(keyBytes, "HmacSHA256");
            SECRET_KEY_VERSION++;
        }
    }

    private static class MacHolderWrapper {
        final Mac mac;
        long keyVersion;

        MacHolderWrapper(Mac mac, long keyVersion) {
            this.mac = mac;
            this.keyVersion = keyVersion;
        }
    }

    private static final FastThreadLocal<MacHolderWrapper> HMAC_SHA256_HOLDER = new FastThreadLocal<MacHolderWrapper>() {
        @Override
        protected MacHolderWrapper initialValue() {
            try {
                Mac mac = Mac.getInstance("HmacSHA256");
                SecretKeySpec keySpec = CURRENT_SECRET_KEY_SPEC;
                long ver = SECRET_KEY_VERSION;
                mac.init(keySpec);
                return new MacHolderWrapper(mac, ver);
            } catch (Exception e) {
                throw new IllegalStateException("Failed to initialize HMAC-SHA256 Mac instance", e);
            }
        }
    };

    private static final FastThreadLocal<byte[]> DIGEST_BUFFER_HOLDER = new FastThreadLocal<byte[]>() {
        @Override
        protected byte[] initialValue() {
            return new byte[32];
        }
    };

    private static final FastThreadLocal<byte[]> BASE64URL_BUFFER_HOLDER = new FastThreadLocal<byte[]>() {
        @Override
        protected byte[] initialValue() {
            return new byte[64];
        }
    };

    private static final FastThreadLocal<byte[]> JWT_CONTENT_BUFFER_HOLDER = new FastThreadLocal<byte[]>() {
        @Override
        protected byte[] initialValue() {
            return new byte[2048]; // 预分配 2KB 线程池共享 Byte 数组，绝对 0 堆分配
        }
    };

    private static final byte[] BASE64URL_ALPHABET = "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789-_".getBytes(StandardCharsets.UTF_8);

    /**
     * 0-GC 验证 JWT Signature 是否与 Header + Payload 内容强契合
     */
    public static boolean verifyJwtSignature0Gc(ByteBuf buf, int jwtStart, int dot2Index, int sigStart, int sigEnd) {
        int contentLen = dot2Index - jwtStart;
        int sigLen = sigEnd - sigStart;
        if (sigLen < 40 || sigLen > 64) {
            return false;
        }

        byte[] contentBuf = JWT_CONTENT_BUFFER_HOLDER.get();
        if (contentLen > contentBuf.length) {
            return false;
        }

        MacHolderWrapper wrapper = HMAC_SHA256_HOLDER.get();
        Mac mac = wrapper.mac;
        if (wrapper.keyVersion != SECRET_KEY_VERSION) {
            try {
                mac.init(CURRENT_SECRET_KEY_SPEC);
                wrapper.keyVersion = SECRET_KEY_VERSION;
            } catch (Exception e) {
                return false;
            }
        }
        mac.reset();

        // 🚀 彻底消除 ByteBuf.nioBuffer() 带来的 ByteBuffer 包装对象分配，100% 0-GC 拷贝至 FastThreadLocal 数组
        buf.getBytes(jwtStart, contentBuf, 0, contentLen);
        mac.update(contentBuf, 0, contentLen);

        byte[] digestBuf = DIGEST_BUFFER_HOLDER.get();
        try {
            mac.doFinal(digestBuf, 0);
        } catch (ShortBufferException e) {
            return false;
        }

        byte[] expectedSigBuf = BASE64URL_BUFFER_HOLDER.get();
        int expectedSigLen = encodeBase64Url32Bytes(digestBuf, expectedSigBuf);

        if (expectedSigLen != sigLen) {
            return false;
        }

        return equals64BitWord0Gc(buf, sigStart, expectedSigBuf, expectedSigLen);
    }

    private static int encodeBase64Url32Bytes(byte[] in32, byte[] out64) {
        int inPos = 0;
        int outPos = 0;

        for (int i = 0; i < 10; i++) {
            int b0 = in32[inPos++] & 0xFF;
            int b1 = in32[inPos++] & 0xFF;
            int b2 = in32[inPos++] & 0xFF;

            out64[outPos++] = BASE64URL_ALPHABET[b0 >> 2];
            out64[outPos++] = BASE64URL_ALPHABET[((b0 & 0x03) << 4) | (b1 >> 4)];
            out64[outPos++] = BASE64URL_ALPHABET[((b1 & 0x0F) << 2) | (b2 >> 6)];
            out64[outPos++] = BASE64URL_ALPHABET[b2 & 0x3F];
        }

        int b0 = in32[inPos++] & 0xFF;
        int b1 = in32[inPos++] & 0xFF;
        out64[outPos++] = BASE64URL_ALPHABET[b0 >> 2];
        out64[outPos++] = BASE64URL_ALPHABET[((b0 & 0x03) << 4) | (b1 >> 4)];
        out64[outPos++] = BASE64URL_ALPHABET[(b1 & 0x0F) << 2];

        return 43;
    }

    private static boolean equals64BitWord0Gc(ByteBuf buf, int bufStart, byte[] expected, int len) {
        int words = len >> 3; // len / 8
        int rem = len & 7;    // len % 8

        int pBuf = bufStart;
        int pExp = 0;
        long diff = 0L;

        for (int i = 0; i < words; i++) {
            long wBuf = buf.getLongLE(pBuf);
            long wExp = getLongLE(expected, pExp);
            diff |= (wBuf ^ wExp);
            pBuf += 8;
            pExp += 8;
        }

        for (int i = 0; i < rem; i++) {
            byte bBuf = buf.getByte(pBuf + i);
            byte bExp = expected[pExp + i];
            diff |= (bBuf ^ bExp);
        }

        return diff == 0L;
    }

    private static long getLongLE(byte[] b, int off) {
        return (b[off] & 0xFFL)
                | ((b[off + 1] & 0xFFL) << 8)
                | ((b[off + 2] & 0xFFL) << 16)
                | ((b[off + 3] & 0xFFL) << 24)
                | ((b[off + 4] & 0xFFL) << 32)
                | ((b[off + 5] & 0xFFL) << 40)
                | ((b[off + 6] & 0xFFL) << 48)
                | ((b[off + 7] & 0xFFL) << 56);
    }
}
