package com.netty.limiter.util;

import io.netty.buffer.ByteBuf;

/**
 * @description: 0-GC 64-bit xxHash64 哈希算法工具类 (直接在 Netty ByteBuf 与 byte[] 原语数组上运行)
 **/
public class XxHash64Util {

    private static final long PRIME64_1 = 0x9E3779B185EBCA87L;
    private static final long PRIME64_2 = 0xC2B2AE3D27D4EB4FL;
    private static final long PRIME64_3 = 0x165667B19E3779F9L;
    private static final long PRIME64_4 = 0x85EBCA77C2B2AE63L;
    private static final long PRIME64_5 = 0x27D4EB2F165667C5L;

    /**
     * 0-GC 64-bit xxHash64 哈希算法：直接在 Netty ByteBuf 上按 64 位 Little-Endian Word 并行计算
     */
    public static long fastHash64(ByteBuf buf, int start, int end) {
        int len = end - start;
        if (len <= 0) return 0L;

        long hash;
        int p = start;

        if (len >= 32) {
            int limit = end - 32;
            long v1 = PRIME64_1 + PRIME64_2;
            long v2 = PRIME64_2;
            long v3 = 0L;
            long v4 = -PRIME64_1;

            do {
                v1 += buf.getLongLE(p) * PRIME64_2;
                v1 = Long.rotateLeft(v1, 31);
                v1 *= PRIME64_1;
                p += 8;

                v2 += buf.getLongLE(p) * PRIME64_2;
                v2 = Long.rotateLeft(v2, 31);
                v2 *= PRIME64_1;
                p += 8;

                v3 += buf.getLongLE(p) * PRIME64_2;
                v3 = Long.rotateLeft(v3, 31);
                v3 *= PRIME64_1;
                p += 8;

                v4 += buf.getLongLE(p) * PRIME64_2;
                v4 = Long.rotateLeft(v4, 31);
                v4 *= PRIME64_1;
                p += 8;
            } while (p <= limit);

            hash = Long.rotateLeft(v1, 1) + Long.rotateLeft(v2, 7) + Long.rotateLeft(v3, 12) + Long.rotateLeft(v4, 18);

            v1 *= PRIME64_2; v1 = Long.rotateLeft(v1, 31); v1 *= PRIME64_1; hash ^= v1; hash = hash * PRIME64_1 + PRIME64_4;
            v2 *= PRIME64_2; v2 = Long.rotateLeft(v2, 31); v2 *= PRIME64_1; hash ^= v2; hash = hash * PRIME64_1 + PRIME64_4;
            v3 *= PRIME64_2; v3 = Long.rotateLeft(v3, 31); v3 *= PRIME64_1; hash ^= v3; hash = hash * PRIME64_1 + PRIME64_4;
            v4 *= PRIME64_2; v4 = Long.rotateLeft(v4, 31); v4 *= PRIME64_1; hash ^= v4; hash = hash * PRIME64_1 + PRIME64_4;
        } else {
            hash = PRIME64_5;
        }

        hash += len;

        while (p + 8 <= end) {
            long k1 = buf.getLongLE(p);
            k1 *= PRIME64_2;
            k1 = Long.rotateLeft(k1, 31);
            k1 *= PRIME64_1;
            hash ^= k1;
            hash = Long.rotateLeft(hash, 27) * PRIME64_1 + PRIME64_4;
            p += 8;
        }

        if (p + 4 <= end) {
            hash ^= (buf.getIntLE(p) & 0xFFFFFFFFL) * PRIME64_1;
            hash = Long.rotateLeft(hash, 23) * PRIME64_2 + PRIME64_3;
            p += 4;
        }

        while (p < end) {
            hash ^= (buf.getByte(p) & 0xFF) * PRIME64_5;
            hash = Long.rotateLeft(hash, 11) * PRIME64_1;
            p++;
        }

        hash ^= hash >>> 33;
        hash *= PRIME64_2;
        hash ^= hash >>> 29;
        hash *= PRIME64_3;
        hash ^= hash >>> 32;

        return hash;
    }

    /**
     * 0-GC 64-bit xxHash64 哈希算法：支持对 byte[] 字节数组计算与 ByteBuf 绝对对齐的 xxHash64 值
     */
    public static long xxHash64(byte[] b, int offset, int len) {
        if (b == null || len <= 0) return 0L;
        int end = offset + len;

        long hash;
        int p = offset;

        if (len >= 32) {
            int limit = end - 32;
            long v1 = PRIME64_1 + PRIME64_2;
            long v2 = PRIME64_2;
            long v3 = 0L;
            long v4 = -PRIME64_1;

            do {
                v1 += getLongLE(b, p) * PRIME64_2;
                v1 = Long.rotateLeft(v1, 31);
                v1 *= PRIME64_1;
                p += 8;

                v2 += getLongLE(b, p) * PRIME64_2;
                v2 = Long.rotateLeft(v2, 31);
                v2 *= PRIME64_1;
                p += 8;

                v3 += getLongLE(b, p) * PRIME64_2;
                v3 = Long.rotateLeft(v3, 31);
                v3 *= PRIME64_1;
                p += 8;

                v4 += getLongLE(b, p) * PRIME64_2;
                v4 = Long.rotateLeft(v4, 31);
                v4 *= PRIME64_1;
                p += 8;
            } while (p <= limit);

            hash = Long.rotateLeft(v1, 1) + Long.rotateLeft(v2, 7) + Long.rotateLeft(v3, 12) + Long.rotateLeft(v4, 18);

            v1 *= PRIME64_2; v1 = Long.rotateLeft(v1, 31); v1 *= PRIME64_1; hash ^= v1; hash = hash * PRIME64_1 + PRIME64_4;
            v2 *= PRIME64_2; v2 = Long.rotateLeft(v2, 31); v2 *= PRIME64_1; hash ^= v2; hash = hash * PRIME64_1 + PRIME64_4;
            v3 *= PRIME64_2; v3 = Long.rotateLeft(v3, 31); v3 *= PRIME64_1; hash ^= v3; hash = hash * PRIME64_1 + PRIME64_4;
            v4 *= PRIME64_2; v4 = Long.rotateLeft(v4, 31); v4 *= PRIME64_1; hash ^= v4; hash = hash * PRIME64_1 + PRIME64_4;
        } else {
            hash = PRIME64_5;
        }

        hash += len;

        while (p + 8 <= end) {
            long k1 = getLongLE(b, p);
            k1 *= PRIME64_2;
            k1 = Long.rotateLeft(k1, 31);
            k1 *= PRIME64_1;
            hash ^= k1;
            hash = Long.rotateLeft(hash, 27) * PRIME64_1 + PRIME64_4;
            p += 8;
        }

        if (p + 4 <= end) {
            hash ^= (getIntLE(b, p) & 0xFFFFFFFFL) * PRIME64_1;
            hash = Long.rotateLeft(hash, 23) * PRIME64_2 + PRIME64_3;
            p += 4;
        }

        while (p < end) {
            hash ^= (b[p] & 0xFF) * PRIME64_5;
            hash = Long.rotateLeft(hash, 11) * PRIME64_1;
            p++;
        }

        hash ^= hash >>> 33;
        hash *= PRIME64_2;
        hash ^= hash >>> 29;
        hash *= PRIME64_3;
        hash ^= hash >>> 32;

        return hash;
    }

    private static long getLongLE(byte[] b, int i) {
        return (b[i] & 0xFFL)
                | ((b[i + 1] & 0xFFL) << 8)
                | ((b[i + 2] & 0xFFL) << 16)
                | ((b[i + 3] & 0xFFL) << 24)
                | ((b[i + 4] & 0xFFL) << 32)
                | ((b[i + 5] & 0xFFL) << 40)
                | ((b[i + 6] & 0xFFL) << 48)
                | ((b[i + 7] & 0xFFL) << 56);
    }

    private static int getIntLE(byte[] b, int i) {
        return (b[i] & 0xFF)
                | ((b[i + 1] & 0xFF) << 8)
                | ((b[i + 2] & 0xFF) << 16)
                | ((b[i + 3] & 0xFF) << 24);
    }
}
