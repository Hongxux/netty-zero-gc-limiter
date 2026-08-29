package com.netty.limiter.util;

import io.netty.buffer.ByteBuf;

/**
 * @description: 0-GC 裸字节与数值/字符串相互转换工具类 (零 JVM 堆内存分配)
 **/
public class ZeroGcNumberUtil {

    /**
     * 🚀 0-GC 裸字节直接提取 Long 数值 (零 String 分配)
     */
    public static long parseLongFromByteBuf(ByteBuf buf, int start, int end) {
        long val = 0;
        boolean digitFound = false;
        for (int i = start; i < end; i++) {
            byte b = buf.getByte(i);
            if (b >= '0' && b <= '9') {
                val = val * 10 + (b - '0');
                digitFound = true;
            } else if (digitFound) {
                break;
            }
        }
        return val;
    }

    private static final sun.misc.Unsafe UNSAFE;
    private static final long BYTE_ARRAY_OFFSET;

    static {
        try {
            java.lang.reflect.Field f = sun.misc.Unsafe.class.getDeclaredField("theUnsafe");
            f.setAccessible(true);
            UNSAFE = (sun.misc.Unsafe) f.get(null);
            BYTE_ARRAY_OFFSET = UNSAFE.arrayBaseOffset(byte[].class);
        } catch (Exception e) {
            throw new Error(e);
        }
    }

    /**
     * 🚀 0-GC Unsafe 多级向量对齐 SWAR (SIMD Within A Register) 大小写不敏感匹配
     * 对齐阶梯: 64位 Long (8B) -> 32位 Int (4B) -> 8位 Byte (1B)
     * 彻底覆盖 4 字节及以上所有关键词，无盲区享受 SIMD 级别加速！
     */
    public static boolean equalsBytesIgnoreCase(ByteBuf buf, int start, byte[] target) {
        int len = target.length;
        if (buf.writerIndex() - start < len) return false;

        int i = 0;
        // 🚀 1. SWAR 64 位 (8 字节) 并行对比
        while (i + 8 <= len) {
            long word = buf.getLong(start + i);
            long targetWord = UNSAFE.getLong(target, BYTE_ARRAY_OFFSET + i);
            if (toLowerSwar64(word) != toLowerSwar64(targetWord)) {
                return false;
            }
            i += 8;
        }

        // 🚀 2. SWAR 32 位 (4 字节) 并行对比
        if (i + 4 <= len) {
            int word = buf.getInt(start + i);
            int targetWord = UNSAFE.getInt(target, BYTE_ARRAY_OFFSET + i);
            if (toLowerSwar32(word) != toLowerSwar32(targetWord)) {
                return false;
            }
            i += 4;
        }

        // 🚀 3. 尾部剩余 1 ~ 3 字节 SWAR 单字节位运算比对
        for (; i < len; i++) {
            byte b1 = buf.getByte(start + i);
            byte b2 = target[i];
            if (toLowerAscii(b1) != toLowerAscii(b2)) {
                return false;
            }
        }
        return true;
    }

    public static boolean equalsStringIgnoreCase(ByteBuf buf, int start, String str) {
        return equalsBytesIgnoreCase(buf, start, str.getBytes(java.nio.charset.StandardCharsets.US_ASCII));
    }

    /**
     * 🚀 64 位 SWAR 8 字节并行 ASCII 转小写
     */
    private static long toLowerSwar64(long word) {
        long msb = word & 0x8080808080808080L;
        long a = word + 0x7F7F7F7F7F7F7F7FL - 0x4141414141414141L;
        long z = word + 0x7F7F7F7F7F7F7F7FL - 0x5B5B5B5B5B5B5B5BL;
        long isUpper = (a & ~z) & ~msb & 0x8080808080808080L;
        return word | ((isUpper >>> 2) & 0x2020202020202020L);
    }

    /**
     * 🚀 32 位 SWAR 4 字节并行 ASCII 转小写
     */
    private static int toLowerSwar32(int word) {
        int msb = word & 0x80808080;
        int a = word + 0x7F7F7F7F - 0x41414141;
        int z = word + 0x7F7F7F7F - 0x5B5B5B5B;
        int isUpper = (a & ~z) & ~msb & 0x80808080;
        return word | ((isUpper >>> 2) & 0x20202020);
    }

    /**
     * 🚀 0-GC 位运算单字节 ASCII 转小写
     */
    private static byte toLowerAscii(byte b) {
        return (byte) (b >= 'A' && b <= 'Z' ? b | 0x20 : b);
    }

    /**
     * 🚀 位运算替代除法与取模 (% 10 和 / 10)，极速写入 Long 的 ASCII 字节
     */
    public static void writeLongToAsciiByteBuf(ByteBuf buf, long value) {
        if (value == 0) {
            buf.writeByte('0');
            return;
        }
        if (value < 0) {
            buf.writeByte('-');
            value = -value;
        }
        int len = getLongAsciiLength(value);
        int startIdx = buf.writerIndex();
        buf.ensureWritable(len);
        buf.writerIndex(startIdx + len);

        int ptr = startIdx + len - 1;
        while (value > 0) {
            if (value <= Integer.MAX_VALUE) {
                int v = (int) value;
                int q = (int) ((v * 0xCCCCCCCDL) >>> 35); // 魔法乘法与位移替代 v / 10
                int r = v - ((q << 3) + (q << 1));        // 位移与减法替代 v % 10 (q * 10 = (q << 3) + (q << 1))
                buf.setByte(ptr--, (byte) ('0' + r));
                value = q;
            } else {
                long q = value / 10;
                long r = value - ((q << 3) + (q << 1));
                buf.setByte(ptr--, (byte) ('0' + (int) r));
                value = q;
            }
        }
    }

    /**
     * 计算 Long 数值转成 ASCII 后的字节长度
     */
    public static int getLongAsciiLength(long val) {
        if (val == 0) return 1;
        if (val == Long.MIN_VALUE) return 20;
        int len = 0;
        if (val < 0) {
            len++;
            val = -val;
        }
        if (val < 10) return len + 1;
        if (val < 100) return len + 2;
        if (val < 1000) return len + 3;
        if (val < 10000) return len + 4;
        if (val < 100000) return len + 5;
        if (val < 1000000) return len + 6;
        if (val < 10000000) return len + 7;
        if (val < 100000000) return len + 8;
        if (val < 1000000000) return len + 9;
        if (val < 10000000000L) return len + 10;
        if (val < 100000000000L) return len + 11;
        if (val < 1000000000000L) return len + 12;
        if (val < 10000000000000L) return len + 13;
        if (val < 100000000000000L) return len + 14;
        if (val < 1000000000000000L) return len + 15;
        if (val < 10000000000000000L) return len + 16;
        if (val < 100000000000000000L) return len + 17;
        if (val < 1000000000000000000L) return len + 18;
        return len + 19;
    }

    /**
     * 🚀 100% 0-GC 极速 IPv4 字节流写入 (在当前 ByteBuf 的 writerIndex 位置追加)
     * @return 写入的 ASCII 字节总数 (9~15 字节)
     */
    public static int writeIpToByteBuf(long ipLong, ByteBuf buf) {
        int targetIndex = buf.writerIndex();
        int len = writeIpToByteBuf(ipLong, buf, targetIndex);
        buf.writerIndex(targetIndex + len);
        return len;
    }

    /**
     * 🚀 100% 0-GC 极速 IPv4 字节流指定位置写入 (写入 ByteBuf 的 targetIndex，彻底消除 String 堆内存分配)
     * @param targetIndex 目标写入起始偏移位置
     * @return 写入的 ASCII 字节总数 (9~15 字节)
     */
    public static int writeIpToByteBuf(long ipLong, ByteBuf buf, int targetIndex) {
        int b1 = (int) ((ipLong >> 24) & 0xFF);
        int b2 = (int) ((ipLong >> 16) & 0xFF);
        int b3 = (int) ((ipLong >> 8) & 0xFF);
        int b4 = (int) (ipLong & 0xFF);

        int pos = targetIndex;
        pos = writeByteIntToBufAt(b1, buf, pos);
        buf.setByte(pos++, '.');
        pos = writeByteIntToBufAt(b2, buf, pos);
        buf.setByte(pos++, '.');
        pos = writeByteIntToBufAt(b3, buf, pos);
        buf.setByte(pos++, '.');
        pos = writeByteIntToBufAt(b4, buf, pos);

        return pos - targetIndex;
    }

    private static int writeByteIntToBufAt(int val, ByteBuf buf, int pos) {
        if (val >= 100) {
            int h = (val * 41) >>> 12;
            val -= (h << 6) + (h << 5) + (h << 2);
            int t = (val * 205) >>> 11;
            int o = val - ((t << 3) + (t << 1));
            buf.setByte(pos++, (byte) ('0' + h));
            buf.setByte(pos++, (byte) ('0' + t));
            buf.setByte(pos++, (byte) ('0' + o));
        } else if (val >= 10) {
            int t = (val * 205) >>> 11;
            int o = val - ((t << 3) + (t << 1));
            buf.setByte(pos++, (byte) ('0' + t));
            buf.setByte(pos++, (byte) ('0' + o));
        } else {
            buf.setByte(pos++, (byte) ('0' + val));
        }
        return pos;
    }

    /**
     * 🚀 高效 IPv4 Long 转 ASCII String 格式化 (彻底消除 StringBuilder 与多次 String 拼接分配)
     */
    public static String formatIpToString(long ipLong) {
        int b1 = (int) ((ipLong >> 24) & 0xFF);
        int b2 = (int) ((ipLong >> 16) & 0xFF);
        int b3 = (int) ((ipLong >> 8) & 0xFF);
        int b4 = (int) (ipLong & 0xFF);

        char[] chars = new char[15];
        int pos = writeIntToChars(b1, chars, 0);
        chars[pos++] = '.';
        pos = writeIntToChars(b2, chars, pos);
        chars[pos++] = '.';
        pos = writeIntToChars(b3, chars, pos);
        chars[pos++] = '.';
        pos = writeIntToChars(b4, chars, pos);

        return new String(chars, 0, pos);
    }

    /**
     * 🚀 位运算 + 乘法位移 (Mul-Shift Magic) 0-CPU 除法开销 (全无 idiv / % 开销)
     * 在 0..255 (Byte 范围) 内部：
     * - (val * 41) >>> 12 精确等价于 val / 100
     * - (val * 205) >>> 11 精确等价于 val / 10
     * - (t << 3) + (t << 1) 位移替代 t * 10
     */
    private static int writeIntToChars(int val, char[] chars, int pos) {
        if (val >= 100) {
            int h = (val * 41) >>> 12;
            val -= (h << 6) + (h << 5) + (h << 2); // val -= h * 100
            int t = (val * 205) >>> 11;
            int o = val - ((t << 3) + (t << 1));   // val % 10
            chars[pos++] = (char) ('0' + h);
            chars[pos++] = (char) ('0' + t);
            chars[pos++] = (char) ('0' + o);
        } else if (val >= 10) {
            int t = (val * 205) >>> 11;
            int o = val - ((t << 3) + (t << 1));   // val % 10
            chars[pos++] = (char) ('0' + t);
            chars[pos++] = (char) ('0' + o);
        } else {
            chars[pos++] = (char) ('0' + val);
        }
        return pos;
    }

    public static final int IP_TYPE_INVALID = 0;
    public static final int IP_TYPE_V4 = 1;
    public static final int IP_TYPE_V6 = 2;

    /**
     * 🚀 0-GC 极速 IPv4 / IPv6 分流字节解析 (统一使用 primitive long 表示)
     * @return 1=IPv4, 2=IPv6, 0=Invalid
     */
    public static int parseIp(ByteBuf buf, int start, int length, long[] ipv4Out, long[] ipv6OutDualLong) {
        if (length <= 0 || length > 45) {
            return IP_TYPE_INVALID;
        }

        int end = start + length;
        boolean isIpv6 = false;
        for (int i = start; i < end; i++) {
            byte b = buf.getByte(i);
            if (b == ':') {
                isIpv6 = true;
                break;
            } else if (b == ',' || b == ' ' || b == '\r') {
                end = i;
                break;
            }
        }

        if (!isIpv6) {
            long ip4 = parseIpv4(buf, start, end - start);
            if (ip4 != 0) {
                ipv4Out[0] = ip4;
                return IP_TYPE_V4;
            }
            return IP_TYPE_INVALID;
        } else {
            if (parseIpv6(buf, start, end - start, ipv6OutDualLong)) {
                return IP_TYPE_V6;
            }
            return IP_TYPE_INVALID;
        }
    }

    /**
     * 🚀 0-GC 将 ByteBuf 里的 IPv4 字节流直接转为 primitive long 数值
     */
    public static long parseIpv4(ByteBuf buf, int start, int length) {
        if (length <= 0 || length > 15) {
            return 0L;
        }

        long ipLong = 0;
        long part = 0;
        int dots = 0;

        int end = start + length;
        for (int i = start; i < end; i++) {
            byte b = buf.getByte(i);
            if (b >= '0' && b <= '9') {
                part = part * 10 + (b - '0');
            } else if (b == '.') {
                if (part > 255) {
                    return 0L;
                }
                ipLong = (ipLong << 8) | part;
                part = 0;
                dots++;
            } else if (b == ',' || b == ' ' || b == '\r') {
                break;
            } else {
                return 0L;
            }
        }

        if (dots == 3 && part <= 255) {
            ipLong = (ipLong << 8) | part;
            return ipLong;
        }
        return 0L;
    }

    private static final io.netty.util.concurrent.FastThreadLocal<int[]> IPV6_GROUPS_HOLDER = new io.netty.util.concurrent.FastThreadLocal<int[]>() {
        @Override
        protected int[] initialValue() {
            return new int[8];
        }
    };

    private static final io.netty.util.concurrent.FastThreadLocal<int[]> IPV6_TAIL_GROUPS_HOLDER = new io.netty.util.concurrent.FastThreadLocal<int[]>() {
        @Override
        protected int[] initialValue() {
            return new int[8];
        }
    };

    /**
     * 🚀 0-GC 极速 IPv6 解析为双 64 位 Long (High 64-bit + Low 64-bit)
     */
    public static boolean parseIpv6(ByteBuf buf, int start, int length, long[] out64) {
        if (length < 2 || length > 39 || out64 == null || out64.length < 2) {
            return false;
        }

        int end = start + length;
        int doubleColonIdx = -1;
        for (int i = start; i < end - 1; i++) {
            if (buf.getByte(i) == ':' && buf.getByte(i + 1) == ':') {
                doubleColonIdx = i;
                break;
            }
        }

        int[] groups = IPV6_GROUPS_HOLDER.get();
        java.util.Arrays.fill(groups, 0);
        int headCount = 0;
        int tailCount = 0;

        int limit = (doubleColonIdx != -1) ? doubleColonIdx : end;
        int curr = start;
        while (curr < limit) {
            int nextColon = -1;
            for (int i = curr; i < limit; i++) {
                if (buf.getByte(i) == ':') {
                    nextColon = i;
                    break;
                }
            }
            int partEnd = (nextColon != -1) ? nextColon : limit;
            int val = parseHex(buf, curr, partEnd - curr);
            if (val < 0 || headCount >= 8) return false;
            groups[headCount++] = val;
            if (nextColon == -1) break;
            curr = nextColon + 1;
        }

        if (doubleColonIdx != -1) {
            curr = doubleColonIdx + 2;
            int[] tailGroups = IPV6_TAIL_GROUPS_HOLDER.get();
            java.util.Arrays.fill(tailGroups, 0);
            while (curr < end) {
                int nextColon = -1;
                for (int i = curr; i < end; i++) {
                    if (buf.getByte(i) == ':') {
                        nextColon = i;
                        break;
                    }
                }
                int partEnd = (nextColon != -1) ? nextColon : end;
                int val = parseHex(buf, curr, partEnd - curr);
                if (val < 0 || tailCount >= 8) return false;
                tailGroups[tailCount++] = val;
                if (nextColon == -1) break;
                curr = nextColon + 1;
            }

            int gap = 8 - headCount - tailCount;
            if (gap < 0) return false;

            for (int i = 0; i < tailCount; i++) {
                groups[headCount + gap + i] = tailGroups[i];
            }
        } else {
            if (headCount != 8) return false;
        }

        long high = ((long) groups[0] << 48) | ((long) groups[1] << 32) | ((long) groups[2] << 16) | groups[3];
        long low  = ((long) groups[4] << 48) | ((long) groups[5] << 32) | ((long) groups[6] << 16) | groups[7];

        out64[0] = high;
        out64[1] = low;
        return true;
    }

    private static int parseHex(ByteBuf buf, int start, int len) {
        if (len <= 0 || len > 4) return -1;
        int val = 0;
        int end = start + len;
        for (int i = start; i < end; i++) {
            byte b = buf.getByte(i);
            int digit;
            if (b >= '0' && b <= '9') digit = b - '0';
            else if (b >= 'a' && b <= 'f') digit = b - 'a' + 10;
            else if (b >= 'A' && b <= 'F') digit = b - 'A' + 10;
            else return -1;
            val = (val << 4) | digit;
        }
        return val;
    }
}
