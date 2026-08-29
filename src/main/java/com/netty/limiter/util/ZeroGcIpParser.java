package com.netty.limiter.util;

import io.netty.buffer.ByteBuf;

/**
 * @description: 0-GC 极速 IPv4 字节解析与快速比较工具类 (Enum Singleton)
 **/
public enum ZeroGcIpParser {

    /**
     * 唯一单例
     */
    INSTANCE;

    /**
     * 将 ByteBuf 里的 IPv4 格式字节流直接转为 primitive long 数值
     */
    public long parseIpToLong(ByteBuf buf, int start, int length) {
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
}
