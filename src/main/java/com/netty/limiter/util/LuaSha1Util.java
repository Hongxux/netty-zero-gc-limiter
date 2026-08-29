package com.netty.limiter.util;

import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;

/**
 * @description: 0-GC Lua 脚本 40 字节 ASCII Hex SHA-1 静态预计算与共享工具类
 **/
public class LuaSha1Util {

    private static final byte[] HEX_DIGITS = "0123456789abcdef".getBytes(StandardCharsets.US_ASCII);

    /**
     * 网关默认 Per-UID 令牌桶 Lua 脚本
     */
    public static final String DEFAULT_LUA_SCRIPT =
            "local key = KEYS[1]\n" +
            "local now_ms = tonumber(ARGV[1])\n" +
            "local max_tokens = tonumber(ARGV[2])\n" +
            "local refill_rate = tonumber(ARGV[3])\n" +
            "local ttl_sec = tonumber(ARGV[4])\n" +
            "local requested = tonumber(ARGV[5]) or 1\n" +
            "local data = redis.call('get', key)\n" +
            "local last_time_ms = now_ms\n" +
            "local tokens = max_tokens\n" +
            "if data then\n" +
            "    local sep = string.find(data, \":\")\n" +
            "    if sep then\n" +
            "        last_time_ms = tonumber(string.sub(data, 1, sep - 1)) or now_ms\n" +
            "        tokens = tonumber(string.sub(data, sep + 1)) or max_tokens\n" +
            "    end\n" +
            "    local delta_sec = math.max(0, (now_ms - last_time_ms) / 1000.0)\n" +
            "    tokens = math.min(max_tokens, tokens + (delta_sec * refill_rate))\n" +
            "end\n" +
            "local granted = math.min(tokens, requested)\n" +
            "if granted >= 1 then\n" +
            "    tokens = tokens - granted\n" +
            "    local new_data = tostring(now_ms) .. \":\" .. tostring(tokens)\n" +
            "    redis.call('set', key, new_data)\n" +
            "    redis.call('expire', key, ttl_sec)\n" +
            "    return math.floor(granted)\n" +
            "else\n" +
            "    return 0\n" +
            "end";

    /**
     * JVM 类加载 static 阶段即完成 40 字节 Hex SHA-1 计算 (0 堆内存分配，全局共享不可变 byte[])
     */
    public static final byte[] DEFAULT_LUA_SHA1_BYTES = computeSha1HexBytes(DEFAULT_LUA_SCRIPT);

    /**
     * 计算任意 Lua 脚本字符串的 40 字节 ASCII Hex SHA-1 字节数组 (用于 Redis EVALSHA 命令)
     */
    public static byte[] computeSha1HexBytes(String scriptText) {
        if (scriptText == null || scriptText.isEmpty()) {
            throw new IllegalArgumentException("Script text must not be empty");
        }
        try {
            MessageDigest md = MessageDigest.getInstance("SHA-1");
            byte[] digest = md.digest(scriptText.getBytes(StandardCharsets.UTF_8));
            byte[] hexBytes = new byte[40];
            for (int i = 0; i < digest.length; i++) {
                int b = digest[i] & 0xFF;
                hexBytes[i * 2]     = HEX_DIGITS[b >>> 4];
                hexBytes[i * 2 + 1] = HEX_DIGITS[b & 0x0F];
            }
            return hexBytes;
        } catch (NoSuchAlgorithmException e) {
            throw new IllegalStateException("SHA-1 algorithm not available in JVM", e);
        }
    }
}
