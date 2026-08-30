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
     * 网关默认 Per-UID 令牌桶 Lua 脚本 (Fast Path 优先扣减 + 80% 水位线提前 W: 预警广播 + 100% 耗尽 U: 封禁)
     */
    public static final String DEFAULT_LUA_SCRIPT =
            "local key = KEYS[1]\n" +
            "local uid_val = tonumber(string.match(key, \"(%d+)\")) or 0\n" +
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
            "end\n" +
            "-- 🚨 80% 水位线临界阈值 (剩余令牌 <= 20% max_tokens)\n" +
            "local watermark_remaining = math.floor(max_tokens * 0.2)\n" +
            "-- 🚀 1. Fast Path: 余量足够直接扣减，擦除浮点与时间开销\n" +
            "if tokens >= requested then\n" +
            "    local old_tokens = tokens\n" +
            "    tokens = tokens - requested\n" +
            "    redis.call('set', key, tostring(now_ms) .. \":\" .. tostring(tokens))\n" +
            "    redis.call('expire', key, ttl_sec)\n" +
            "    -- 🚨 80% 水位提前预警广播 (W: 预警标记，敏捷短冷静期 1 秒，防窗口期暴击且不误杀后续正常流量)\n" +
            "    if old_tokens > watermark_remaining and tokens <= watermark_remaining then\n" +
            "        redis.call('publish', 'NETTY_LIMITER_BAN_CHANNEL', 'W:' .. tostring(key) .. ':1')\n" +
            "    end\n" +
            "    return tostring(uid_val) .. \":1\"\n" +
            "end\n" +
            "-- 🚀 2. Slow Path: 令牌不足时按时间差惰性回填\n" +
            "local delta_sec = math.max(0, (now_ms - last_time_ms) / 1000.0)\n" +
            "if delta_sec > 0 then\n" +
            "    tokens = math.min(max_tokens, tokens + (delta_sec * refill_rate))\n" +
            "    last_time_ms = now_ms\n" +
            "end\n" +
            "-- 🚀 3. 回填后再次尝试扣减\n" +
            "if tokens >= requested then\n" +
            "    local old_tokens = tokens\n" +
            "    tokens = tokens - requested\n" +
            "    redis.call('set', key, tostring(now_ms) .. \":\" .. tostring(tokens))\n" +
            "    redis.call('expire', key, ttl_sec)\n" +
            "    -- 🚨 80% 水位提前预警广播\n" +
            "    if old_tokens > watermark_remaining and tokens <= watermark_remaining then\n" +
            "        redis.call('publish', 'NETTY_LIMITER_BAN_CHANNEL', 'W:' .. tostring(key) .. ':1')\n" +
            "    end\n" +
            "    return tostring(uid_val) .. \":1\"\n" +
            "end\n" +
            "-- 🚀 4. 100% 彻底耗尽：触发限流并全网 Pub/Sub 广播正式封禁 (U: 封禁标记)\n" +
            "redis.call('publish', 'NETTY_LIMITER_BAN_CHANNEL', 'U:' .. tostring(key) .. ':' .. tostring(ttl_sec))\n" +
            "return tostring(uid_val) .. \":0\"";

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
