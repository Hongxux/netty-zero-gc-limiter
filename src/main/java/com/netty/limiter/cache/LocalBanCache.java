package com.netty.limiter.cache;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;

import java.lang.invoke.MethodHandles;
import java.lang.invoke.VarHandle;
import java.util.concurrent.ConcurrentHashMap;

/**
 * @description: 本地黑名单缓存（支持 0-GC primitive long UID 扁平数组查询 + 高效字符串规则查询）
 **/
@Slf4j
@Component
public class LocalBanCache {

    @Data
    @AllArgsConstructor
    @NoArgsConstructor
    public static class BanInfo {
        private String message;
        private long expireTimeMillis;
    }

    private static final int INITIAL_CAPACITY = 65536;
    private static final int MASK = INITIAL_CAPACITY - 1;
    private static final int MAX_PROBE = 16;

    private static final VarHandle KEYS_HANDLE = MethodHandles.arrayElementVarHandle(long[].class);
    private static final VarHandle EXPIRES_HANDLE = MethodHandles.arrayElementVarHandle(long[].class);

    private final long[] uidKeys = new long[INITIAL_CAPACITY];
    private final long[] uidExpires = new long[INITIAL_CAPACITY];

    private final ConcurrentHashMap<String, BanInfo> stringRuleMap = new ConcurrentHashMap<>(1024);

    public void putUserBan(long userId, long durationSeconds) {
        if (userId <= 0) {
            return;
        }
        int baseIndex = (int) (mixHash(userId) & MASK);
        long now = System.currentTimeMillis();
        long expireTime = now + (durationSeconds * 1000L);

        // 开放寻址线性探查 (MAX_PROBE=16)，防止单槽碰撞覆盖
        for (int i = 0; i < MAX_PROBE; i++) {
            int index = (baseIndex + i) & MASK;
            long storedUid = (long) KEYS_HANDLE.getAcquire(uidKeys, index);

            if (storedUid == userId) {
                // 已存在该 UID，直接更新过期时间并返回
                EXPIRES_HANDLE.setRelease(uidExpires, index, expireTime);
                return;
            }

            if (storedUid == 0L) {
                // 遇到空槽位，直接写入
                KEYS_HANDLE.setRelease(uidKeys, index, userId);
                EXPIRES_HANDLE.setRelease(uidExpires, index, expireTime);
                return;
            }

            // 槽位已有其他 UID，判断其是否已过期；若已过期，可安全覆盖
            long storedExpire = (long) EXPIRES_HANDLE.getAcquire(uidExpires, index);
            if (storedExpire <= now) {
                KEYS_HANDLE.setRelease(uidKeys, index, userId);
                EXPIRES_HANDLE.setRelease(uidExpires, index, expireTime);
                return;
            }
        }

        // 极端情况下探查满槽，兜底回退写入首槽
        KEYS_HANDLE.setRelease(uidKeys, baseIndex, userId);
        EXPIRES_HANDLE.setRelease(uidExpires, baseIndex, expireTime);
    }

    public BanInfo getUserBanInfo(long userId) {
        if (userId <= 0) {
            return null;
        }
        int baseIndex = (int) (mixHash(userId) & MASK);
        long now = System.currentTimeMillis();

        for (int i = 0; i < MAX_PROBE; i++) {
            int index = (baseIndex + i) & MASK;
            long storedUid = (long) KEYS_HANDLE.getAcquire(uidKeys, index);

            if (storedUid == userId) {
                long expireTime = (long) EXPIRES_HANDLE.getAcquire(uidExpires, index);
                if (expireTime > now) {
                    return new BanInfo("User is in local ban list", expireTime);
                } else {
                    return null;
                }
            }

            if (storedUid == 0L) {
                // 遇到空槽位停止探查
                return null;
            }
        }
        return null;
    }

    public void putBanInfo(String key, String message, long durationSeconds) {
        long expireTime = System.currentTimeMillis() + (durationSeconds * 1000L);
        stringRuleMap.put(key, new BanInfo(message, expireTime));
    }

    public BanInfo getBanInfo(String key) {
        BanInfo info = stringRuleMap.get(key);
        if (info != null) {
            if (info.getExpireTimeMillis() > System.currentTimeMillis()) {
                return info;
            } else {
                stringRuleMap.remove(key);
            }
        }
        return null;
    }

    private static long mixHash(long key) {
        key = (~key) + (key << 21);
        key = key ^ (key >>> 24);
        key = (key + (key << 3)) + (key << 8);
        key = key ^ (key >>> 14);
        key = (key + (key << 2)) + (key << 4);
        key = key ^ (key >>> 28);
        key = key + (key << 31);
        return key;
    }
}
