package com.netty.limiter.util.jwt;

import com.netty.limiter.cache.JwtSigUidCache;
import io.netty.buffer.ByteBuf;

/**
 * @description: 0-GC 极速 Base64URL 解码与 "uid": / "exp": 字段 DFA 状态机解析器
 * 极致纯原语栈内存运算 (100% Zero Heap & Zero Object & Zero Array)：
 * 1. 彻底淘汰 ThreadLocal/ParseResult 对象/数组封装，解析出的 uid 与 expSec 纯粹存在于 JVM 局部变量栈 (Stack Primities) 中。
 * 2. 对 Base64URL 字符进行单通道 (Single-Pass) 流式解码，在解码出的字节流上通过并行 DFA 状态机检测 "uid": 与 "exp": 关键字。
 * 3. 一旦匹配关键字，直接进行 primitive long 数字累加。
 * 4. 校验 `uid > 0 && expSec > nowSec` 合法性，通过后直接向 JwtSigUidCache 写入缓存并返回 primitive long uid。
 **/
public class JwtPayloadDfaParser {

    /**
     * 【0-GC & 0 对象 & 0 数组】流式 Base64URL 解码、DFA 提炼 UID/EXP、有效性校验与缓存回写
     * @return 鉴权通过返回 primitive long UID，未通过或已过期返回 0L
     */
    public static long parseAndCacheValidUid(ByteBuf buf, int start, int max, long sigHash, long sigPrefix) {
        long uid = 0L;
        long expSec = 0L;

        int p = start;
        int buf4 = 0;
        int bits = 0;

        int uidMatchState = 0;
        int expMatchState = 0;
        int activeParsing = 0; // 0: Searching, 1: Parsing UID, 2: Parsing EXP

        boolean hasUidDigit = false;
        boolean hasExpDigit = false;

        while (p < max) {
            byte b = buf.getByte(p);
            if (b == '\r' || b == '\n' || b == ' ' || b == '"' || b == '.') {
                break;
            }

            int val = decodeBase64Char(b);
            if (val >= 0) {
                buf4 = (buf4 << 6) | val;
                bits += 6;
                if (bits >= 8) {
                    bits -= 8;
                    byte decodedByte = (byte) ((buf4 >> bits) & 0xFF);

                    if (activeParsing == 1) { // 正在解析 UID
                        if (decodedByte >= '0' && decodedByte <= '9') {
                            uid = uid * 10 + (decodedByte - '0');
                            hasUidDigit = true;
                        } else if (hasUidDigit) {
                            activeParsing = 0; // UID 解析完毕，切回 Searching!
                        } else if (decodedByte != ' ' && decodedByte != '"' && decodedByte != ':') {
                            activeParsing = 0;
                        }
                    } else if (activeParsing == 2) { // 正在解析 EXP
                        if (decodedByte >= '0' && decodedByte <= '9') {
                            expSec = expSec * 10 + (decodedByte - '0');
                            hasExpDigit = true;
                        } else if (hasExpDigit) {
                            activeParsing = 0; // EXP 解析完毕，切回 Searching!
                        } else if (decodedByte != ' ' && decodedByte != '"' && decodedByte != ':') {
                            activeParsing = 0;
                        }
                    }

                    if (activeParsing == 0) {
                        // DFA 无缝并行搜索 "uid": 与 "exp":
                        uidMatchState = advanceUidMatch(decodedByte, uidMatchState);
                        if (uidMatchState == 6) {
                            activeParsing = 1;
                            uidMatchState = 0;
                        } else {
                            expMatchState = advanceExpMatch(decodedByte, expMatchState);
                            if (expMatchState == 6) {
                                activeParsing = 2;
                                expMatchState = 0;
                            }
                        }
                    }
                }
            }
            p++;
        }

        // 纯栈原语校验：判断 UID 是否合法且未过期 (没有 exp 字段或已过期返回 -2L 哨兵值)
        if (uid > 0) {
            long nowSec = System.currentTimeMillis() / 1000;
            if (isNotExpired(expSec, nowSec)) {
                // 鉴权成功：回写 0-GC 双静态 Flat Table 缓存供后续 Fast Path 命中 (二重防碰撞)
                JwtSigUidCache.INSTANCE.put(sigHash, sigPrefix, uid, expSec);
                return uid;
            } else {
                return -2L; // 签名合法但 Token 已过期/无exp字段
            }
        }
        return -1L;
    }

    /**
     * 判断 JWT 是否在有效期内 (JWT 规范 exp 字段单位为秒)
     */
    private static boolean isNotExpired(long expSec, long nowSec) {
        return expSec > 0 && nowSec < expSec;
    }

    private static int advanceUidMatch(byte b, int state) {
        switch (state) {
            case 0: return (b == '"') ? 1 : 0;
            case 1: if (b == 'u') return 2; if (b == '"') return 1; return 0;
            case 2: if (b == 'i') return 3; if (b == '"') return 1; return 0;
            case 3: if (b == 'd') return 4; if (b == '"') return 1; return 0;
            case 4: if (b == '"') return 5; return 0;
            case 5: if (b == ':') return 6; if (b == '"') return 1; return 0;
            default: return 0;
        }
    }

    private static int advanceExpMatch(byte b, int state) {
        switch (state) {
            case 0: return (b == '"') ? 1 : 0;
            case 1: if (b == 'e') return 2; if (b == '"') return 1; return 0;
            case 2: if (b == 'x') return 3; if (b == '"') return 1; return 0;
            case 3: if (b == 'p') return 4; if (b == '"') return 1; return 0;
            case 4: if (b == '"') return 5; return 0;
            case 5: if (b == ':') return 6; if (b == '"') return 1; return 0;
            default: return 0;
        }
    }

    private static int decodeBase64Char(byte b) {
        if (b >= 'A' && b <= 'Z') return b - 'A';
        if (b >= 'a' && b <= 'z') return b - 'a' + 26;
        if (b >= '0' && b <= '9') return b - '0' + 52;
        if (b == '+' || b == '-') return 62;
        if (b == '/' || b == '_') return 63;
        return -1;
    }
}
