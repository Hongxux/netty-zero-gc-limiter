package com.netty.limiter.limiter;

import com.netty.limiter.util.LuaSha1Util;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

public class LuaWatermarkEarlyWarningTest {

    @Test
    @DisplayName("验证 Lua 脚本包含 80% 水位线提前 PubSub 广播逻辑")
    public void testLuaScriptContainsWatermarkEarlyWarning() {
        String script = LuaSha1Util.DEFAULT_LUA_SCRIPT;
        Assertions.assertNotNull(script);
        Assertions.assertTrue(script.contains("watermark_remaining"), "Lua 脚本中应包含 watermark_remaining 80% 水位线阈值计算");
        Assertions.assertTrue(script.contains("math.floor(max_tokens * 0.2)"), "Lua 脚本中应包含 80% 水位线计算公式");
        Assertions.assertTrue(script.contains("old_tokens > watermark_remaining and tokens <= watermark_remaining"),
                "Lua 脚本中应包含跨越 80% 水位线提前发布 PubSub 广播的条件表达式");
    }
}
