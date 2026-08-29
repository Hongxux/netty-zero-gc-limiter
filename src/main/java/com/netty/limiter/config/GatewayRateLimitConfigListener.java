package com.netty.limiter.config;

import com.alibaba.fastjson2.JSON;
import com.alibaba.fastjson2.JSONObject;
import com.alibaba.nacos.api.NacosFactory;
import com.alibaba.nacos.api.config.ConfigService;
import com.alibaba.nacos.api.config.listener.Listener;
import com.netty.limiter.limiter.LocalGlobalRateLimiter;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.CommandLineRunner;
import org.springframework.stereotype.Component;

import java.util.Properties;
import java.util.concurrent.Executor;

/**
 * @description: Nacos 动态热更新配置监听器 (支持无 Nacos 兜底)
 **/
@Slf4j
@Component
public class GatewayRateLimitConfigListener implements CommandLineRunner {

    @Value("${spring.cloud.nacos.config.server-addr:}")
    private String serverAddr;

    @Value("${netty.limiter.nacos.data-id:netty-rate-limiter.json}")
    private String dataId;

    @Value("${netty.limiter.nacos.group:DEFAULT_GROUP}")
    private String group;

    @Autowired
    private GatewayRateLimitProperties properties;

    @Autowired
    private LocalGlobalRateLimiter localGlobalRateLimiter;

    @Override
    public void run(String... args) throws Exception {
        if (properties.getJwtSecret() != null) {
            com.netty.limiter.util.jwt.JwtAuthenticator.updateSecretKey(properties.getJwtSecret());
        }

        if (serverAddr == null || serverAddr.isEmpty()) {
            log.info("Nacos serverAddr is empty. Using local properties for rate limiter.");
            localGlobalRateLimiter.updateConfig(properties.getGlobalQps(), properties.getFillRate());
            return;
        }

        try {
            Properties nacosProps = new Properties();
            nacosProps.put("serverAddr", serverAddr);
            ConfigService configService = NacosFactory.createConfigService(nacosProps);

            String content = configService.getConfig(dataId, group, 5000);
            if (content != null) {
                parseAndUpdateConfig(content);
            }

            configService.addListener(dataId, group, new Listener() {
                @Override
                public Executor getExecutor() {
                    return null;
                }

                @Override
                public void receiveConfigInfo(String configInfo) {
                    parseAndUpdateConfig(configInfo);
                }
            });
        } catch (Exception e) {
            log.warn("Failed to initialize Nacos config listener for rate limiter. Falling back to local configuration.", e);
            localGlobalRateLimiter.updateConfig(properties.getGlobalQps(), properties.getFillRate());
        }
    }

    private void parseAndUpdateConfig(String content) {
        try {
            JSONObject json = JSON.parseObject(content);
            if (json != null) {
                Integer qps = json.getInteger("globalQps");
                Integer fillRate = json.getInteger("fillRate");
                if (qps != null && fillRate != null) {
                    if (qps > 0 && fillRate > 0) {
                        localGlobalRateLimiter.updateConfig(qps, fillRate);
                        log.info("Successfully updated LocalGlobalRateLimiter config from Nacos: QPS={}, fillRate={}", qps, fillRate);
                    } else {
                        log.warn("Ignored invalid Nacos rate limit config parameters: globalQps={}, fillRate={}. Values must be positive (> 0).", qps, fillRate);
                    }
                }

                String jwtSecret = json.getString("jwtSecret");
                if (jwtSecret != null && !jwtSecret.trim().isEmpty()) {
                    com.netty.limiter.util.jwt.JwtAuthenticator.updateSecretKey(jwtSecret);
                    log.info("Successfully rotated JWT Secret Key from Nacos config.");
                }
            }
        } catch (Exception e) {
            log.error("Failed to parse rate limiter Nacos config content", e);
        }
    }
}
