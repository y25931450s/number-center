package com.xiaoying.common.mq.config;

import com.ctrip.framework.apollo.Config;
import com.ctrip.framework.apollo.ConfigService;

/**
 * 获取Apollo上rocketMq的配置
 *
 * @Author: misskey
 * @Date: 2019-02-27
 * @Version 1.0
 */
public class RocketMqConfigFacade {


    private String prefix = "rocketmq.";

    public RocketMqConfigFacade() {
    }


    /**
     * 查找apollo上rocketmq nameServer配置
     *
     * @return
     */
    public RocketMQProperties findRocketMQProperties() {
        Config config = ConfigService.getConfig("rocketmq_common");
        RocketMQProperties rocketMQProperties = new RocketMQProperties();
        rocketMQProperties.setNameServer(config.getProperty(prefix + "nameServer", "localhost:9876"));
        return rocketMQProperties;
    }


    /**
     * 查找apollo上rocketmq producer配置
     *
     * @return
     */
    public RocketMQProducerProperties findRocketMQProducerProperties() {
        Config config = ConfigService.getAppConfig();
        RocketMQProducerProperties rocketMQProducerProperties = new RocketMQProducerProperties();
        rocketMQProducerProperties.setCompressMessageBodyThreshold(config.getIntProperty(prefix + "compressMessageBodyThreshold", 1024));
        rocketMQProducerProperties.setGroup(config.getProperty(prefix + "group", "default"));
        rocketMQProducerProperties.setRetryNextServer(config.getBooleanProperty(prefix + "retryNextServer", true));
        rocketMQProducerProperties.setRetryTimesWhenSendAsyncFailed(config.getIntProperty(prefix + "retryTimesWhenSendAsyncFailed", 1));
        rocketMQProducerProperties.setRetryTimesWhenSendFailed(config.getIntProperty(prefix + "retryTimesWhenSendFailed", 1));
        rocketMQProducerProperties.setSendMessageTimeout(config.getIntProperty(prefix + "sendMessageTimeout", 1000));
        rocketMQProducerProperties.setMaxMessageSize(config.getIntProperty(prefix + "maxMessageSize", 1024));
        return rocketMQProducerProperties;
    }

}
