package com.xiaoying.common.mq.autoconfig;

import org.springframework.boot.autoconfigure.AutoConfigureAfter;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;

/**
 * @author:lijin,E-mail:jin.li@quvideo.com>
 * @created:2019/1/4
 * @function: 消费者自动装配类
 */
@Import(RocketMQCommonConfiguration.class)
@AutoConfigureAfter(RocketMQCommonConfiguration.class)
public class RocketMQConsumerAutoConfiguration {
    @Bean
    public ListenerContainer listenerContainer() {
        return new ListenerContainer();
    }
}
