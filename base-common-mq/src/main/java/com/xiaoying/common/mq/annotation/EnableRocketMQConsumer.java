package com.xiaoying.common.mq.annotation;

import com.xiaoying.common.mq.autoconfig.RocketMQConsumerAutoConfiguration;
import org.springframework.context.annotation.Import;

import java.lang.annotation.*;

/**
 * @author:lijin,E-mail:jin.li@quvideo.com>
 * @created:2019/1/3
 * @function: 自动装配rocketmq消息者注解
 */
@Target(ElementType.TYPE)
@Retention(RetentionPolicy.RUNTIME)
@Import(RocketMQConsumerAutoConfiguration.class)
@Documented
public @interface EnableRocketMQConsumer {
}
