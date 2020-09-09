package com.xiaoying.common.mq.annotation;

import com.xiaoying.common.mq.autoconfig.RocketMQProducerAutoConfiguration;
import org.springframework.context.annotation.Import;

import java.lang.annotation.*;

/**
 * @author:lijin,E-mail:jin.li@quvideo.com>
 * @created:2019/1/3
 * @function: 打开rocketmq 生产者
 */
@Target(ElementType.TYPE)
@Retention(RetentionPolicy.RUNTIME)
@Import(RocketMQProducerAutoConfiguration.class)
@Documented
public @interface EnableRocketMQProducer {
}
