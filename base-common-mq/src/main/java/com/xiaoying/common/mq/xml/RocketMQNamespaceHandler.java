package com.xiaoying.common.mq.xml;

import org.springframework.beans.factory.xml.NamespaceHandlerSupport;

/**解析 mq:consumer
 * @Author: misskey
 * @Date: 2019-03-04
 * @Version 1.0
 */
public class RocketMQNamespaceHandler  extends NamespaceHandlerSupport {
    @Override
    public void init() {
        registerBeanDefinitionParser("consumer",new RocketMQConsumerParser());
        registerBeanDefinitionParser("producer",new RocketMQProducerParser());
    }
}
