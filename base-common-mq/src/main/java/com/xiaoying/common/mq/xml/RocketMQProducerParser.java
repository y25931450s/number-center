package com.xiaoying.common.mq.xml;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.xiaoying.common.mq.config.RocketMQProducerProperties;
import com.xiaoying.common.mq.config.RocketMQProperties;
import com.xiaoying.common.mq.config.RocketMqConfigFacade;
import com.xiaoying.common.mq.core.RocketMQTemplate;
import org.apache.rocketmq.client.producer.DefaultMQProducer;
import org.springframework.beans.factory.config.BeanDefinition;
import org.springframework.beans.factory.support.AbstractBeanDefinition;
import org.springframework.beans.factory.support.BeanDefinitionBuilder;
import org.springframework.beans.factory.support.BeanDefinitionRegistry;
import org.springframework.beans.factory.xml.AbstractBeanDefinitionParser;
import org.springframework.beans.factory.xml.ParserContext;
import org.w3c.dom.Element;

/**
 * 解析<mq:producer enable=""/> 注入RocketMQTemplate
 *
 * @Author: misskey
 * @Date: 2019-02-27
 * @Version 1.0
 */

public class RocketMQProducerParser extends AbstractBeanDefinitionParser {
    private RocketMqConfigFacade rocketMqConfigFacade;

    public RocketMQProducerParser() {
        this.rocketMqConfigFacade = new RocketMqConfigFacade();
    }


    /**
     * 不需要自己传入id值
     *
     * @return
     */
    @Override
    protected boolean shouldGenerateId() {
        return true;
    }

    /**
     * 注入RocketMQTemplate
     *
     * @param element
     * @param parserContext
     * @return
     */
    @Override
    protected AbstractBeanDefinition parseInternal(Element element, ParserContext parserContext) {
        return (AbstractBeanDefinition) createRocketMQTemplate(parserContext.getRegistry());
    }

    private BeanDefinition createRocketMQTemplate(BeanDefinitionRegistry beanDefinitionRegistry) {
        BeanDefinitionBuilder beanDefinitionBuilder = BeanDefinitionBuilder.rootBeanDefinition(RocketMQTemplate.class);
        beanDefinitionBuilder.addConstructorArgReference(createDefaultMQProducerBean(beanDefinitionRegistry));
        beanDefinitionBuilder.addConstructorArgReference(createObjectMapperBean(beanDefinitionRegistry));
        return beanDefinitionBuilder.getBeanDefinition();
    }

    private String createDefaultMQProducerBean(BeanDefinitionRegistry beanDefinitionRegistry) {
        RocketMQProperties rocketMQProperties = rocketMqConfigFacade.findRocketMQProperties();
        RocketMQProducerProperties rocketMQProducerProperties = rocketMqConfigFacade.findRocketMQProducerProperties();
        BeanDefinitionBuilder beanDefinitionBuilder = BeanDefinitionBuilder.rootBeanDefinition(DefaultMQProducer.class);
        beanDefinitionBuilder.addPropertyValue("namesrvAddr", rocketMQProperties.getNameServer());
        beanDefinitionBuilder.addPropertyValue("sendMsgTimeout", rocketMQProducerProperties.getSendMessageTimeout());
        beanDefinitionBuilder.addPropertyValue("retryTimesWhenSendFailed", rocketMQProducerProperties.getRetryTimesWhenSendFailed());
        beanDefinitionBuilder.addPropertyValue("retryTimesWhenSendAsyncFailed", rocketMQProducerProperties.getRetryTimesWhenSendAsyncFailed());
        beanDefinitionBuilder.addPropertyValue("maxMessageSize", rocketMQProducerProperties.getMaxMessageSize());
        beanDefinitionBuilder.addPropertyValue("compressMsgBodyOverHowmuch", rocketMQProducerProperties.getCompressMessageBodyThreshold());
        beanDefinitionBuilder.addPropertyValue("retryAnotherBrokerWhenNotStoreOK", rocketMQProducerProperties.isRetryNextServer());
        beanDefinitionBuilder.setScope(BeanDefinition.SCOPE_SINGLETON);
        // producerGroup配置
        beanDefinitionBuilder.addConstructorArgValue(rocketMQProducerProperties.getGroup());
        // 开启追踪
        beanDefinitionBuilder.addConstructorArgValue(rocketMQProducerProperties.isEnableTrace());
        beanDefinitionRegistry.registerBeanDefinition(DefaultMQProducer.class.getSimpleName(), beanDefinitionBuilder.getBeanDefinition());
        return DefaultMQProducer.class.getSimpleName();
    }

    private String createObjectMapperBean(BeanDefinitionRegistry beanDefinitionRegistry) {
        BeanDefinitionBuilder beanDefinitionBuilder = BeanDefinitionBuilder.rootBeanDefinition(ObjectMapper.class);
        beanDefinitionBuilder.setScope(BeanDefinition.SCOPE_SINGLETON);
        beanDefinitionRegistry.registerBeanDefinition(ObjectMapper.class.getSimpleName(), beanDefinitionBuilder.getBeanDefinition());
        return ObjectMapper.class.getSimpleName();
    }
}
