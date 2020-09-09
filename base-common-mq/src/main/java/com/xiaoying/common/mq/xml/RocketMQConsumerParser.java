package com.xiaoying.common.mq.xml;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.xiaoying.common.mq.config.RocketMqConfigFacade;
import com.xiaoying.common.mq.constant.ConsumeMode;
import com.xiaoying.common.mq.constant.MessageModel;
import com.xiaoying.common.mq.constant.RocketMQConstant;
import com.xiaoying.common.mq.constant.SelectorType;
import com.xiaoying.common.mq.core.RocketMQListener;
import com.xiaoying.common.mq.support.DefaultRocketMQListenerContainer;
import org.springframework.beans.factory.BeanCreationException;
import org.springframework.beans.factory.config.BeanDefinition;
import org.springframework.beans.factory.support.AbstractBeanDefinition;
import org.springframework.beans.factory.support.BeanDefinitionBuilder;
import org.springframework.beans.factory.support.BeanDefinitionRegistry;
import org.springframework.beans.factory.xml.AbstractBeanDefinitionParser;
import org.springframework.beans.factory.xml.ParserContext;
import org.springframework.core.env.Environment;
import org.springframework.util.Assert;
import org.w3c.dom.Element;

/**
 * 解析 <mq:consumer consume-group="test" bean-ref="testMqReceivers"
 * max-retry-time="3" topic="junit-test" consume-mode="CONCURRENTLY"
 * message-model="CLUSTERING" consume-thread-max="1"/>
 *
 * @Author: misskey
 * @Date: 2019-02-27
 * @Version 1.0
 */


public class RocketMQConsumerParser extends AbstractBeanDefinitionParser {

    private RocketMqConfigFacade rocketMqConfigFacade;


    public RocketMQConsumerParser() {
        rocketMqConfigFacade = new RocketMqConfigFacade();
    }


    @Override
    protected boolean shouldGenerateId() {
        return true;
    }

    @Override
    protected AbstractBeanDefinition parseInternal(Element element, ParserContext parserContext) {
        BeanDefinitionBuilder beanDefinitionBuilder = BeanDefinitionBuilder.rootBeanDefinition(DefaultRocketMQListenerContainer.class);
        Environment environment = parserContext.getReaderContext().getEnvironment();
        beanDefinitionBuilder.addPropertyValue("nameServer", rocketMqConfigFacade.findRocketMQProperties().getNameServer());
        beanDefinitionBuilder.addPropertyValue("topic", environment.resolvePlaceholders(element.getAttribute(XmlAttributeConstant.topic)));
        beanDefinitionBuilder.addPropertyValue("maxRetryTimes", element.getAttribute(XmlAttributeConstant.maxRetryTime));
        beanDefinitionBuilder.addPropertyValue("selectorType", getSelectType(element));
        beanDefinitionBuilder.addPropertyValue("selectorExpression", environment.resolvePlaceholders(element.getAttribute(XmlAttributeConstant.selectorExpression)));
        beanDefinitionBuilder.addPropertyValue("consumerGroup", environment.resolvePlaceholders(element.getAttribute(XmlAttributeConstant.consumeGroup)));
        beanDefinitionBuilder.addPropertyValue("consumeThreadMax", element.getAttribute(XmlAttributeConstant.consumeThreadMax));
        beanDefinitionBuilder.addPropertyValue("consumeMode", getConsumeMode(element));
        beanDefinitionBuilder.addPropertyValue("messageModel", getMessageMode(element));
        beanDefinitionBuilder.addPropertyReference("rocketMQListener", getRocketMQListener(element, parserContext.getRegistry()));
        beanDefinitionBuilder.addPropertyValue("objectMapper", getObjectMapperBeanDefinition(parserContext.getRegistry()));
        beanDefinitionBuilder.addPropertyValue("delayLevelWhenNextConsume", element.getAttribute(XmlAttributeConstant.delayLevelWhenNextConsume));
        beanDefinitionBuilder.addPropertyValue("enableTrace", element.getAttribute(XmlAttributeConstant.enableTrace));
        beanDefinitionBuilder.setInitMethodName("start");
        beanDefinitionBuilder.setDestroyMethodName("destroy");
        return beanDefinitionBuilder.getBeanDefinition();
    }


    private BeanDefinition getObjectMapperBeanDefinition(BeanDefinitionRegistry beanDefinitionRegistry) {

        if (!beanDefinitionRegistry.containsBeanDefinition(RocketMQConstant.ROCKET_MQOBJECT_MAPPER_BEANNAME)) {
            BeanDefinitionBuilder beanDefinitionBuilder = BeanDefinitionBuilder.rootBeanDefinition(ObjectMapper.class);
            BeanDefinition definition = beanDefinitionBuilder.getBeanDefinition();
            beanDefinitionRegistry.registerBeanDefinition(RocketMQConstant.ROCKET_MQOBJECT_MAPPER_BEANNAME, definition);
        }
        return beanDefinitionRegistry.getBeanDefinition(RocketMQConstant.ROCKET_MQOBJECT_MAPPER_BEANNAME);
    }

    private String getRocketMQListener(Element element, BeanDefinitionRegistry beanDefinitionRegistry) {
        String beanRef = element.getAttribute(XmlAttributeConstant.beanRef);
        Assert.notNull(beanRef, "RocketMQ consumer ref bean 不能为空");
        BeanDefinition rocketMQBean = beanDefinitionRegistry.getBeanDefinition(beanRef);
        String className = rocketMQBean.getBeanClassName();
        Class clazz = null;
        try {
            clazz = Class.forName(className);
        } catch (ClassNotFoundException e) {
            throw new BeanCreationException(String.format("未找到%s类,请查看该类是否存在", className));
        }
        if (!RocketMQListener.class.isAssignableFrom(clazz)) {
            throw new BeanCreationException(String.format("该类%s没有继承RocketMQListener," +
                    "请详细查看文档(https://quvideo.worktile.com/drive/5c32b3bcf553e62a90f585a5/5c32b441f553e62a90f585ab)", className));
        }
        return beanRef;
    }

    private SelectorType getSelectType(Element element) {
        String selectType = element.getAttribute(XmlAttributeConstant.selectorType);
        Assert.hasText(selectType, "RocketMQ consume xml配置 select-type 不能为空");
        return SelectorType.valueOf(selectType.toUpperCase());
    }

    private ConsumeMode getConsumeMode(Element element) {
        String consumeMode = element.getAttribute(XmlAttributeConstant.consumeMode);
        Assert.hasText(consumeMode, "RocketMQ consume xml配置 consume-model 不能为空");
        return ConsumeMode.valueOf(consumeMode.toUpperCase());
    }

    private MessageModel getMessageMode(Element element) {
        String messageModel = element.getAttribute(XmlAttributeConstant.messageModel);
        Assert.hasText(messageModel, "RocketMQ consume xml配置 message-model 不能为空");
        return MessageModel.valueOf(messageModel.toUpperCase());
    }

}
