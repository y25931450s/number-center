/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.xiaoying.common.mq.support;

import com.dianping.cat.Cat;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.xiaoying.common.mq.annotation.RocketMQMessageListener;
import com.xiaoying.common.mq.constant.ConsumeMode;
import com.xiaoying.common.mq.constant.MessageModel;
import com.xiaoying.common.mq.constant.RocketMQConstant;
import com.xiaoying.common.mq.constant.SelectorType;
import com.xiaoying.common.mq.core.RocketMQListener;
import com.xiaoying.common.mq.core.RocketMQPushConsumerLifecycleListener;
import lombok.Data;
import org.apache.rocketmq.client.consumer.DefaultMQPushConsumer;
import org.apache.rocketmq.client.consumer.MessageSelector;
import org.apache.rocketmq.client.consumer.listener.*;
import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.common.message.MessageExt;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.aop.support.AopUtils;
import org.springframework.beans.factory.InitializingBean;
import org.springframework.context.SmartLifecycle;
import org.springframework.util.Assert;

import java.lang.reflect.ParameterizedType;
import java.lang.reflect.Type;
import java.nio.charset.Charset;
import java.util.List;
import java.util.Objects;

@SuppressWarnings("WeakerAccess")
@Data
public class DefaultRocketMQListenerContainer implements InitializingBean, RocketMQListenerContainer, SmartLifecycle {
    private final static Logger log = LoggerFactory.getLogger(DefaultRocketMQListenerContainer.class);

    private long suspendCurrentQueueTimeMillis = 1000;

    /**
     * Message consume retry strategy<br> -1,no retry,put into DLQ directly<br> 0,broker control retry frequency<br>
     * >0,client control retry frequency.
     */
    private int delayLevelWhenNextConsume = 0;

    private String nameServer;

    private String consumerGroup;

    private String topic;

    private int consumeThreadMax = 64;

    private String charset = "UTF-8";

    private ObjectMapper objectMapper;

    private RocketMQListener rocketMQListener;


    private DefaultMQPushConsumer consumer;

    private Class messageType;

    private boolean running;

    /**
     * 最大重试次数
     */
    private int maxRetryTimes;

    /**
     * 是否开启消费追踪，默认为false
     */
    private boolean enableTrace;
    /**
     * 下面的属性从RocketMQMessageListener copy过来
     */
    private ConsumeMode consumeMode;
    private SelectorType selectorType;
    private String selectorExpression;
    private MessageModel messageModel;


    public void copyPropertiesFromRocketMqMessageListener(RocketMQMessageListener rocketMQMessageListener) {
        this.consumeMode = rocketMQMessageListener.consumeMode();
        this.consumeThreadMax = rocketMQMessageListener.consumeThreadMax();
        this.messageModel = rocketMQMessageListener.messageModel();
        this.selectorExpression = rocketMQMessageListener.selectorExpression();
        this.selectorType = rocketMQMessageListener.selectorType();
        this.delayLevelWhenNextConsume = rocketMQMessageListener.delayLevel();
        this.enableTrace = rocketMQMessageListener.enableTrace();
        this.maxRetryTimes = rocketMQMessageListener.maxRetryTimes();
    }

    @Override
    public void setupMessageListener(RocketMQListener rocketMQListener) {
        this.rocketMQListener = rocketMQListener;
    }

    @Override
    public void destroy() {
        this.setRunning(false);
        if (Objects.nonNull(consumer)) {
            consumer.shutdown();
        }
        log.info("container destroyed, {}", this.toString());
    }

    @Override
    public boolean isAutoStartup() {
        return true;
    }

    @Override
    public void stop(Runnable callback) {
        stop();
        callback.run();
    }

    @Override
    public void start() {
        if (this.isRunning()) {
            throw new IllegalStateException("container already running. " + this.toString());
        }

        try {
            consumer.start();
        } catch (MQClientException e) {
            throw new IllegalStateException("Failed to start RocketMQ push consumer", e);
        }
        this.setRunning(true);

        log.info("running container: {}", this.toString());
    }

    @Override
    public void stop() {
        if (this.isRunning()) {
            if (Objects.nonNull(consumer)) {
                consumer.shutdown();
            }
            setRunning(false);
        }
    }

    @Override
    public boolean isRunning() {
        return running;
    }

    private void setRunning(boolean running) {
        this.running = running;
    }

    @Override
    public int getPhase() {
        // Returning Integer.MAX_VALUE only suggests that
        // we will be the first bean to shutdown and last bean to start
        return Integer.MAX_VALUE;
    }


    @Override
    public void afterPropertiesSet() throws Exception {
        initRocketMQPushConsumer();
        this.messageType = getMessageType();
        log.debug("RocketMQ messageType: {}", messageType.getName());
    }

    @Override
    public String toString() {
        return "DefaultRocketMQListenerContainer{" +
                "consumerGroup='" + consumerGroup + '\'' +
                ", nameServer='" + nameServer + '\'' +
                ", topic='" + topic + '\'' +
                ", consumeMode=" + consumeMode +
                ", selectorType=" + selectorType +
                ", selectorExpression='" + selectorExpression + '\'' +
                ", messageModel=" + messageModel +
                '}';
    }

    /**
     * 消费非顺序消息
     */
    public class DefaultMessageListenerConcurrently implements MessageListenerConcurrently {

        @SuppressWarnings("unchecked")
        @Override
        public ConsumeConcurrentlyStatus consumeMessage(List<MessageExt> msgs, ConsumeConcurrentlyContext context) {
            for (MessageExt messageExt : msgs) {
                log.debug("received msg: {}", messageExt);
                CatTranscation transaction = new CatTranscation(RocketMQConstant.CAT_TRANSACTION_CONSUMER_TYPE, messageExt.getTopic() + ":" + messageExt.getTags());
                transaction.addEvent("messageId", messageExt.getMsgId());
                try {
                    rocketMQListener.onMessage(doConvertMessage(messageExt));
                } catch (Exception e) {
                    transaction.addEvent("reconsumeTimes", messageExt.getReconsumeTimes() + "");
                    transaction.setStatus(e);
                    log.warn("consume message failed.{}", String.format("message topic:%s,tags:%s,id:%s,consumeTime:%s",
                            messageExt.getTopic(), messageExt.getTags(), messageExt.getMsgId(), messageExt.getReconsumeTimes()), e);
                    if (maxRetryTimes == RocketMQConstant.DEFAULT_CONSUMER_MAX_RETRY_TIMES
                            || messageExt.getReconsumeTimes() < maxRetryTimes) {
                        context.setDelayLevelWhenNextConsume(delayLevelWhenNextConsume);
                        return ConsumeConcurrentlyStatus.RECONSUME_LATER;
                    } else {
                        return ConsumeConcurrentlyStatus.CONSUME_SUCCESS;
                    }
                } finally {
                    transaction.complete();
                }
            }

            return ConsumeConcurrentlyStatus.CONSUME_SUCCESS;
        }
    }

    /**
     * 消费顺序消息
     */
    public class DefaultMessageListenerOrderly implements MessageListenerOrderly {

        @SuppressWarnings("unchecked")
        @Override
        public ConsumeOrderlyStatus consumeMessage(List<MessageExt> msgs, ConsumeOrderlyContext context) {
            for (MessageExt messageExt : msgs) {
                CatTranscation transaction = new CatTranscation(RocketMQConstant.CAT_TRANSACTION_CONSUMER_TYPE, messageExt.getTopic() + ":" + messageExt.getTags());
                Cat.logEvent("messageId", messageExt.getMsgId());
                log.debug("received msg: {}", messageExt);
                try {
                    rocketMQListener.onMessage(doConvertMessage(messageExt));
                } catch (Exception e) {
                    transaction.addEvent("reconsumeTimes", messageExt.getReconsumeTimes() + "");
                    transaction.setStatus(e);
                    log.warn("consume message failed.{}", String.format("The message topic:%s,tags:%s," +
                            "id:%s", messageExt.getTopic(), messageExt.getTags(), messageExt.getMsgId()), e);
                    if (maxRetryTimes == RocketMQConstant.DEFAULT_CONSUMER_MAX_RETRY_TIMES
                            || messageExt.getReconsumeTimes() < maxRetryTimes) {
                        context.setSuspendCurrentQueueTimeMillis(suspendCurrentQueueTimeMillis);
                        return ConsumeOrderlyStatus.SUSPEND_CURRENT_QUEUE_A_MOMENT;
                    } else {
                        return ConsumeOrderlyStatus.SUCCESS;
                    }
                } finally {
                    transaction.complete();
                }
            }

            return ConsumeOrderlyStatus.SUCCESS;
        }
    }


    @SuppressWarnings("unchecked")
    private Object doConvertMessage(MessageExt messageExt) {
        if (Objects.equals(messageType, MessageExt.class)) {
            return messageExt;
        } else {
            String str = new String(messageExt.getBody(), Charset.forName(charset));
            if (Objects.equals(messageType, String.class)) {
                return str;
            } else {
                try {
                    // JSON里面包含了实体没有的字段导致反序列化失败,故开启该特性
                    objectMapper.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);
                    return objectMapper.readValue(str, messageType);
                } catch (Exception e) {
                    log.info("convert failed. str:{}, msgType:{}", str, messageType);
                    throw new RuntimeException("cannot convert message to " + messageType, e);
                }
            }
        }
    }

    private Class getMessageType() {
        Class<?> targetClass = AopUtils.getTargetClass(rocketMQListener);
        Type[] interfaces = targetClass.getGenericInterfaces();
        Class<?> superclass = targetClass.getSuperclass();
        while ((Objects.isNull(interfaces) || 0 == interfaces.length) && Objects.nonNull(superclass)) {
            interfaces = superclass.getGenericInterfaces();
            superclass = targetClass.getSuperclass();
        }
        if (Objects.nonNull(interfaces)) {
            for (Type type : interfaces) {
                if (type instanceof ParameterizedType) {
                    ParameterizedType parameterizedType = (ParameterizedType) type;
                    if (Objects.equals(parameterizedType.getRawType(), RocketMQListener.class)) {
                        Type[] actualTypeArguments = parameterizedType.getActualTypeArguments();
                        if (Objects.nonNull(actualTypeArguments) && actualTypeArguments.length > 0) {
                            return (Class) actualTypeArguments[0];
                        } else {
                            return Object.class;
                        }
                    }
                }
            }

            return Object.class;
        } else {
            return Object.class;
        }
    }

    public void initRocketMQPushConsumer() throws MQClientException {
        Assert.notNull(rocketMQListener, "Property 'rocketMQListener' is required");
        Assert.notNull(consumerGroup, "Property 'consumerGroup' is required");
        Assert.notNull(nameServer, "Property 'nameServer' is required");
        Assert.notNull(topic, "Property 'topic' is required");

        consumer = new DefaultMQPushConsumer(consumerGroup, enableTrace);
        consumer.setNamesrvAddr(nameServer);
        consumer.setConsumeThreadMax(consumeThreadMax);
        if (consumeThreadMax < consumer.getConsumeThreadMin()) {
            consumer.setConsumeThreadMin(consumeThreadMax);
        }

        switch (messageModel) {
            case BROADCASTING:
                consumer.setMessageModel(org.apache.rocketmq.common.protocol.heartbeat.MessageModel.BROADCASTING);
                break;
            case CLUSTERING:
                consumer.setMessageModel(org.apache.rocketmq.common.protocol.heartbeat.MessageModel.CLUSTERING);
                break;
            default:
                throw new IllegalArgumentException("Property 'messageModel' was wrong.");
        }

        switch (selectorType) {
            case TAG:
                consumer.subscribe(topic, selectorExpression);
                break;
            case SQL92:
                consumer.subscribe(topic, MessageSelector.bySql(selectorExpression));
                break;
            default:
                throw new IllegalArgumentException("Property 'selectorType' was wrong.");
        }

        switch (consumeMode) {
            case ORDERLY:
                consumer.setMessageListener(new DefaultMessageListenerOrderly());
                break;
            case CONCURRENTLY:
                consumer.setMessageListener(new DefaultMessageListenerConcurrently());
                break;
            default:
                throw new IllegalArgumentException("Property 'consumeMode' was wrong.");
        }

        if (rocketMQListener instanceof RocketMQPushConsumerLifecycleListener) {
            ((RocketMQPushConsumerLifecycleListener) rocketMQListener).prepareStart(consumer);
        }

    }

}
