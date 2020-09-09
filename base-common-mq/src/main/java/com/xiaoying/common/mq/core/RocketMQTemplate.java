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

package com.xiaoying.common.mq.core;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.xiaoying.common.mq.constant.RocketMQConstant;
import com.xiaoying.common.mq.support.CatTranscation;
import com.xiaoying.common.mq.support.RocketMQUtil;
import org.apache.rocketmq.client.Validators;
import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.client.producer.DefaultMQProducer;
import org.apache.rocketmq.client.producer.MessageQueueSelector;
import org.apache.rocketmq.client.producer.SendCallback;
import org.apache.rocketmq.client.producer.SendResult;
import org.apache.rocketmq.client.producer.selector.SelectMessageQueueByHash;
import org.apache.rocketmq.common.message.MessageBatch;
import org.apache.rocketmq.common.message.MessageClientIDSetter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.DisposableBean;
import org.springframework.beans.factory.InitializingBean;
import org.springframework.messaging.Message;
import org.springframework.messaging.MessageHeaders;
import org.springframework.messaging.MessagingException;
import org.springframework.messaging.core.AbstractMessageSendingTemplate;
import org.springframework.messaging.core.MessagePostProcessor;
import org.springframework.messaging.support.MessageBuilder;
import org.springframework.util.Assert;
import org.springframework.util.MimeTypeUtils;

import java.util.*;

/**
 * 发送消息模板类
 *
 * @author misskey
 * @author lijin
 */
@SuppressWarnings({"WeakerAccess", "unused"})
public class RocketMQTemplate extends AbstractMessageSendingTemplate<String> implements InitializingBean, DisposableBean {
    private static final Logger log = LoggerFactory.getLogger(RocketMQTemplate.class);

    private DefaultMQProducer producer;

    private ObjectMapper objectMapper;

    private String charset = "UTF-8";

    private MessageQueueSelector messageQueueSelector = new SelectMessageQueueByHash();

    public RocketMQTemplate() {
    }

    public RocketMQTemplate(DefaultMQProducer producer, ObjectMapper objectMapper) {
        this.producer = producer;
        this.objectMapper = objectMapper;
    }

    public DefaultMQProducer getProducer() {
        return producer;
    }

    public void setProducer(DefaultMQProducer producer) {
        this.producer = producer;
    }

    public ObjectMapper getObjectMapper() {
        return objectMapper;
    }

    public void setObjectMapper(ObjectMapper objectMapper) {
        this.objectMapper = objectMapper;
    }

    public String getCharset() {
        return charset;
    }

    public void setCharset(String charset) {
        this.charset = charset;
    }

    public MessageQueueSelector getMessageQueueSelector() {
        return messageQueueSelector;
    }

    public void setMessageQueueSelector(MessageQueueSelector messageQueueSelector) {
        this.messageQueueSelector = messageQueueSelector;
    }

    /**
     * <p> Send message in synchronous mode. This method returns only when the sending procedure totally completes.
     * Reliable synchronous transmission is used in extensive scenes, such as important notification messages, SMS
     * notification, SMS marketing system, etc.. </p>
     * <p>
     * <strong>Warn:</strong> this method has internal retry-mechanism, that is, internal implementation will retry
     * {@link DefaultMQProducer#getRetryTimesWhenSendFailed} times before claiming failure. As a result, multiple
     * messages may potentially delivered to broker(s). It's up to the application developers to resolve potential
     * duplication issue.
     *
     * @param destination formats: `topicName:tags`
     * @param message     {@link org.springframework.messaging.Message}
     * @return {@link SendResult}
     */
    public SendResult syncSend(String destination, Message<?> message) {
        return syncSend(destination, message, producer.getSendMsgTimeout());
    }

    /**
     * Same to {@link #syncSend(String, Message)} with send timeout specified in addition.
     *
     * @param destination formats: `topicName:tags`
     * @param message     {@link org.springframework.messaging.Message}
     * @param timeout     send timeout with millis
     * @return {@link SendResult}
     */
    public SendResult syncSend(String destination, Message<?> message, long timeout) {
        return syncSend(destination, message, timeout, 0);
    }

    /**
     * Same to {@link #syncSend(String, Message)} with send timeout specified in addition.
     *
     * @param destination formats: `topicName:tags`
     * @param message     {@link org.springframework.messaging.Message}
     * @param timeout     send timeout with millis
     * @param delayLevel  level for the delay message
     * @return {@link SendResult}
     */
    public SendResult syncSend(String destination, Message<?> message, long timeout, int delayLevel) {
        if (Objects.isNull(message) || Objects.isNull(message.getPayload())) {
            log.error("syncSend failed. destination:{}, message is null ", destination);
            throw new IllegalArgumentException("`message` and `message.payload` cannot be null");
        }
        CatTranscation transaction = new CatTranscation(RocketMQConstant.CAT_TRANSACTION_PRODUCE_TYPE, destination);
        try {
            long now = System.currentTimeMillis();
            org.apache.rocketmq.common.message.Message rocketMsg = RocketMQUtil.convertToRocketMessage(objectMapper,
                    charset, destination, message);
            if (delayLevel > 0) {
                rocketMsg.setDelayTimeLevel(delayLevel);
            }
            SendResult sendResult = producer.send(rocketMsg, timeout);
            long costTime = System.currentTimeMillis() - now;
            transaction.addEvent("messageId", sendResult.getMsgId());
            transaction.addEvent("sendStatus", sendResult.getSendStatus().toString());
            log.debug("send message cost: {} ms, msgId:{}", costTime, sendResult.getMsgId());
            return sendResult;
        } catch (Exception e) {
            transaction.setStatus(e);
            log.error("syncSend failed. destination:{}, message:{} ", destination, message);
            throw new MessagingException(e.getMessage(), e);
        } finally {
            transaction.complete();
        }
    }


    /**
     * 同步发送批量消息
     * @param destination topic + tag  比如"test:tag"
     * @param payloads 消息内容，如果需要定制化些参数，@see org.springframework.messaging.Message
     * @throws MessagingException,IllegalArgumentException
     * @return
     */
    public SendResult syncBatchSend(String destination,List<Object> payloads) {
       return syncBatchSend(destination,payloads,producer.getSendMsgTimeout());
    }

    /**
     * 批量发送消息
     * @param destination  topic + tag  比如"test:tag"
     * @param payloads 消息内容，如果需要定制化些参数，@see org.springframework.messaging.Message
     * @param timeout 毫秒
     * @throws MessagingException,IllegalArgumentException
     * @return
     */
    public SendResult syncBatchSend(String destination, List<Object> payloads, int timeout) {
        if (Objects.isNull(payloads) || payloads.isEmpty()) {
            log.error("syncBatchSend failed. message is null.");
            throw new IllegalArgumentException("payloads cannot be null");
        }
        List<org.apache.rocketmq.common.message.Message> batchMessages = transformRocketMessage(payloads,destination);
        CatTranscation transaction = new CatTranscation(RocketMQConstant.CAT_TRANSACTION_PRODUCE_BATCH_TYPE, destination);
        try {
            long now = System.currentTimeMillis();
            SendResult sendResult = producer.send(batchMessages, timeout);
            transaction.addEvent("messageId", sendResult.getMsgId());
            transaction.addEvent("sendStatus", sendResult.getSendStatus().toString());
            long costTime = System.currentTimeMillis() - now;
            return  sendResult;
        } catch (Exception e) {
            transaction.setStatus(e);
            log.error("syncBatchSend failed. destination:{}", destination);
            throw new MessagingException(e.getMessage(), e);
        }finally {
            transaction.complete();
        }
    }

    private void checkPayLoad(List<Object> payloads) {
        if (Objects.isNull(payloads) || payloads.isEmpty()) {
            log.error("syncBatchSend failed. Message is null.");
            throw new IllegalArgumentException("payloads cannot be null");
        }
    }


    private List<org.apache.rocketmq.common.message.Message> transformRocketMessage(List<Object> payloads,String destination) {
        List<org.apache.rocketmq.common.message.Message> batchMessages = new ArrayList<>(payloads.size());
        for (int i = 0; i <  payloads.size(); i++) {
            Object payload = payloads.get(i);
            Message<?> message = null;
            if (payload instanceof  Message) {
                message = (Message<?>) payload;
            } else {
                message = this.doConvert(payload, null, null);
            }
            org.apache.rocketmq.common.message.Message rocketMqMessage =RocketMQUtil.convertToRocketMessage(objectMapper,"UTF8",destination,message);
            // 消息校验
            try {
                Validators.checkMessage(rocketMqMessage, producer);
            } catch (MQClientException e) {
                throw new MessagingException(e.getMessage(), e);
            }
            MessageClientIDSetter.setUniqID(rocketMqMessage);
            batchMessages.add(rocketMqMessage);
        }
        return batchMessages;
    }

    /**
     * Same to {@link #syncSend(String, Message)}.
     *
     * @param destination formats: `topicName:tags`
     * @param payload     the Object to use as payload
     * @return {@link SendResult}
     */
    public SendResult syncSend(String destination, Object payload) {
        return syncSend(destination, payload, producer.getSendMsgTimeout());
    }

    /**
     * Same to {@link #syncSend(String, Object)} with send timeout specified in addition.
     *
     * @param destination formats: `topicName:tags`
     * @param payload     the Object to use as payload
     * @param timeout     send timeout with millis
     * @return {@link SendResult}
     */
    public SendResult syncSend(String destination, Object payload, long timeout) {
        Message<?> message = this.doConvert(payload, null, null);
        return syncSend(destination, message, timeout);
    }

    /**
     * Same to {@link #syncSend(String, Message)} with send orderly with hashKey by specified.
     *
     * @param destination formats: `topicName:tags`
     * @param message     {@link org.springframework.messaging.Message}
     * @param hashKey     use this key to select queue. for example: orderId, productId ...
     * @return {@link SendResult}
     */
    public SendResult syncSendOrderly(String destination, Message<?> message, String hashKey) {
        return syncSendOrderly(destination, message, hashKey, producer.getSendMsgTimeout());
    }

    /**
     * Same to {@link #syncSendOrderly(String, Message, String)} with send timeout specified in addition.
     *
     * @param destination formats: `topicName:tags`
     * @param message     {@link org.springframework.messaging.Message}
     * @param hashKey     use this key to select queue. for example: orderId, productId ...
     * @param timeout     send timeout with millis
     * @return {@link SendResult}
     */
    public SendResult syncSendOrderly(String destination, Message<?> message, String hashKey, long timeout) {
        if (Objects.isNull(message) || Objects.isNull(message.getPayload())) {
            log.error("syncSendOrderly failed. destination:{}, message is null ", destination);
            throw new IllegalArgumentException("`message` and `message.payload` cannot be null");
        }
        CatTranscation transaction = new CatTranscation(RocketMQConstant.CAT_TRANSACTION_PRODUCE_TYPE, destination);
        try {
            long now = System.currentTimeMillis();
            org.apache.rocketmq.common.message.Message rocketMsg = RocketMQUtil.convertToRocketMessage(objectMapper,
                    charset, destination, message);
            SendResult sendResult = producer.send(rocketMsg, messageQueueSelector, hashKey, timeout);
            transaction.addEvent("messageId", sendResult.getMsgId());
            transaction.addEvent("sendStatus", sendResult.getSendStatus().toString());
            long costTime = System.currentTimeMillis() - now;
            log.debug("send message cost: {} ms, msgId:{}", costTime, sendResult.getMsgId());
            return sendResult;
        } catch (Exception e) {
            transaction.setStatus(e);
            log.error("syncSendOrderly failed. destination:{}, message:{} ", destination, message);
            throw new MessagingException(e.getMessage(), e);
        } finally {
            transaction.complete();
        }
    }

    /**
     * Same to {@link #syncSend(String, Object)} with send orderly with hashKey by specified.
     *
     * @param destination formats: `topicName:tags`
     * @param payload     the Object to use as payload
     * @param hashKey     use this key to select queue. for example: orderId, productId ...
     * @return {@link SendResult}
     */
    public SendResult syncSendOrderly(String destination, Object payload, String hashKey) {
        return syncSendOrderly(destination, payload, hashKey, producer.getSendMsgTimeout());
    }

    /**
     * Same to {@link #syncSendOrderly(String, Object, String)} with send timeout specified in addition.
     *
     * @param destination formats: `topicName:tags`
     * @param payload     the Object to use as payload
     * @param hashKey     use this key to select queue. for example: orderId, productId ...
     * @param timeout     send timeout with millis
     * @return {@link SendResult}
     */
    public SendResult syncSendOrderly(String destination, Object payload, String hashKey, long timeout) {
        Message<?> message = this.doConvert(payload, null, null);
        return syncSendOrderly(destination, message, hashKey, producer.getSendMsgTimeout());
    }


    /**
     * 发送顺序消息
     * @param destination 格式: topic:tags
     * @param payloads 消息内容，如果需要定制化些参数，@see org.springframework.messaging.Message
     * @param hashKey 定位逻辑的消息队列,用于保证消息的顺序
     * @return
     */
    public SendResult syncBatchSendOrderly(String destination, List<Object> payloads,String hashKey) {
        return  syncBatchSendOrderly(destination,payloads,hashKey,producer.getSendMsgTimeout());
    }


    /**
     * 发送顺序消息
     * @param destination 格式: topic:tags
     * @param payloads 消息内容，如果需要定制化些参数，@see org.springframework.messaging.Message
     * @param hashKey 定位逻辑的消息队列,用于保证消息的顺序
     * @param timeout 超时时间
     * @return
     */
    public SendResult syncBatchSendOrderly(String destination, List<Object> payloads,String hashKey,int timeout) {
        checkPayLoad(payloads);
        List<org.apache.rocketmq.common.message.Message> messages = transformRocketMessage(payloads,destination);
        CatTranscation transaction = new CatTranscation(RocketMQConstant.CAT_TRANSACTION_PRODUCE_BATCH_TYPE, destination);
        try {
            long now = System.currentTimeMillis();
            MessageBatch messageBatch = MessageBatch.generateFromList(messages);
            messageBatch.setBody(messageBatch.encode());
            SendResult sendResult = producer.send(messageBatch,messageQueueSelector,hashKey, timeout);
            transaction.addEvent("messageId", sendResult.getMsgId());
            transaction.addEvent("sendStatus", sendResult.getSendStatus().toString());
            long costTime = System.currentTimeMillis() - now;
            return  sendResult;
        } catch (Exception e) {
            transaction.setStatus(e);
            log.error("syncSend failed. destination:{}", destination);
            throw new MessagingException(e.getMessage(), e);
        }finally {
            transaction.complete();
        }
    }

    /**
     * Same to {@link #asyncSend(String, Message, SendCallback)} with send timeout specified in addition.
     *
     * @param destination  formats: `topicName:tags`
     * @param message      {@link org.springframework.messaging.Message}
     * @param sendCallback {@link SendCallback}
     * @param timeout      send timeout with millis
     */
    public void asyncSend(String destination, Message<?> message, SendCallback sendCallback, long timeout) {
        if (Objects.isNull(message) || Objects.isNull(message.getPayload())) {
            log.error("asyncSend failed. destination:{}, message is null ", destination);
            throw new IllegalArgumentException("`message` and `message.payload` cannot be null");
        }
        CatTranscation transaction = new CatTranscation(RocketMQConstant.CAT_TRANSACTION_PRODUCE_TYPE, destination);
        try {
            org.apache.rocketmq.common.message.Message rocketMsg = RocketMQUtil.convertToRocketMessage(objectMapper,
                    charset, destination, message);
            producer.send(rocketMsg, sendCallback, timeout);
        } catch (Exception e) {
            transaction.setStatus(e);
            log.info("asyncSend failed. destination:{}, message:{} ", destination, message);
            throw new MessagingException(e.getMessage(), e);
        } finally {
            transaction.complete();
        }
    }


    /**
     * <p> Send message to broker asynchronously. asynchronous transmission is generally used in response time sensitive
     * business scenarios. </p>
     * <p>
     * This method returns immediately. On sending completion, <code>sendCallback</code> will be executed.
     * <p>
     * Similar to {@link #syncSend(String, Object)}, internal implementation would potentially retry up to {@link
     * DefaultMQProducer#getRetryTimesWhenSendAsyncFailed} times before claiming sending failure, which may yield
     * message duplication and application developers are the one to resolve this potential issue.
     *
     * @param destination  formats: `topicName:tags`
     * @param message      {@link org.springframework.messaging.Message}
     * @param sendCallback {@link SendCallback}
     */
    public void asyncSend(String destination, Message<?> message, SendCallback sendCallback) {
        asyncSend(destination, message, sendCallback, producer.getSendMsgTimeout());
    }

    /**
     * Same to {@link #asyncSend(String, Object, SendCallback)} with send timeout specified in addition.
     *
     * @param destination  formats: `topicName:tags`
     * @param payload      the Object to use as payload
     * @param sendCallback {@link SendCallback}
     * @param timeout      send timeout with millis
     */
    public void asyncSend(String destination, Object payload, SendCallback sendCallback, long timeout) {
        Message<?> message = this.doConvert(payload, null, null);
        asyncSend(destination, message, sendCallback, timeout);
    }

    /**
     * Same to {@link #asyncSend(String, Message, SendCallback)}.
     *
     * @param destination  formats: `topicName:tags`
     * @param payload      the Object to use as payload
     * @param sendCallback {@link SendCallback}
     */
    public void asyncSend(String destination, Object payload, SendCallback sendCallback) {
        asyncSend(destination, payload, sendCallback, producer.getSendMsgTimeout());
    }


    /**
     * 批量异步发送消息
     * @param destination  格式: topic:tags
     * @param payloads 消息内容，如果需要定制化些参数，@see org.springframework.messaging.Message
     * @param sendCallback 回调函数
     */
    public void asyncBatchSend(String destination,List<Object> payloads,SendCallback sendCallback) {
        asyncBatchSend(destination,payloads,sendCallback,producer.getSendMsgTimeout());
    }

    /**
     * 批量异步发送消息
     * @param destination  格式: topic:tags
     * @param payloads 消息内容，如果需要定制化些参数，@see org.springframework.messaging.Message
     * @param sendCallback 回调函数
     * @param timeout 超时时间
     */
    public void asyncBatchSend(String destination,List<Object> payloads,SendCallback sendCallback,int timeout) {

        if (Objects.isNull(payloads) ||payloads.isEmpty()) {
            log.error("asyncBatchSend failed. destination:{}, message is null ", destination);
            throw new IllegalArgumentException("`message` and `message.payload` cannot be null");
        }
        CatTranscation transaction = new CatTranscation(RocketMQConstant.CAT_TRANSACTION_PRODUCE_TYPE, destination);
        try {
            List<org.apache.rocketmq.common.message.Message> messages = transformRocketMessage(payloads,destination);
            MessageBatch messageBatch = MessageBatch.generateFromList(messages);
            messageBatch.setBody(messageBatch.encode());
            producer.send(messageBatch, sendCallback, timeout);
        } catch (Exception e) {
            transaction.setStatus(e);
            log.error("asyncBatchSend failed. destination:{} ", destination);
            throw new MessagingException(e.getMessage(), e);
        } finally {
            transaction.complete();
        }
    }

    /**
     * Same to {@link #asyncSendOrderly(String, Message, String, SendCallback)} with send timeout specified in
     * addition.
     *
     * @param destination  formats: `topicName:tags`
     * @param message      {@link org.springframework.messaging.Message}
     * @param hashKey      use this key to select queue. for example: orderId, productId ...
     * @param sendCallback {@link SendCallback}
     * @param timeout      send timeout with millis
     */
    public void asyncSendOrderly(String destination, Message<?> message, String hashKey, SendCallback sendCallback,
                                 long timeout) {
        if (Objects.isNull(message) || Objects.isNull(message.getPayload())) {
            log.error("asyncSendOrderly failed. destination:{}, message is null ", destination);
            throw new IllegalArgumentException("`message` and `message.payload` cannot be null");
        }
        CatTranscation transaction = new CatTranscation(RocketMQConstant.CAT_TRANSACTION_PRODUCE_TYPE, destination);
        try {
            org.apache.rocketmq.common.message.Message rocketMsg = RocketMQUtil.convertToRocketMessage(objectMapper,
                    charset, destination, message);
            producer.send(rocketMsg, messageQueueSelector, hashKey, sendCallback, timeout);
        } catch (Exception e) {
            transaction.setStatus(e);
            log.error("asyncSendOrderly failed. destination:{}, message:{} ", destination, message);
            throw new MessagingException(e.getMessage(), e);
        } finally {
            transaction.complete();
        }
    }

    /**
     * Same to {@link #asyncSend(String, Message, SendCallback)} with send orderly with hashKey by specified.
     *
     * @param destination  formats: `topicName:tags`
     * @param message      {@link org.springframework.messaging.Message}
     * @param hashKey      use this key to select queue. for example: orderId, productId ...
     * @param sendCallback {@link SendCallback}
     */
    public void asyncSendOrderly(String destination, Message<?> message, String hashKey, SendCallback sendCallback) {
        asyncSendOrderly(destination, message, hashKey, sendCallback, producer.getSendMsgTimeout());
    }

    /**
     * Same to {@link #asyncSendOrderly(String, Message, String, SendCallback)}.
     *
     * @param destination  formats: `topicName:tags`
     * @param payload      the Object to use as payload
     * @param hashKey      use this key to select queue. for example: orderId, productId ...
     * @param sendCallback {@link SendCallback}
     */
    public void asyncSendOrderly(String destination, Object payload, String hashKey, SendCallback sendCallback) {
        asyncSendOrderly(destination, payload, hashKey, sendCallback, producer.getSendMsgTimeout());
    }

    /**
     * Same to {@link #asyncSendOrderly(String, Object, String, SendCallback)} with send timeout specified in addition.
     *
     * @param destination  formats: `topicName:tags`
     * @param payload      the Object to use as payload
     * @param hashKey      use this key to select queue. for example: orderId, productId ...
     * @param sendCallback {@link SendCallback}
     * @param timeout      send timeout with millis
     */
    public void asyncSendOrderly(String destination, Object payload, String hashKey, SendCallback sendCallback,
                                 long timeout) {
        Message<?> message = this.doConvert(payload, null, null);
        asyncSendOrderly(destination, message, hashKey, sendCallback, timeout);
    }

    /**
     * 批量异步发送顺序消息
     * @param destination  格式: topic:tags
     * @param payloads 消息内容，如果需要定制化些参数，@see org.springframework.messaging.Message
     * @param hashKey  定位发送那个消息队列(逻辑队列）
     * @param sendCallback 回调函数
     */
    public void asyncBatchSendOrderly(String destination,List<Object> payloads,String hashKey,SendCallback sendCallback) {
        asyncBatchSendOrderly(destination,payloads,hashKey,sendCallback,producer.getSendMsgTimeout());
    }

    /**
     * 批量异步发送顺序消息
     * @param destination  格式: topic:tags
     * @param payloads 消息内容，如果需要定制化些参数，@see org.springframework.messaging.Message
     * @param hashKey  定位发送那个消息队列(逻辑队列）
     * @param sendCallback 回调函数
     * @param timeout 超时时间
     */
    public void asyncBatchSendOrderly(String destination,List<Object> payloads,String hashKey,SendCallback sendCallback,int timeout) {

        if (Objects.isNull(payloads) ||payloads.isEmpty()) {
            log.error("asyncBatchSendOrderly failed. destination:{}, message is null ", destination);
            throw new IllegalArgumentException("`message` and `message.payload` cannot be null");
        }
        CatTranscation transaction = new CatTranscation(RocketMQConstant.CAT_TRANSACTION_PRODUCE_TYPE, destination);
        try {
            List<org.apache.rocketmq.common.message.Message> messages =  transformRocketMessage(payloads,destination);
            MessageBatch messageBatch = MessageBatch.generateFromList(messages);
            messageBatch.setBody(messageBatch.encode());
            producer.send(messageBatch, messageQueueSelector, hashKey, sendCallback, timeout);
        } catch (Exception e) {
            transaction.setStatus(e);
            log.error("asyncBatchSendOrderly failed. destination:{} ", destination);
            throw new MessagingException(e.getMessage(), e);
        } finally {
            transaction.complete();
        }
    }

    /**
     * Similar to <a href="https://en.wikipedia.org/wiki/User_Datagram_Protocol">UDP</a>, this method won't wait for
     * acknowledgement from broker before return. Obviously, it has maximums throughput yet potentials of message loss.
     * <p>
     * One-way transmission is used for cases requiring moderate reliability, such as log collection.
     *
     * @param destination formats: `topicName:tags`
     * @param message     {@link org.springframework.messaging.Message}
     */
    public void sendOneWay(String destination, Message<?> message) {
        if (Objects.isNull(message) || Objects.isNull(message.getPayload())) {
            log.error("sendOneWay failed. destination:{}, message is null ", destination);
            throw new IllegalArgumentException("`message` and `message.payload` cannot be null");
        }
        CatTranscation transaction = new CatTranscation(RocketMQConstant.CAT_TRANSACTION_PRODUCE_TYPE, destination);
        try {
            org.apache.rocketmq.common.message.Message rocketMsg = RocketMQUtil.convertToRocketMessage(objectMapper,
                    charset, destination, message);
            producer.sendOneway(rocketMsg);
        } catch (Exception e) {
            transaction.setStatus(e);
            log.error("sendOneWay failed. destination:{}, message:{} ", destination, message);
            throw new MessagingException(e.getMessage(), e);
        } finally {
            transaction.complete();
        }
    }

    /**
     * Same to {@link #sendOneWay(String, Message)}
     *
     * @param destination formats: `topicName:tags`
     * @param payload     the Object to use as payload
     */
    public void sendOneWay(String destination, Object payload) {
        Message<?> message = this.doConvert(payload, null, null);
        sendOneWay(destination, message);
    }

    /**
     * Same to {@link #sendOneWay(String, Message)} with send orderly with hashKey by specified.
     *
     * @param destination formats: `topicName:tags`
     * @param message     {@link org.springframework.messaging.Message}
     * @param hashKey     use this key to select queue. for example: orderId, productId ...
     */
    public void sendOneWayOrderly(String destination, Message<?> message, String hashKey) {
        if (Objects.isNull(message) || Objects.isNull(message.getPayload())) {
            log.error("sendOneWayOrderly failed. destination:{}, message is null ", destination);
            throw new IllegalArgumentException("`message` and `message.payload` cannot be null");
        }
        CatTranscation transaction = new CatTranscation(RocketMQConstant.CAT_TRANSACTION_PRODUCE_TYPE, destination);
        try {
            org.apache.rocketmq.common.message.Message rocketMsg = RocketMQUtil.convertToRocketMessage(objectMapper,
                    charset, destination, message);
            producer.sendOneway(rocketMsg, messageQueueSelector, hashKey);
        } catch (Exception e) {
            transaction.setStatus(e);
            log.error("sendOneWayOrderly failed. destination:{}, message:{}", destination, message);
            throw new MessagingException(e.getMessage(), e);
        } finally {
            transaction.complete();
        }
    }

    /**
     * Same to {@link #sendOneWayOrderly(String, Message, String)}
     *
     * @param destination formats: `topicName:tags`
     * @param payload     the Object to use as payload
     */
    public void sendOneWayOrderly(String destination, Object payload, String hashKey) {
        Message<?> message = this.doConvert(payload, null, null);
        sendOneWayOrderly(destination, message, hashKey);
    }

    @Override
    public void afterPropertiesSet() throws Exception {
        Assert.notNull(producer, "Property 'producer' is required");
        producer.start();
    }

    @Override
    protected void doSend(String destination, Message<?> message) {
        SendResult sendResult = syncSend(destination, message);
        log.debug("send message to `{}` finished. result:{}", destination, sendResult);
    }


    @Override
    protected Message<?> doConvert(Object payload, Map<String, Object> headers, MessagePostProcessor postProcessor) {
        String content;
        if (payload instanceof String) {
            content = (String) payload;
        } else {
            // If payload not as string, use objectMapper change it.
            try {
                content = objectMapper.writeValueAsString(payload);
            } catch (JsonProcessingException e) {
                log.error("convert payload to String failed. payload:{}", payload);
                throw new RuntimeException("convert to payload to String failed.", e);
            }
        }

        MessageBuilder<?> builder = MessageBuilder.withPayload(content);
        if (headers != null) {
            builder.copyHeaders(headers);
        }
        builder.setHeaderIfAbsent(MessageHeaders.CONTENT_TYPE, MimeTypeUtils.TEXT_PLAIN);

        Message<?> message = builder.build();
        if (postProcessor != null) {
            message = postProcessor.postProcessMessage(message);
        }
        return message;
    }

    @Override
    public void destroy() {
        if (Objects.nonNull(producer)) {
            producer.shutdown();
        }
    }
}
