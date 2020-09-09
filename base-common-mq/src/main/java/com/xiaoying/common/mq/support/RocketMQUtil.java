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

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.common.message.Message;
import org.apache.rocketmq.common.message.MessageExt;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.messaging.MessageHeaders;
import org.springframework.messaging.MessagingException;
import org.springframework.messaging.support.MessageBuilder;
import org.springframework.util.StringUtils;

import java.nio.charset.Charset;
import java.util.Objects;

public class RocketMQUtil {
    private final static Logger log = LoggerFactory.getLogger(RocketMQUtil.class);

    public static MessagingException convert(MQClientException e) {
        return new MessagingException(e.getErrorMessage(), e);
    }

    public static org.springframework.messaging.Message convertToSpringMessage(
        MessageExt message) {
        org.springframework.messaging.Message retMessage =
            MessageBuilder.withPayload(message.getBody()).
                setHeader(RocketMQHeaders.KEYS, message.getKeys()).
                setHeader(RocketMQHeaders.TAGS, message.getTags()).
                setHeader(RocketMQHeaders.TOPIC, message.getTopic()).
                setHeader(RocketMQHeaders.MESSAGE_ID, message.getMsgId()).
                setHeader(RocketMQHeaders.BORN_TIMESTAMP, message.getBornTimestamp()).
                setHeader(RocketMQHeaders.BORN_HOST, message.getBornHostString()).
                setHeader(RocketMQHeaders.FLAG, message.getFlag()).
                setHeader(RocketMQHeaders.QUEUE_ID, message.getQueueId()).
                setHeader(RocketMQHeaders.SYS_FLAG, message.getSysFlag()).
                setHeader(RocketMQHeaders.TRANSACTION_ID, message.getTransactionId()).
                setHeader(RocketMQHeaders.PROPERTIES, message.getProperties()).
                build();

        return retMessage;
    }

    public static org.springframework.messaging.Message convertToSpringMessage (
        Message message) {
        org.springframework.messaging.Message retMessage =
            MessageBuilder.withPayload(message.getBody()).
                setHeader(RocketMQHeaders.KEYS, message.getKeys()).
                setHeader(RocketMQHeaders.TAGS, message.getTags()).
                setHeader(RocketMQHeaders.TOPIC, message.getTopic()).
                setHeader(RocketMQHeaders.FLAG, message.getFlag()).
                setHeader(RocketMQHeaders.TRANSACTION_ID, message.getTransactionId()).
                setHeader(RocketMQHeaders.PROPERTIES, message.getProperties()).
                build();

        return retMessage;
    }

    public static Message convertToRocketMessage(
        ObjectMapper objectMapper, String charset,
        String destination, org.springframework.messaging.Message<?> message) {
        Object payloadObj = message.getPayload();
        byte[] payloads;

        if (payloadObj instanceof String) {
            payloads = ((String) payloadObj).getBytes(Charset.forName(charset));
        } else {
            try {
                String jsonObj = objectMapper.writeValueAsString(payloadObj);
                payloads = jsonObj.getBytes(Charset.forName(charset));
            } catch (Exception e) {
                throw new RuntimeException("convert to RocketMQ message failed.", e);
            }
        }

        String[] tempArr = destination.split(":", 2);
        String topic = tempArr[0];
        String tags = "";
        if (tempArr.length > 1) {
            tags = tempArr[1];
        }

        Message rocketMsg = new Message(topic, tags, payloads);

        MessageHeaders headers = message.getHeaders();
        if (Objects.nonNull(headers) && !headers.isEmpty()) {
            Object keys = headers.get(RocketMQHeaders.KEYS);
            if (!StringUtils.isEmpty(keys)) { // if headers has 'KEYS', set rocketMQ message key
                rocketMsg.setKeys(keys.toString());
            }

            Object flagObj = headers.getOrDefault("FLAG", "0");
            int flag = 0;
            try {
                flag = Integer.parseInt(flagObj.toString());
            } catch (NumberFormatException e) {
                // Ignore it
                log.info("flag must be integer, flagObj:{}", flagObj);
            }
            rocketMsg.setFlag(flag);

            Object waitStoreMsgOkObj = headers.getOrDefault("WAIT_STORE_MSG_OK", "true");
            boolean waitStoreMsgOK = Boolean.TRUE.equals(waitStoreMsgOkObj);
            rocketMsg.setWaitStoreMsgOK(waitStoreMsgOK);

            headers.entrySet().stream()
                .filter(entry -> !Objects.equals(entry.getKey(), RocketMQHeaders.KEYS)
                    && !Objects.equals(entry.getKey(), "FLAG")
                    && !Objects.equals(entry.getKey(), "WAIT_STORE_MSG_OK")) // exclude "KEYS", "FLAG", "WAIT_STORE_MSG_OK"
                .forEach(entry -> {
                    rocketMsg.putUserProperty("USERS_" + entry.getKey(), String.valueOf(entry.getValue())); // add other properties with prefix "USERS_"
                });

        }

        return rocketMsg;
    }
}
