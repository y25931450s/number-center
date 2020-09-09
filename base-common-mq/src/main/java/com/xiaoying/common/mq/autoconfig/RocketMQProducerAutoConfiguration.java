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

package com.xiaoying.common.mq.autoconfig;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.xiaoying.common.mq.config.RocketMQProducerProperties;
import com.xiaoying.common.mq.config.RocketMQProperties;
import com.xiaoying.common.mq.constant.RocketMQConstant;
import com.xiaoying.common.mq.core.RocketMQTemplate;
import org.apache.rocketmq.client.producer.DefaultMQProducer;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.autoconfigure.AutoConfigureAfter;
import org.springframework.boot.autoconfigure.condition.ConditionalOnBean;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Import;
import org.springframework.util.Assert;


@Import({RocketMQCommonConfiguration.class})
@AutoConfigureAfter(RocketMQCommonConfiguration.class)
public class RocketMQProducerAutoConfiguration {

    @Autowired
    private RocketMQProperties rocketMQProperties;

    @Autowired
    private RocketMQProducerProperties rocketMQProducerProperties;

    @Bean
    @ConditionalOnMissingBean(DefaultMQProducer.class)
    public DefaultMQProducer defaultMQProducer() {
        String nameServer = rocketMQProperties.getNameServer();
        String groupName = rocketMQProducerProperties.getGroup();
        Assert.hasText(nameServer, "[rocketmq.name-server] must not be null");
        Assert.hasText(groupName, "[rocketmq.producer.group] must not be null");

        DefaultMQProducer producer = new DefaultMQProducer(groupName,rocketMQProducerProperties.isEnableTrace());
        producer.setNamesrvAddr(nameServer);
        producer.setSendMsgTimeout(rocketMQProducerProperties.getSendMessageTimeout());
        producer.setRetryTimesWhenSendFailed(rocketMQProducerProperties.getRetryTimesWhenSendFailed());
        producer.setRetryTimesWhenSendAsyncFailed(rocketMQProducerProperties.getRetryTimesWhenSendAsyncFailed());
        producer.setMaxMessageSize(rocketMQProducerProperties.getMaxMessageSize());
        producer.setCompressMsgBodyOverHowmuch(rocketMQProducerProperties.getCompressMessageBodyThreshold());
        producer.setRetryAnotherBrokerWhenNotStoreOK(rocketMQProducerProperties.isRetryNextServer());
        return producer;
    }

    @Bean(destroyMethod = "destroy")
    @ConditionalOnBean(value = {DefaultMQProducer.class},name = {RocketMQConstant.ROCKET_MQOBJECT_MAPPER_BEANNAME})
    @ConditionalOnMissingBean(RocketMQTemplate.class)
    public RocketMQTemplate rocketMQTemplate(DefaultMQProducer mqProducer, ObjectMapper rocketMQMessageObjectMapper) {
        RocketMQTemplate rocketMQTemplate = new RocketMQTemplate();
        rocketMQTemplate.setProducer(mqProducer);
        rocketMQTemplate.setObjectMapper(rocketMQMessageObjectMapper);
        return rocketMQTemplate;
    }
}
