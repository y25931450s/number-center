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

package com.xiaoying.common.mq.annotation;

import com.xiaoying.common.mq.constant.ConsumeMode;
import com.xiaoying.common.mq.constant.MessageModel;
import com.xiaoying.common.mq.constant.RocketMQConstant;
import com.xiaoying.common.mq.constant.SelectorType;

import java.lang.annotation.*;

/**
 * @author yeyang
 * @author lijin
 */
@Target(ElementType.TYPE)
@Retention(RetentionPolicy.RUNTIME)
@Documented
public @interface RocketMQMessageListener {

    /**
     * Consumers of the same role is required to have exactly same subscriptions and consumerGroup to correctly achieve
     * load balance. It's required and needs to be globally unique.
     * <p>
     * <p>
     * See <a href="http://rocketmq.apache.org/docs/core-concept/">here</a> for further discussion.
     */
    String consumerGroup();

    /**
     * 最大重试次数
     */
    int maxRetryTimes() default RocketMQConstant.DEFAULT_CONSUMER_MAX_RETRY_TIMES;

    /**
     * Topic name.
     */
    String topic();

    /**
     * Control how to selector message.
     *
     * @see SelectorType
     */
    SelectorType selectorType() default SelectorType.TAG;

    /**
     * Control which message can be select. Grammar please see {@link SelectorType#TAG} and {@link SelectorType#SQL92}
     */
    String selectorExpression() default "*";

    /**
     * Control consume mode, you can choice receive message concurrently or orderly.
     */
    ConsumeMode consumeMode() default ConsumeMode.CONCURRENTLY;

    /**
     * Control message mode, if you want all subscribers receive message all message, broadcasting is a good choice.
     */
    MessageModel messageModel() default MessageModel.CLUSTERING;

    /**
     * Max consumer thread number.
     */
    int consumeThreadMax() default 64;


    /**
     * 用于控制重试等级，-1直接放入死信topic，0 broker重试,大于0 客户端控制消费（发回broker后本地再过5秒重试消费一次，如果这次成功，下次就不再消费）
     * see <a href="http://tech.dianwoda.com/2018/02/09/rocketmq-reconsume/">consumer failed</a>
     *
     * @return
     */
    int delayLevel() default 0;


    /**
     * 是否开启消息追踪
     * @return
     */
    boolean enableTrace() default false;

}
