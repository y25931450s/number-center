package com.xiaoying.common.mq.constant;

/**
 * @author:lijin,E-mail:jin.li@quvideo.com>
 * @created:2019/1/4
 * @function: 常量
 */
public class RocketMQConstant {
    /**
     * MQ 生产者 Transaction Type
     */
    public static final String CAT_TRANSACTION_PRODUCE_TYPE = "MQ.Produce";

    /**
     * MQ 批量生产消息
     */
    public static final String CAT_TRANSACTION_PRODUCE_BATCH_TYPE = "MQ.Produce.Batch";

    /**
     * MQ 消费者 Transaction Type
     */
    public static final String CAT_TRANSACTION_CONSUMER_TYPE = "MQ.Consumer";

    /**
     * 默认消费最大重试次数,无限次数
     */
    public static final int DEFAULT_CONSUMER_MAX_RETRY_TIMES = -1;


    /**
     * MQ使用序列化对象Bean名字
     */
    public static final String ROCKET_MQOBJECT_MAPPER_BEANNAME = "rocketMQObjectMapper";
}
