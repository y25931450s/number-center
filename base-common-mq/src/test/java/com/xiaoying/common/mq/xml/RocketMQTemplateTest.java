package com.xiaoying.common.mq.xml;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.xiaoying.common.mq.core.RocketMQTemplate;
import com.xiaoying.common.mq.support.DefaultRocketMQListenerContainer;
import org.apache.rocketmq.client.producer.DefaultMQProducer;
import org.apache.rocketmq.client.producer.SendResult;
import org.apache.rocketmq.client.producer.SendStatus;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.test.context.ContextConfiguration;

/**
 * @Author: misskey
 * @Date: 2019-02-28
 * @Version 1.0
 */
@ContextConfiguration(locations = "classpath:xml/test-rocketmqTemplate.xml")
public class RocketMQTemplateTest extends BaseTest {


    @Autowired
    private RocketMQTemplate rocketMQTemplate;
    @Autowired
    private TestMqReceviers testMqReceviers;
    @Autowired
    private DefaultRocketMQListenerContainer defaultRocketMQListenerContainer;


    @Before
    public void pre() {

        Message message = new Message();
        message.setInfo("test");
        SendResult sendResult = rocketMQTemplate.syncSend("junit-test:*", message);
        org.junit.Assert.assertEquals(sendResult.getSendStatus(), SendStatus.SEND_OK);
    }


    @Test
    public void test() {

        try {
            Thread.sleep(10000);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        applicationContext.getAutowireCapableBeanFactory().getBean(ObjectMapper.class.getSimpleName());
        applicationContext.getAutowireCapableBeanFactory().getBean(DefaultMQProducer.class.getSimpleName());
        Message message = testMqReceviers.getMessage();
        Assert.assertNotNull("message 不能为空", message);
        Assert.assertEquals("message  内容必须为test", message.getInfo(), "test");
    }

}
