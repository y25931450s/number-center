package com.xiaoying.common.mq.xml;

import lombok.Data;

/**
 * @Author: misskey
 * @Date: 2019-02-28
 * @Version 1.0
 */
@Data
public class TestMqReceviers{

    private volatile  com.xiaoying.common.mq.xml.Message message;

    public void onMessage(Message message) {
        System.out.println(message);
        this.message = message;
    }



}
