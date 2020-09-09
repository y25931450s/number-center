package com.xiaoying.common.mq.xml;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.Data;

import java.io.IOException;

/**
 * @Author: misskey
 * @Date: 2019-03-06
 * @Version 1.0
 */
public class Test {

    @Data
    public static class Info {

        private String one;
    }


    public static void main(String[] args) {
        Info info = new Info();
        info.setOne("test");
        ObjectMapper objectMapper = new ObjectMapper();
        try {
            String jsonObj = objectMapper.writeValueAsString(info);
            System.out.println(jsonObj);
        } catch (JsonProcessingException e) {
            e.printStackTrace();
        }
        String json ="{\"one\":\"test\",\"test2\":1}";
        try {
            Info info1= objectMapper.readValue(json,Info.class);
            System.out.println(info1);
        } catch (IOException e) {
            e.printStackTrace();
        }

    }
}
