package com.xiaoying.common.mq.config;

import com.ctrip.framework.apollo.spring.annotation.EnableApolloConfig;
import org.springframework.boot.context.properties.ConfigurationProperties;

/**
 * @author:lijin,E-mail:jin.li@quvideo.com>
 * @created:2019/1/4
 * @function: 生产者相关配置类
 */
@EnableApolloConfig
@ConfigurationProperties(prefix = "rocketmq")
public class RocketMQProducerProperties {
    /**
     * Name of producer.
     */
    private String group;

    /**
     * Millis of send message timeout.
     */
    private int sendMessageTimeout = 3000;

    /**
     * Compress message body threshold, namely, message body larger than 4k will be compressed on default.
     */
    private int compressMessageBodyThreshold = 1024 * 4;

    /**
     * Maximum number of retry to perform internally before claiming sending failure in synchronous mode.
     * This may potentially cause message duplication which is up to application developers to resolve.
     */
    private int retryTimesWhenSendFailed = 2;

    /**
     * <p> Maximum number of retry to perform internally before claiming sending failure in asynchronous mode. </p>
     * This may potentially cause message duplication which is up to application developers to resolve.
     */
    private int retryTimesWhenSendAsyncFailed = 2;

    /**
     * 当一台broker发送失败时，重试另外的broker（类似自定转移），避免业务抖动，默认开启该功能
     */
    private boolean retryNextServer = true;

    /**
     * 是否开启MQ Trace（暂时不开启）
     */
    private boolean enableTrace;
    /**
     * Maximum allowed message size in bytes.
     */
    private int maxMessageSize = 1024 * 1024 * 4;

    public String getGroup() {
        return group;
    }

    public void setGroup(String group) {
        this.group = group;
    }

    public int getSendMessageTimeout() {
        return sendMessageTimeout;
    }

    public void setSendMessageTimeout(int sendMessageTimeout) {
        this.sendMessageTimeout = sendMessageTimeout;
    }

    public int getCompressMessageBodyThreshold() {
        return compressMessageBodyThreshold;
    }

    public void setCompressMessageBodyThreshold(int compressMessageBodyThreshold) {
        this.compressMessageBodyThreshold = compressMessageBodyThreshold;
    }

    public int getRetryTimesWhenSendFailed() {
        return retryTimesWhenSendFailed;
    }

    public void setRetryTimesWhenSendFailed(int retryTimesWhenSendFailed) {
        this.retryTimesWhenSendFailed = retryTimesWhenSendFailed;
    }

    public int getRetryTimesWhenSendAsyncFailed() {
        return retryTimesWhenSendAsyncFailed;
    }

    public void setRetryTimesWhenSendAsyncFailed(int retryTimesWhenSendAsyncFailed) {
        this.retryTimesWhenSendAsyncFailed = retryTimesWhenSendAsyncFailed;
    }

    public boolean isRetryNextServer() {
        return retryNextServer;
    }

    public void setRetryNextServer(boolean retryNextServer) {
        this.retryNextServer = retryNextServer;
    }

    public int getMaxMessageSize() {
        return maxMessageSize;
    }

    public void setMaxMessageSize(int maxMessageSize) {
        this.maxMessageSize = maxMessageSize;
    }

    public boolean isEnableTrace() {
        return enableTrace;
    }

    public void setEnableTrace(boolean enableTrace) {
        this.enableTrace = enableTrace;
    }
}
