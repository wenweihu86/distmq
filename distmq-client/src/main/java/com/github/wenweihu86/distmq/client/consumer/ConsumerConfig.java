package com.github.wenweihu86.distmq.client.consumer;

import com.github.wenweihu86.distmq.client.CommonConfig;

import java.util.UUID;

/**
 * Created by wenweihu86 on 2017/6/24.
 */
public class ConsumerConfig extends CommonConfig {
    private String consumerGroup;
    private String topic;
    private String consumerId = UUID.randomUUID().toString();
    private int maxMessageCountPerRequest = 50;

    public String getConsumerGroup() {
        return consumerGroup;
    }

    public void setConsumerGroup(String consumerGroup) {
        this.consumerGroup = consumerGroup;
    }

    public String getTopic() {
        return topic;
    }

    public void setTopic(String topic) {
        this.topic = topic;
    }

    public String getConsumerId() {
        return consumerId;
    }

    public void setConsumerId(String consumerId) {
        this.consumerId = consumerId;
    }

    public int getMaxMessageCountPerRequest() {
        return maxMessageCountPerRequest;
    }

    public void setMaxMessageCountPerRequest(int maxMessageCountPerRequest) {
        this.maxMessageCountPerRequest = maxMessageCountPerRequest;
    }
}
