package com.github.wenweihu86.distmq.client.consumer;

import com.github.wenweihu86.distmq.client.api.BrokerMessage;

import java.util.List;

/**
 * Created by wenweihu86 on 2017/6/24.
 */
public interface MessageListener {

    void consumeMessage(List<BrokerMessage.MessageContent> messages);
}
