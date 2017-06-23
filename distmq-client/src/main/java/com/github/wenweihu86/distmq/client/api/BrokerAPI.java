package com.github.wenweihu86.distmq.client.api;

/**
 * Created by wenweihu86 on 2017/6/15.
 */
public interface BrokerAPI {

    BrokerMessage.SendMessageResponse sendMessage(BrokerMessage.SendMessageRequest request);

    BrokerMessage.PullMessageResponse pullMessage(BrokerMessage.PullMessageRequest request);
}
