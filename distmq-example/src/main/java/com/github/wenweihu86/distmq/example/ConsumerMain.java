package com.github.wenweihu86.distmq.example;

import com.github.wenweihu86.distmq.client.api.BrokerMessage;
import com.github.wenweihu86.distmq.client.consumer.Consumer;
import com.github.wenweihu86.distmq.client.consumer.ConsumerConfig;
import com.github.wenweihu86.distmq.client.consumer.MessageListener;

import java.util.List;

/**
 * Created by wenweihu86 on 2017/6/25.
 */
public class ConsumerMain {
    public static void main(String[] args) {
        final ConsumerConfig config = new ConsumerConfig();
        config.setServers("127.0.0.1:2181,127.0.0.1:2182,127.0.0.1:2183");
        config.setConsumerGroup("example-consumer-group");
        config.setTopic("example-topic");
        Consumer consumer = new Consumer(config, new MessageListener() {
            @Override
            public void consumeMessage(List<BrokerMessage.MessageContent> messages) {
                for (BrokerMessage.MessageContent message : messages) {
                    String content = new String(message.getContent().toByteArray());
                    String topic = message.getTopic();
                    int queue = message.getQueue();
                    long offset = message.getOffset();
                    System.out.printf("topic=%s, queue=%d, offset=%d, message=%s\n",
                            topic, queue, offset, content);
                }
            }
        });
        consumer.start();
    }
}
