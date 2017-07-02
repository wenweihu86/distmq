package com.github.wenweihu86.distmq.example;

import com.github.wenweihu86.distmq.client.producer.Producer;
import com.github.wenweihu86.distmq.client.producer.ProducerConfig;

import java.util.UUID;

/**
 * Created by wenweihu86 on 2017/6/25.
 */
public class ProducerMain {

    public static void main(String[] args) {
        ProducerConfig config = new ProducerConfig();
        config.setZKServers("127.0.0.1:2181,127.0.0.1:2182,127.0.0.1:2183");
        Producer producer = new Producer(config);
        String topic = "example-topic";
        while (true) {
            String message = UUID.randomUUID().toString();
            boolean success = producer.send(topic, message.getBytes());
            if (success) {
                System.out.printf("send message success, topic=%s, message=%s\n",
                        topic, message);
            }
        }
    }

}
