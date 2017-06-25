package com.github.wenweihu86.distmq.example;

import com.github.wenweihu86.distmq.client.producer.Producer;
import com.github.wenweihu86.distmq.client.producer.ProducerConfig;

/**
 * Created by wenweihu86 on 2017/6/25.
 */
public class ProducerMain {

    public static void main(String[] args) {
        ProducerConfig config = new ProducerConfig();
        config.setServers("127.0.0.1:2181,127.0.0.1:2182,127.0.0.1:2183");
        Producer producer = new Producer(config);
        String topic = "example-topic";
        for (int i = 0; i < 100; i++) {
            String message = "hello-distmq-" + i;
            boolean success = producer.send(topic, message.getBytes());
            if (success) {
                System.out.printf("send message success, topic=%s, message=%s\n",
                        topic, message);
            }
        }
    }

}
