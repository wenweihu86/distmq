package com.github.wenweihu86.distmq.client.producer;

import com.github.wenweihu86.distmq.client.CommonConfig;

/**
 * Created by wenweihu86 on 2017/6/24.
 */
public class ProducerConfig extends CommonConfig {
    private int queueCountPerTopic = 4;

    public int getQueueCountPerTopic() {
        return queueCountPerTopic;
    }

    public void setQueueCountPerTopic(int queueCountPerTopic) {
        this.queueCountPerTopic = queueCountPerTopic;
    }
}
