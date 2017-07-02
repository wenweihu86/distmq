package com.github.wenweihu86.distmq.client.producer;

import com.github.wenweihu86.distmq.client.BrokerClient;
import com.github.wenweihu86.distmq.client.api.BrokerMessage;
import com.github.wenweihu86.distmq.client.zk.MetadataManager;
import com.google.protobuf.ByteString;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.concurrent.ThreadLocalRandom;

/**
 * Created by wenweihu86 on 2017/6/24.
 */
public class Producer {
    private static final Logger LOG = LoggerFactory.getLogger(Producer.class);
    private ProducerConfig config;
    private MetadataManager metadataManager;

    public Producer(ProducerConfig config) {
        this.config = config;
        metadataManager = new MetadataManager(config);
        metadataManager.subscribeBroker();
        metadataManager.subscribeTopic();
    }

    public boolean send(String topic, byte[] messageBytes) {
        // 如果topic尚不存在，则创建
        boolean topicExist = metadataManager.checkTopicExist(topic);
        if (!topicExist) {
            metadataManager.registerTopic(topic, config.getQueueCountPerTopic());
            Map<Integer, Integer> queueMap = metadataManager.readTopicInfo(topic);
            if (queueMap.size() != config.getQueueCountPerTopic()) {
                LOG.warn("create topic failed, topic={}", topic);
                return false;
            }
            metadataManager.updateTopicMap(topic, queueMap);
        }

        // 获取topic的queueId和对应的shardingId
        Map<Integer, Integer> queueMap = metadataManager.getTopicQueueMap(topic);
        int queueCount = queueMap.size();
        int randomIndex = ThreadLocalRandom.current().nextInt(0, queueCount);
        Integer[] queueArray = queueMap.keySet().toArray(new Integer[0]);
        Integer queueId = queueArray[randomIndex];
        Integer shardingId = queueMap.get(queueId);

        // send message to broker
        BrokerClient brokerClient = metadataManager.getBrokerClient(shardingId);
        BrokerMessage.SendMessageRequest request = BrokerMessage.SendMessageRequest.newBuilder()
                .setTopic(topic)
                .setQueue(queueId)
                .setContent(ByteString.copyFrom(messageBytes))
                .build();
        BrokerMessage.SendMessageResponse response = brokerClient.getBrokerAPI().sendMessage(request);
        if (response == null || response.getBaseRes().getResCode() != BrokerMessage.ResCode.RES_CODE_SUCCESS) {
            LOG.warn("send message failed, topic={}, queue={}, shardingId={}",
                    topic, queueId, shardingId);
            return false;
        }
        return true;
    }

    public ProducerConfig getConfig() {
        return config;
    }

    public void setConfig(ProducerConfig config) {
        this.config = config;
    }
}
