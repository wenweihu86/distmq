package com.github.wenweihu86.distmq.client.producer;

import com.github.wenweihu86.distmq.client.BrokerClient;
import com.github.wenweihu86.distmq.client.BrokerClientManager;
import com.github.wenweihu86.distmq.client.api.BrokerMessage;
import com.github.wenweihu86.distmq.client.zk.ZKClient;
import com.github.wenweihu86.distmq.client.zk.ZKData;
import com.google.protobuf.ByteString;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Map;
import java.util.concurrent.ThreadLocalRandom;

/**
 * Created by wenweihu86 on 2017/6/24.
 */
public class Producer {
    private static final Logger LOG = LoggerFactory.getLogger(Producer.class);
    private ProducerConfig config;
    private ZKClient zkClient;

    public Producer(ProducerConfig config) {
        this.config = config;
        BrokerClientManager.setRpcClientOptions(this.config.getRPCClientOptions());
        zkClient = new ZKClient(config);
        zkClient.subscribeBroker();
        zkClient.subscribeTopic();
    }

    public boolean send(String topic, byte[] messageBytes) {
        ZKData zkData = ZKData.getInstance();
        Map<Integer, Integer> queueMap;
        zkData.getTopicLock().lock();
        try {
            queueMap = zkData.getTopicMap().get(topic);
        } finally {
            zkData.getTopicLock().unlock();
        }
        Integer queueId;
        Integer shardingId;
        if (queueMap == null) {
            zkClient.registerTopic(topic, config.getQueueCountPerTopic());
            zkData.getTopicLock().lock();
            try {
                while (!zkData.getTopicMap().containsKey(topic)) {
                    zkData.getTopicCondition().awaitUninterruptibly();
                }
            } finally {
                zkData.getTopicLock().unlock();
            }
        }

        zkData.getTopicLock().lock();
        try {
            queueMap = zkData.getTopicMap().get(topic);
            int queueCount = queueMap.size();
            int randomIndex = ThreadLocalRandom.current().nextInt(0, queueCount);
            Integer[] queueArray = queueMap.keySet().toArray(new Integer[0]);
            queueId = queueArray[randomIndex];
            shardingId = queueMap.get(queueId);
        } finally {
            zkData.getTopicLock().unlock();
        }

        // send message to broker
        BrokerMessage.SendMessageRequest request = BrokerMessage.SendMessageRequest.newBuilder()
                .setTopic(topic)
                .setQueue(queueId)
                .setContent(ByteString.copyFrom(messageBytes))
                .build();

        List<String> brokerAddressList;
        zkData.getBrokerLock().lock();
        try {
            brokerAddressList = zkData.getBrokerMap().get(shardingId);
        } finally {
            zkData.getBrokerLock().unlock();
        }
        int randIndex = ThreadLocalRandom.current().nextInt(0, brokerAddressList.size());
        String brokerAddress = brokerAddressList.get(randIndex);
        BrokerClient brokerClient = BrokerClientManager.getInstance().getBrokerClientMap().get(brokerAddress);
        BrokerMessage.SendMessageResponse response = brokerClient.getBrokerAPI().sendMessage(request);
        if (response == null || response.getBaseRes().getResCode() != BrokerMessage.ResCode.RES_CODE_SUCCESS) {
            LOG.warn("send message failed, topic={}, queue={}, brokerAddress={}",
                    topic, queueId, brokerAddress);
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
