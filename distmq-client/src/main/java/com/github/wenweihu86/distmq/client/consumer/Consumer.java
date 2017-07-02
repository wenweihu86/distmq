package com.github.wenweihu86.distmq.client.consumer;

import com.github.wenweihu86.distmq.client.BrokerClient;
import com.github.wenweihu86.distmq.client.api.BrokerMessage;
import com.github.wenweihu86.distmq.client.zk.MetadataManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.concurrent.*;

/**
 * Created by wenweihu86 on 2017/6/24.
 */
public class Consumer implements Runnable {
    private static final Logger LOG = LoggerFactory.getLogger(Consumer.class);
    private ConsumerConfig config;
    private MetadataManager metadataManager;
    private ScheduledExecutorService timer = Executors.newScheduledThreadPool(1);
    private MessageListener listener;

    public Consumer(ConsumerConfig config, MessageListener listener) {
        this.config = config;
        this.listener = listener;
        metadataManager = new MetadataManager(config);
        metadataManager.registerConsumer(config.getConsumerGroup(), config.getConsumerId());
        metadataManager.updateConsumerIds(config.getConsumerGroup());
        metadataManager.subscribeConsumer(config.getConsumerGroup());
        metadataManager.subscribeBroker();
        metadataManager.subscribeTopic();
        // 从zk读取offset
        metadataManager.readConsumerOffset(config.getConsumerGroup(), config.getTopic());
    }

    public void start() {
        timer.scheduleAtFixedRate(this, 1000, 5000, TimeUnit.MILLISECONDS);
    }

    @Override
    public void run() {
        Map<Integer, Integer> queueMap = getConsumedQueue();
        for (Map.Entry<Integer, Integer> entry : queueMap.entrySet()) {
            Integer queueId = entry.getKey();
            Integer shardingId = entry.getValue();
            long offset = metadataManager.getConsumerOffset(queueId);
            BrokerClient brokerClient = metadataManager.getBrokerClient(shardingId);

            BrokerMessage.PullMessageRequest request = BrokerMessage.PullMessageRequest.newBuilder()
                    .setTopic(config.getTopic())
                    .setQueue(queueId)
                    .setMessageCount(config.getMaxMessageCountPerRequest())
                    .setOffset(offset)
                    .build();
            BrokerMessage.PullMessageResponse response = brokerClient.getBrokerAPI().pullMessage(request);
            if (response == null || response.getBaseRes().getResCode() != BrokerMessage.ResCode.RES_CODE_SUCCESS) {
                LOG.warn("pullMessage failed, topic={}, queue={}, offset={}, shardingId={}",
                        request.getTopic(), request.getQueue(), request.getOffset(), shardingId);
            } else {
                LOG.info("pullMessage success, topic={}, queue={}, offset={}, size={}",
                        request.getTopic(), request.getQueue(), request.getOffset(),
                        response.getContentsCount());
                if (response.getContentsCount() > 0) {
                    listener.consumeMessage(response.getContentsList());
                    BrokerMessage.MessageContent lastMessage = response.getContents(response.getContentsCount() - 1);
                    offset = lastMessage.getOffset() + lastMessage.getSize();
                    metadataManager.updateConsumerOffset(
                            config.getConsumerGroup(), config.getTopic(), queueId, offset);
                }
            }
        }
    }

    /**
     * 获取分配给本节点的queue，以及对应的broker sharding id
     * @return key是queueId，value是shardingId
     */
    private Map<Integer, Integer> getConsumedQueue() {
        // 获取所有queueMap
        Map<Integer, Integer> queueMap = metadataManager.getTopicQueueMap(config.getTopic());
        Integer[] queueIds = queueMap.keySet().toArray(new Integer[0]);
        Arrays.sort(queueIds);
        // 获取所有consumer list
        List<String> consumerIdList = metadataManager.getConsumerIds();

        int queueSize = queueMap.size();
        int consumerSize = consumerIdList.size();
        int index = consumerIdList.indexOf(config.getConsumerId());
        int mod = queueSize % consumerSize;

        int averageSize;
        if (queueSize <= consumerSize) {
            averageSize = 1;
        } else {
            if (mod > 0 && index < mod) {
                averageSize = queueSize / consumerSize + 1;
            } else {
                averageSize = queueSize / consumerSize;
            }
        }

        int startIndex;
        if (mod > 0 && index < mod) {
            startIndex = index * averageSize;
        } else {
            startIndex = index * averageSize + mod;
        }

        Map<Integer, Integer> result = new HashMap<>();
        for (int i = startIndex; i < startIndex + averageSize && i < queueSize; i++) {
            result.put(queueIds[i], queueMap.get(queueIds[i]));
        }
        return result;
    }
}
