package com.github.wenweihu86.distmq.client.consumer;

import com.github.wenweihu86.distmq.client.BrokerClient;
import com.github.wenweihu86.distmq.client.BrokerClientManager;
import com.github.wenweihu86.distmq.client.api.BrokerMessage;
import com.github.wenweihu86.distmq.client.utils.JsonUtil;
import com.github.wenweihu86.distmq.client.zk.ZKClient;
import com.github.wenweihu86.distmq.client.zk.ZKData;
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
    private ZKClient zkClient;
    private ScheduledExecutorService timer = Executors.newScheduledThreadPool(1);
    private MessageListener listener;

    public Consumer(ConsumerConfig config, MessageListener listener) {
        this.config = config;
        this.listener = listener;
        BrokerClientManager.setRpcClientOptions(this.config.getRPCClientOptions());
        zkClient = new ZKClient(config);
        zkClient.registerConsumer(config.getConsumerGroup(), config.getConsumerId());
        zkClient.subscribeConsumer(config.getConsumerGroup());
        zkClient.subscribeBroker();
        zkClient.subscribeTopic();
        // 更新offset
        Map<Integer, Long> queueOffsetMap = zkClient.readConsumerOffset(config.getConsumerGroup(), config.getTopic());
        ZKData zkData = ZKData.getInstance();
        Map<String, Map<Integer, Long>> topicOffsetMap
                = zkData.getConsumerOffsetMap().get(config.getConsumerGroup());
        if (topicOffsetMap == null) {
            topicOffsetMap = new HashMap<>();
            topicOffsetMap.put(config.getTopic(), queueOffsetMap);
            zkData.getConsumerOffsetMap().put(config.getConsumerGroup(), topicOffsetMap);
        } else {
            Map<Integer, Long> oldQueueOffsetMap = topicOffsetMap.get(config.getTopic());
            if (oldQueueOffsetMap == null) {
                topicOffsetMap.put(config.getTopic(), queueOffsetMap);
            } else {
                oldQueueOffsetMap.putAll(queueOffsetMap);
            }
        }
        LOG.info("new consumer offset={}", JsonUtil.toJson(zkData.getConsumerOffsetMap()));
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
            long offset;
            // 获取offset
            ZKData zkData = ZKData.getInstance();
            zkData.getConsumerOffsetLock().lock();
            try {
                offset = zkData.getConsumerOffsetMap()
                        .get(config.getConsumerGroup())
                        .get(config.getTopic())
                        .get(queueId);
            } catch (Exception ex) {
                offset = 0;
            } finally {
                zkData.getConsumerOffsetLock().unlock();
            }

            BrokerMessage.PullMessageRequest request = BrokerMessage.PullMessageRequest.newBuilder()
                    .setTopic(config.getTopic())
                    .setQueue(queueId)
                    .setMessageCount(config.getMaxMessageCountPerRequest())
                    .setOffset(offset)
                    .build();

            List<String> brokers;
            zkData.getBrokerLock().lock();
            try {
                brokers = zkData.getBrokerMap().get(shardingId);
            } finally {
                zkData.getBrokerLock().unlock();
            }

            int randIndex = ThreadLocalRandom.current().nextInt(0, brokers.size());
            ConcurrentMap<String, BrokerClient> brokerClientMap
                    = BrokerClientManager.getInstance().getBrokerClientMap();
            BrokerClient brokerClient = brokerClientMap.get(brokers.get(randIndex));

            BrokerMessage.PullMessageResponse response = brokerClient.getBrokerAPI().pullMessage(request);
            if (response == null || response.getBaseRes().getResCode() != BrokerMessage.ResCode.RES_CODE_SUCCESS) {
                LOG.warn("pullMessage failed, topic={}, queue={}, offset={}, broker={}",
                        request.getTopic(), request.getQueue(), request.getOffset(),
                        brokers.get(randIndex));
            } else {
                LOG.info("pullMessage success, topic={}, queue={}, offset={}, size={}",
                        request.getTopic(), request.getQueue(), request.getOffset(),
                        response.getContentsCount());
                if (response.getContentsCount() > 0) {
                    listener.consumeMessage(response.getContentsList());
                    BrokerMessage.MessageContent lastMessage = response.getContents(response.getContentsCount() - 1);
                    offset = lastMessage.getOffset() + lastMessage.getSize();
                    zkClient.updateConsumerOffset(config.getConsumerGroup(), config.getTopic(), queueId, offset);
                    // 更新offset
                    zkData.getConsumerOffsetLock().lock();
                    try {
                        Map<String, Map<Integer, Long>> topicOffsetMap
                                = zkData.getConsumerOffsetMap().get(config.getConsumerGroup());
                        if (topicOffsetMap == null) {
                            Map<Integer, Long> queueOffsetMap = new HashMap<>();
                            queueOffsetMap.put(queueId, offset);
                            topicOffsetMap = new HashMap<>();
                            topicOffsetMap.put(config.getTopic(), queueOffsetMap);
                            zkData.getConsumerOffsetMap().put(config.getConsumerGroup(), topicOffsetMap);
                        } else {
                            Map<Integer, Long> queueOffsetMap = topicOffsetMap.get(config.getTopic());
                            if (queueOffsetMap == null) {
                                queueOffsetMap = new HashMap<>();
                                queueOffsetMap.put(queueId, offset);
                                topicOffsetMap.put(config.getTopic(), queueOffsetMap);
                            } else {
                                queueOffsetMap.put(queueId, offset);
                            }
                        }
                        LOG.debug("new consumer offset={}", JsonUtil.toJson(zkData.getConsumerOffsetMap()));
                    } finally {
                        zkData.getConsumerOffsetLock().unlock();
                    }
                }
            }
        }
    }

    /**
     * 获取分配给本节点的queue，以及对应的broker sharding id
     * @return key是queueId，value是shardingId
     */
    private Map<Integer, Integer> getConsumedQueue() {
        ZKData zkData = ZKData.getInstance();
        // 获取所有queueMap
        Map<Integer, Integer> queueMap = new HashMap<>();
        zkData.getTopicLock().lock();
        try {
            if (zkData.getTopicMap().containsKey(config.getTopic())) {
                queueMap.putAll(zkData.getTopicMap().get(config.getTopic()));
            }
        } finally {
            zkData.getTopicLock().unlock();
        }
        Integer[] queueIds = queueMap.keySet().toArray(new Integer[0]);
        Arrays.sort(queueIds);

        // 获取所有consumer list
        List<String> consumerIdList = new ArrayList<>();
        zkData.getConsumerIdsLock().lock();
        try {
            consumerIdList.addAll(zkData.getConsumerIds());
        } finally {
            zkData.getConsumerIdsLock().unlock();
        }

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
