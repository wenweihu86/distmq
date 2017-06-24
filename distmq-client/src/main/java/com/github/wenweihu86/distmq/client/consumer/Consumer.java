package com.github.wenweihu86.distmq.client.consumer;

import com.github.wenweihu86.distmq.client.BrokerClient;
import com.github.wenweihu86.distmq.client.BrokerClientManager;
import com.github.wenweihu86.distmq.client.api.BrokerMessage;
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
    private long offset;

    public Consumer(ConsumerConfig config, MessageListener listener) {
        this.config = config;
        this.listener = listener;
        BrokerClientManager.setRpcClientOptions(this.config.getRPCClientOptions());
        zkClient = new ZKClient(config);
        zkClient.registerConsumer(config.getConsumerGroup(), config.getConsumerId());
        zkClient.subscribeConsumer(config.getConsumerGroup());
        zkClient.subscribeBroker();
        zkClient.subscribeTopic();
        this.offset = zkClient.readConsumerOffset(config.getConsumerGroup(), config.getTopic());
        this.timer.scheduleAtFixedRate(this, 1000, 5000, TimeUnit.MILLISECONDS);
    }

    @Override
    public void run() {
        Map<Integer, Integer> queueMap = getConsumedQueue();
        for (Map.Entry<Integer, Integer> entry : queueMap.entrySet()) {
            Integer queueId = entry.getKey();
            Integer shardingId = entry.getValue();
            BrokerMessage.PullMessageRequest request = BrokerMessage.PullMessageRequest.newBuilder()
                    .setTopic(config.getTopic())
                    .setQueue(queueId)
                    .setMessageCount(config.getMaxMessageCountPerRequest())
                    .setOffset(offset)
                    .build();

            List<String> brokers;
            ZKData zkData = ZKData.getInstance();
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
                    offset = lastMessage.getOffset() + 1;
                    zkClient.updateConsumerOffset(config.getConsumerGroup(), config.getTopic(), offset);
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
