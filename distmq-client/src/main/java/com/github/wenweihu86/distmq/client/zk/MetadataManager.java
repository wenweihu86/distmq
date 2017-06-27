package com.github.wenweihu86.distmq.client.zk;

import com.github.wenweihu86.distmq.client.BrokerClient;
import com.github.wenweihu86.distmq.client.BrokerClientManager;
import com.github.wenweihu86.distmq.client.consumer.ConsumerConfig;
import com.github.wenweihu86.distmq.client.producer.ProducerConfig;
import com.github.wenweihu86.distmq.client.utils.JsonUtil;
import com.github.wenweihu86.rpc.client.RPCClientOptions;
import org.apache.commons.collections.CollectionUtils;
import org.apache.curator.RetryPolicy;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.framework.api.CuratorWatcher;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.List;
import java.util.concurrent.ConcurrentMap;

/**
 * Created by wenweihu86 on 2017/6/21.
 */
public class MetadataManager {
    private static final Logger LOG = LoggerFactory.getLogger(MetadataManager.class);
    private ZKConf zkConf;
    private CuratorFramework zkClient;
    private Metadata metadata;
    private boolean isProducer = false;
    private boolean isConsumer = false;

    public MetadataManager(ZKConf conf) {
        this.zkConf = conf;
        if (conf instanceof ProducerConfig) {
            isProducer = true;
        } else if (conf instanceof ConsumerConfig) {
            isConsumer = true;
        }
        this.metadata = new Metadata();

        RetryPolicy retryPolicy = new ExponentialBackoffRetry(
                zkConf.getRetryIntervalMs(), zkConf.getRetryCount());
        this.zkClient = CuratorFrameworkFactory.builder()
                .connectString(zkConf.getServers())
                .retryPolicy(retryPolicy)
                .connectionTimeoutMs(zkConf.getConnectTimeoutMs())
                .sessionTimeoutMs(zkConf.getSessionTimeoutMs())
                .build();
        this.zkClient.start();

        // create path
        String brokersPath = zkConf.getBasePath() + "/brokers";
        createPath(brokersPath, CreateMode.PERSISTENT);
        String topicsPath = zkConf.getBasePath() + "/topics";
        createPath(topicsPath, CreateMode.PERSISTENT);
        if (isConsumer) {
            ConsumerConfig consumerConfig = (ConsumerConfig) conf;
            String consumerIdsPath = zkConf.getBasePath() + "/consumers/" + consumerConfig.getConsumerGroup() + "/ids";
            createPath(consumerIdsPath, CreateMode.PERSISTENT);
            String offsetsPath = zkConf.getBasePath() + "/consumers/" + consumerConfig.getConsumerGroup() + "/offsets";
            createPath(offsetsPath, CreateMode.PERSISTENT);
        }
    }

    private boolean createPath(String path, CreateMode createMode) {
        boolean success;
        try {
            zkClient.create()
                    .creatingParentsIfNeeded()
                    .withMode(createMode)
                    .forPath(path, "".getBytes());
            success = true;
            LOG.info("create path success, path={}", path);
        } catch (KeeperException.NodeExistsException ex1) {
            success = true;
            LOG.debug("node exist, path={}", path);
        } catch (Exception ex2) {
            success = false;
            LOG.debug("createPath exception:", ex2);
        }
        return success;
    }

    public void registerBroker(int shardingId, String ip, int port) {
        String path = zkConf.getBasePath() + "/brokers/" + shardingId + "/" + ip + ":" + port;
        boolean success = createPath(path, CreateMode.EPHEMERAL);
        if (success) {
            LOG.info("register broker sucess, ip={}, port={}", ip, port);
        } else {
            LOG.warn("register broker failed, ip={}, port={}", ip, port);
        }
    }

    public void subscribeBroker() {
        try {
            final String brokerParentPath = zkConf.getBasePath() + "/brokers";
            List<String> shardings = zkClient.getChildren().forPath(brokerParentPath);
            for (String sharding : shardings) {
                final int shardingId = Integer.valueOf(sharding);
                final String shardingPath = brokerParentPath + "/" + sharding;
                List<String> brokerAddressList = zkClient.getChildren().forPath(shardingPath);
                if (isProducer || isConsumer) {
                    RPCClientOptions rpcClientOptions = BrokerClientManager.getRpcClientOptions();
                    for (String address : brokerAddressList) {
                        BrokerClient brokerClient = new BrokerClient(address, rpcClientOptions);
                        BrokerClientManager.getInstance().getBrokerClientMap().put(address, brokerClient);
                    }
                }

                metadata.getBrokerLock().lock();
                try {
                    metadata.getBrokerMap().put(shardingId, brokerAddressList);
                } finally {
                    metadata.getBrokerLock().unlock();
                }

                // 监听broker分片变化
                zkClient.getChildren().usingWatcher(new BrokerShardingWather(shardingId)).forPath(shardingPath);
            }
            // 监听/brokers孩子节点变化
            zkClient.getChildren().usingWatcher(new BrokersWatcher()).forPath(brokerParentPath);
        } catch (Exception ex) {
            LOG.warn("subscribeBroker exception:", ex);
        }
    }

    /**
     * 创建新的topic，加锁是防止重复创建
     * @param topic topic名称
     * @param queueNum queue个数
     */
    public synchronized void registerTopic(String topic, int queueNum) {
        List<Integer> shardingIds = metadata.getBrokerShardingIds();
        int shardingNum = shardingIds.size();
        String topicPath = zkConf.getBasePath() + "/topics/" + topic;
        for (int queueId = 0; queueId < queueNum; queueId++) {
            int index = queueId % shardingNum;
            int shardingId = shardingIds.get(index);
            String queuePath = topicPath + "/" + queueId;
            byte[] queueData = String.valueOf(shardingId).getBytes();
            try {
                zkClient.create()
                        .creatingParentsIfNeeded()
                        .forPath(queuePath, queueData);
            } catch (Exception ex) {
                LOG.warn("registerTopic failed, queue={}", queueId);
            }
        }
    }

    public Map<Integer, Integer> readTopic(String topic) {
        Map<Integer, Integer> queueMap = new HashMap<>();
        String path = zkConf.getBasePath() + "/topics/" + topic;
        try {
            List<String> queues = zkClient.getChildren().forPath(path);
            for (String queue : queues) {
                String queuePath = path + "/" + queue;
                byte[] dataBytes = zkClient.getData().forPath(queuePath);
                if (dataBytes != null) {
                    Integer shardingId = Integer.valueOf(new String(dataBytes));
                    queueMap.put(Integer.valueOf(queue), shardingId);
                }
            }
            LOG.info("readTopic success, topic={}", topic);
        } catch (Exception ex) {
            LOG.warn("readTopic exception:", ex);
        }
        return queueMap;
    }

    public void subscribeTopic() {
        String topicParentPath = zkConf.getBasePath() + "/topics";
        try {
            List<String> topics = zkClient.getChildren().forPath(topicParentPath);
            for (String topic : topics) {
                Map<Integer, Integer> queueMap = readTopicInfo(topic);
                metadata.updateTopicMap(topic, queueMap);
                // 监听topic下的queue变化事件
                // 这里假定queue与shardingId映射关系不会发生变化，所以没有监听queue节点变化事情
                String topicPath = topicParentPath + "/" + topic;
                zkClient.getChildren().usingWatcher(new TopicWatcher(topic)).forPath(topicPath);
            }
            // 监听/topics孩子节点变化情况
            zkClient.getChildren().usingWatcher(new TopicsWather()).forPath(topicParentPath);
        } catch (Exception ex) {
            LOG.warn("subscribeTopic exception:", ex);
        }
    }

    public boolean registerConsumer(String consumerGroup, String consumerId) {
        String path = zkConf.getBasePath() + "/consumers/" + consumerGroup + "/ids/" + consumerId;
        boolean success = createPath(path, CreateMode.EPHEMERAL);
        if (success) {
            LOG.info("registerConsumer sucess, consumerGroup={}, consumerId={}", consumerGroup, consumerId);
        } else {
            LOG.warn("registerConsumer failed, consumerGroup={}, consumerId={}", consumerGroup, consumerId);
        }
        return success;
    }

    public void updateConsumerIds(String consumerGroup) {
        String path = zkConf.getBasePath() + "/consumers/" + consumerGroup + "/ids";
        try {
            List<String> consumerIds = zkClient.getChildren().forPath(path);
            metadata.getConsumerIdsLock().lock();
            try {
                metadata.setConsumerIds(consumerIds);
            } finally {
                metadata.getConsumerIdsLock().unlock();
            }
        } catch (Exception ex) {
            LOG.warn("updateConsumerIds exception:", ex);
        }
    }

    public void subscribeConsumer(String consumerGroup) {
        String path = zkConf.getBasePath() + "/consumers/" + consumerGroup + "/ids";
        try {
            zkClient.getChildren()
                    .usingWatcher(new ConsumerWatcher(consumerGroup))
                    .forPath(path);
        } catch (Exception ex) {
            LOG.warn("subscribeConsumer exception:", ex);
        }
    }

    /**
     * 读取topic消费进度
     * @param consumerGroup 消费组名称
     * @param topic topic
     * @return queue -> offset映射
     */
    public Map<Integer, Long> readConsumerOffset(String consumerGroup, String topic) {
        Map<Integer, Long> queueOffsetMap = new HashMap<>();
        String path = zkConf.getBasePath() + "/consumers/" + consumerGroup + "/offsets/" + topic;
        try {
            // 从zk读取
            List<String> queues = zkClient.getChildren().forPath(path);
            for (String queue : queues) {
                String queuePath = path + "/" + queue;
                byte[] dataBytes = zkClient.getData().forPath(queuePath);
                if (dataBytes != null) {
                    Long offset = Long.valueOf(new String(dataBytes));
                    queueOffsetMap.put(Integer.valueOf(queue), offset);
                }
            }

            // 更新本地内存
            metadata.getConsumerOffsetLock().lock();
            try {
                metadata.getConsumerOffsetMap().putAll(queueOffsetMap);
                LOG.info("read consumer offset success, result={}",
                        JsonUtil.toJson(metadata.getConsumerOffsetMap()));
            } finally {
                metadata.getConsumerOffsetLock().unlock();
            }
        } catch (Exception ex) {
            LOG.debug("readConsumerOffset exception:", ex);
        }
        return queueOffsetMap;
    }

    public void updateConsumerOffset(String consumerGroup, String topic, Integer queueId, long offset) {
        String path = zkConf.getBasePath() + "/consumers/" + consumerGroup + "/offsets/" + topic + "/" + queueId;
        int maxTryCount = 2;
        int currentTryCount = 0;
        while (currentTryCount++ < maxTryCount) {
            try {
                // 更新zk
                byte[] dataBytes = String.valueOf(offset).getBytes();
                zkClient.setData().forPath(path, dataBytes);
                LOG.info("updateConsumerOffset success, consumerGroup={}, topic={}, queue={}, offset={}",
                        consumerGroup, topic, queueId, offset);

                // 更新本地内存
                metadata.getConsumerOffsetLock().lock();
                try {
                    metadata.getConsumerOffsetMap().put(queueId, offset);
                    LOG.info("new consumer offset map={}",
                            JsonUtil.toJson(metadata.getConsumerOffsetMap()));
                } finally {
                    metadata.getConsumerOffsetLock().unlock();
                }
                break;
            } catch (KeeperException.NoNodeException ex1) {
                createPath(path, CreateMode.PERSISTENT);
                continue;
            } catch (Exception ex2) {
                LOG.warn("updateConsumerOffset exception:", ex2);
                break;
            }
        }
    }

    public Map<Integer, Integer> readTopicInfo(String topic) {
        Map<Integer, Integer> queueMap = new HashMap<>();
        String topicPath = zkConf.getBasePath() + "/topics/" + topic;
        try {
            List<String> queues = zkClient.getChildren().forPath(topicPath);
            for (String queue : queues) {
                String queuePath = topicPath + "/" + queue;
                String queueData = new String(zkClient.getData().forPath(queuePath));
                Integer shardingId = Integer.valueOf(queueData);
                Integer queueId = Integer.valueOf(queue);
                queueMap.put(queueId, shardingId);
            }
        } catch (Exception ex) {
            LOG.info("readTopic failed, exception:", ex);
        }
        return queueMap;
    }

    public long getConsumerOffset(Integer queueId) {
        return metadata.getConsumerOffset(queueId);
    }

    public List<String> getBrokerAddressList(Integer shardingId) {
        return metadata.getBrokerAddressList(shardingId);
    }

    public boolean checkTopicExist(String topic) {
        return metadata.checkTopicExist(topic);
    }

    public void updateTopicMap(String topic, Map<Integer, Integer> queueMap) {
        metadata.updateTopicMap(topic, queueMap);
    }

    public Map<Integer, Integer> getTopicQueueMap(String topic) {
        return metadata.getTopicQueueMap(topic);
    }

    public Integer getQueueSharding(String topic, Integer queueId) {
        return metadata.getQueueSharding(topic, queueId);
    }

    public List<String> getConsumerIds() {
        return metadata.getConsumerIds();
    }

    // 监听consumerGroup下的consumer节点数变化
    private class ConsumerWatcher implements CuratorWatcher {
        private String consumerGroup;

        public ConsumerWatcher(String consumerGroup) {
            this.consumerGroup = consumerGroup;
        }

        @Override
        public void process(WatchedEvent event) throws Exception {
            String path = zkConf.getBasePath() + "/consumers/" + consumerGroup + "/ids";
            LOG.info("get zookeeper notification for path={}", path);
            if (event.getType() == Watcher.Event.EventType.NodeChildrenChanged) {
                updateConsumerIds(consumerGroup);
            }
            zkClient.getChildren()
                    .usingWatcher(new ConsumerWatcher(consumerGroup))
                    .forPath(path);
        }
    }

    // 监听所有broker的分片信息变化事件
    private class BrokersWatcher implements CuratorWatcher {
        @Override
        public void process(WatchedEvent event) throws Exception {
            String brokerPath = zkConf.getBasePath() + "/brokers";
            LOG.info("get zookeeper notification for path={}", brokerPath);
            if (event.getType() == Watcher.Event.EventType.NodeChildrenChanged) {
                List<String> newShardings = zkClient.getChildren().forPath(brokerPath);
                List<String> oldShardings = metadata.getBrokerShardings();
                Collection<String> addedShardings = CollectionUtils.subtract(newShardings, oldShardings);
                Collection<String> deletedShardings = CollectionUtils.subtract(oldShardings, newShardings);
                for (String sharding : addedShardings) {
                    int shardingId = Integer.valueOf(sharding);
                    String shardingPath = brokerPath + "/" + sharding;
                    List<String> brokerAddressList = zkClient.getChildren().forPath(shardingPath);
                    for (String address : brokerAddressList) {
                        if (isProducer || isConsumer) {
                            BrokerClient brokerClient = new BrokerClient(
                                    address, BrokerClientManager.getRpcClientOptions());
                            BrokerClientManager.getInstance().getBrokerClientMap().putIfAbsent(address, brokerClient);
                        }
                    }
                    metadata.updateBrokerSharding(shardingId, brokerAddressList);
                    zkClient.getChildren().usingWatcher(new BrokerShardingWather(shardingId)).forPath(shardingPath);
                }

                for (String sharding : deletedShardings) {
                    List<String> brokerList = metadata.removeBrokerSharding(Integer.valueOf(sharding));
                    if ((isProducer || isConsumer) && CollectionUtils.isNotEmpty(brokerList)) {
                        ConcurrentMap<String, BrokerClient> brokerClientMap
                                = BrokerClientManager.getInstance().getBrokerClientMap();
                        for (String address : brokerList) {
                            BrokerClient client = brokerClientMap.get(address);
                            client.getRpcClient().stop();
                            brokerClientMap.remove(address);
                        }
                    }
                }
            }
            zkClient.getChildren()
                    .usingWatcher(new BrokersWatcher())
                    .forPath(brokerPath);
        }
    }

    // 监听broker某个分片下的节点变化
    private class BrokerShardingWather implements CuratorWatcher {
        private int shardingId;

        public BrokerShardingWather(int shardingId) {
            this.shardingId = shardingId;
        }

        @Override
        public void process(WatchedEvent event) throws Exception {
            String shardingPath = zkConf.getBasePath() + "/brokers/" + shardingId;
            LOG.info("get zookeeper notification for path={}", shardingPath);
            if (event.getType() == Watcher.Event.EventType.NodeChildrenChanged) {
                List<String> newBrokerAddressList = zkClient.getChildren().forPath(shardingPath);
                List<String> oldBrokerAddressList = metadata.getBrokerAddressList(shardingId);
                Collection<String> addedBrokerAddressList
                        = CollectionUtils.subtract(newBrokerAddressList, oldBrokerAddressList);
                Collection<String> deletedBrokerAddressList
                        = CollectionUtils.subtract(oldBrokerAddressList, newBrokerAddressList);
                for (String address : addedBrokerAddressList) {
                    metadata.addShardingBrokerAddress(shardingId, address);
                    if (isProducer || isConsumer) {
                        BrokerClient brokerClient = new BrokerClient(
                                address, BrokerClientManager.getRpcClientOptions());
                        BrokerClientManager.getInstance().getBrokerClientMap().putIfAbsent(address, brokerClient);
                    }
                }
                for (String address : deletedBrokerAddressList) {
                    metadata.removeShardingBrokerAddress(shardingId, address);
                    if (isProducer || isConsumer) {
                        BrokerClient brokerClient
                                = BrokerClientManager.getInstance().getBrokerClientMap().remove(address);
                        brokerClient.getRpcClient().stop();
                    }
                }
            }
            zkClient.getChildren()
                    .usingWatcher(new BrokerShardingWather(shardingId))
                    .forPath(shardingPath);
        }
    }

    // 监听所有topic的更变
    private class TopicsWather implements CuratorWatcher {
        @Override
        public void process(WatchedEvent event) throws Exception {
            String topicParentPath = zkConf.getBasePath() + "/topics";
            LOG.info("get zookeeper notification for path={}", topicParentPath);
            if (event.getType() == Watcher.Event.EventType.NodeChildrenChanged) {
                List<String> newTopics = zkClient.getChildren().forPath(topicParentPath);
                List<String> oldTopics = metadata.getAllTopics();
                Collection<String> addedTopics = CollectionUtils.subtract(newTopics, oldTopics);
                Collection<String> deletedTopics = CollectionUtils.subtract(oldTopics, newTopics);
                for (String topic : addedTopics) {
                    String topicPath = topicParentPath + "/" + topic;
                    zkClient.getChildren()
                            .usingWatcher(new TopicWatcher(topic))
                            .forPath(topicPath);
                    Map<Integer, Integer> queueMap = readTopicInfo(topic);
                    metadata.updateTopicMap(topic, queueMap);
                }
                metadata.removeTopics(deletedTopics);
            }
            zkClient.getChildren()
                    .usingWatcher(new TopicsWather())
                    .forPath(topicParentPath);
        }
    }

    // 监听具体某个topic下queue的变更
    private class TopicWatcher implements CuratorWatcher {
        private String topic;

        public TopicWatcher(String topic) {
            this.topic = topic;
        }

        @Override
        public void process(WatchedEvent event) throws Exception {
            String topicPath = zkConf.getBasePath() + "/topics/" + topic;
            LOG.info("get zookeeper notification for path={}", topicPath);
            if (event.getType() == Watcher.Event.EventType.NodeChildrenChanged) {
                List<String> newQueues = zkClient.getChildren().forPath(topicPath);
                List<Integer> newQueueIds = new ArrayList<>();
                for (String queue : newQueues) {
                    newQueueIds.add(Integer.valueOf(queue));
                }
                List<Integer> oldQueueIds = metadata.getTopicQueues(topic);
                Collection<Integer> addedQueueIds = CollectionUtils.subtract(newQueueIds, oldQueueIds);
                Collection<Integer> deletedQueueIds = CollectionUtils.subtract(oldQueueIds, newQueueIds);
                for (Integer queueId : addedQueueIds) {
                    String queuePath = topicPath + "/" + queueId;
                    String queueData = new String(zkClient.getData().forPath(queuePath));
                    Integer shardingId = Integer.valueOf(queueData);
                    metadata.addTopicQueue(topic, queueId, shardingId);
                }
                metadata.deleteTopicQueue(topic, deletedQueueIds);
            }
            zkClient.getChildren()
                    .usingWatcher(new TopicWatcher(topic))
                    .forPath(topicPath);
        }
    }

}
