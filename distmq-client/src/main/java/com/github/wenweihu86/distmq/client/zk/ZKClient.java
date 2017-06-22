package com.github.wenweihu86.distmq.client.zk;

import org.apache.commons.collections.CollectionUtils;
import org.apache.curator.RetryPolicy;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.framework.api.CuratorWatcher;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

/**
 * Created by wenweihu86 on 2017/6/21.
 */
public class ZKClient {
    private static final Logger LOG = LoggerFactory.getLogger(ZKClient.class);
    private ZKConf zkConf;
    private CuratorFramework zkClient;

    public ZKClient(ZKConf conf) {
        this.zkConf = conf;
        RetryPolicy retryPolicy = new ExponentialBackoffRetry(
                zkConf.getRetryIntervalMs(), zkConf.getRetryCount());
        this.zkClient = CuratorFrameworkFactory.builder()
                .connectString(zkConf.getServers())
                .retryPolicy(retryPolicy)
                .connectionTimeoutMs(zkConf.getConnectTimeoutMs())
                .sessionTimeoutMs(zkConf.getSessionTimeoutMs())
                .build();
        this.zkClient.start();
    }

    public void registerBroker(int shardingId, String ip, int port) {
        String path = String.format("%s/brokers/%d/%s:%d",
                zkConf.getBasePath(), shardingId, ip, port);
        try {
            zkClient.create()
                    .creatingParentsIfNeeded()
                    .withMode(CreateMode.EPHEMERAL)
                    .forPath(path, "".getBytes());
        } catch (Exception ex) {
            LOG.warn("registerBroker exception:", ex);
        }
    }

    public void subscribeBroker() {
        final ZKData zkData = ZKData.getInstance();
        final Map<Integer, List<String>> brokerMap = zkData.getBrokerMap();
        try {
            final String brokerParentPath = zkConf.getBasePath() + "/brokers";
            List<String> shardings = zkClient.getChildren().forPath(brokerParentPath);
            for (String sharding : shardings) {
                final int shardingId = Integer.valueOf(sharding);
                final String shardingPath = brokerParentPath + "/" + sharding;
                List<String> brokerAddressList = zkClient.getChildren().forPath(shardingPath);
                brokerMap.put(shardingId, brokerAddressList);
                // 监听broker分片变化
                zkClient.getChildren().usingWatcher(
                        new BrokerShardingWather(shardingId))
                        .forPath(shardingPath);
            }
            // 监听/brokers孩子节点变化
            zkClient.getChildren().usingWatcher(new BrokersWatcher()).forPath(brokerParentPath);
        } catch (Exception ex) {
            LOG.warn("subscribeBroker exception:", ex);
        }
    }

    public void registerTopic(String topic, int queueNum) {
        ZKData zkData = ZKData.getInstance();
        Map<Integer, List<String>> brokerMap = zkData.getBrokerMap();
        List<Integer> shardingIds = new ArrayList<>(brokerMap.keySet());
        int shardingNum = shardingIds.size();
        String topicPath = zkConf.getBasePath() + "/topics/";
        for (int queueId = 0; queueId < queueNum; queueId++) {
            int shardingId = queueId % shardingNum;
            String queuePath = topicPath + queueId;
            byte[] queueData = String.valueOf(shardingId).getBytes();
            try {
                zkClient.create()
                        .creatingParentsIfNeeded()
                        .forPath(queuePath, queueData);
            } catch (Exception ex) {
                LOG.warn("registerTopic exception:", ex);
            }
        }
    }

    public void subscribeTopic() {
        ZKData zkData = ZKData.getInstance();
        Map<String, Map<Integer, Integer>> topicMap = zkData.getTopicMap();
        String topicParentPath = zkConf.getBasePath() + "/topics/";
        try {
            List<String> topics = zkClient.getChildren().forPath(topicParentPath);
            for (String topic : topics) {
                Map<Integer, Integer> queueMap = topicMap.get(topic);
                if (queueMap == null) {
                    queueMap = new HashMap<>();
                    topicMap.put(topic, queueMap);
                }
                String topicPath = topicParentPath + "/" + topic;
                List<String> queues = zkClient.getChildren().forPath(topicPath);
                for (String queue : queues) {
                    String queuePath = topicPath + "/" + queue;
                    String queueData = new String(zkClient.getData().forPath(queuePath));
                    Integer shardingId = Integer.valueOf(queueData);
                    Integer queueId = Integer.valueOf(queue);
                    queueMap.put(queueId, shardingId);
                }
                // 监听topic下的queue变化事件
                // 这里假定queue与shardingId映射关系不会发生变化，所以没有监听queue节点变化事情
                zkClient.getChildren().usingWatcher(
                        new TopicWatcher(topic))
                        .forPath(topicPath);
            }
            // 监听/topics孩子节点变化情况
            zkClient.getChildren().usingWatcher(
                    new TopicsWather())
                    .forPath(topicParentPath);
        } catch (Exception ex) {
            LOG.warn("subscribeTopic exception:", ex);
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
            ZKData zkData = ZKData.getInstance();
            Map<Integer, List<String>> brokerMap = zkData.getBrokerMap();
            if (event.getType() == Watcher.Event.EventType.NodeChildrenChanged) {
                String shardingPath = zkConf.getBasePath() + "/brokers/" + shardingId;
                List<String> newBrokerAddressList = zkClient.getChildren().forPath(shardingPath);
                // TODO: 对于client需要关闭被删除节点的连接，以及新建新增节点连接
                brokerMap.put(shardingId, newBrokerAddressList);
            }
        }
    }

    // 监听所有broker的分片信息变化事件
    private class BrokersWatcher implements CuratorWatcher {
        @Override
        public void process(WatchedEvent event) throws Exception {
            ZKData zkData = ZKData.getInstance();
            Map<Integer, List<String>> brokerMap = zkData.getBrokerMap();
            if (event.getType() == Watcher.Event.EventType.NodeChildrenChanged) {
                String brokerPath = zkConf.getBasePath() + "/brokers";
                List<String> newShardings = zkClient.getChildren().forPath(brokerPath);
                Iterator<Map.Entry<Integer, List<String>>> iterator = brokerMap.entrySet().iterator();
                while (iterator.hasNext()){
                    Map.Entry<Integer, List<String>> entry = iterator.next();
                    if (!newShardings.contains(Integer.valueOf(entry.getKey()))) {
                        // TODO:对于client，需要删除对应节点的连接
                        iterator.remove();
                    }
                }
                for (String sharding : newShardings) {
                    int shardingId = Integer.valueOf(sharding);
                    if (!brokerMap.containsKey(shardingId)) {
                        String shardingPath = brokerPath + "/" + sharding;
                        zkClient.getChildren().usingWatcher(
                                new BrokerShardingWather(shardingId))
                                .forPath(shardingPath);
                    }
                }
            }
        }
    }

    // 监听所有topic的更变
    private class TopicsWather implements CuratorWatcher {
        @Override
        public void process(WatchedEvent event) throws Exception {
            ZKData zkData = ZKData.getInstance();
            Map<String, Map<Integer, Integer>> topicMap = zkData.getTopicMap();
            if (event.getType() == Watcher.Event.EventType.NodeChildrenChanged) {
                String topicParentPath = zkConf.getBasePath() + "/topics";
                List<String> newTopics = zkClient.getChildren().forPath(topicParentPath);
                List<String> oldTopics = new ArrayList<>(topicMap.keySet());
                if (CollectionUtils.isEmpty(newTopics)) {
                    LOG.warn("there is no topics");
                    topicMap.clear();
                } else {
                    Collection<String> addedTopics = CollectionUtils.subtract(newTopics, oldTopics);
                    Collection<String> deletedTopics = CollectionUtils.subtract(oldTopics, newTopics);
                    for (String topic : addedTopics) {
                        Map<Integer, Integer> queueMap = topicMap.get(topic);
                        String topicPath = topicParentPath + "/" + topic;
                        List<String> queueList = zkClient.getChildren().forPath(topicPath);
                        for (String queue : queueList) {
                            String queuePath = topicPath + "/" + queue;
                            String queueData = new String(zkClient.getData().forPath(queuePath));
                            int shardingId = Integer.valueOf(queueData);
                            int queueId = Integer.valueOf(queue);
                            queueMap.put(queueId, shardingId);
                        }
                    }

                    for (String topic : deletedTopics) {
                        topicMap.remove(topic);
                        // TODO: is need remove watcher?
                    }
                }
            }
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
            ZKData zkData = ZKData.getInstance();
            Map<String, Map<Integer, Integer>> topicMap = zkData.getTopicMap();
            Map<Integer, Integer> queueMap = topicMap.get(topic);
            if (event.getType() == Watcher.Event.EventType.NodeChildrenChanged) {
                String topicPath = zkConf.getBasePath() + "/topics/" + topic;
                List<String> newQueues = zkClient.getChildren().forPath(topicPath);
                List<Integer> newQueueIds = new ArrayList<>();
                for (String queue : newQueues) {
                    newQueueIds.add(Integer.valueOf(queue));
                }
                List<Integer> oldQueueIds = new ArrayList<>(queueMap.keySet());
                Collection<Integer> addedQueueIds = CollectionUtils.subtract(newQueueIds, oldQueueIds);
                Collection<Integer> deletedQueueIds = CollectionUtils.subtract(oldQueueIds, newQueueIds);
                for (Integer queueId : addedQueueIds) {
                    String queuePath = topicPath + "/" + queueId;
                    String queueData = new String(zkClient.getData().forPath(queuePath));
                    Integer shardingId = Integer.valueOf(queueData);
                    queueMap.put(queueId, shardingId);
                }
                for (Integer queueId : deletedQueueIds) {
                    queueMap.remove(queueId);
                }
            }
        }
    }

}
