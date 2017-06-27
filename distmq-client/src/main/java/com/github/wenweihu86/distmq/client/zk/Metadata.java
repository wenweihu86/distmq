package com.github.wenweihu86.distmq.client.zk;

import java.util.*;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

/**
 * Created by huwenwei on 2017/6/21.
 */
public class Metadata {
    // shardingId -> broker address list
    private Map<Integer, List<String>> brokerMap = new HashMap<>();
    private Lock brokerLock = new ReentrantLock();

    // topic -> (queueId -> shardingId)
    private Map<String, Map<Integer, Integer>> topicMap = new HashMap<>();
    private Lock topicLock = new ReentrantLock();
    private Condition topicCondition = topicLock.newCondition();

    // consumer ids of group
    private List<String> consumerIds = new ArrayList<>();
    private Lock consumerIdsLock = new ReentrantLock();

    // consumer offset, consumerGroup和topic确定情况下, 只需存储queue -> offset的映射
    private Map<Integer, Long> consumerOffsetMap = new HashMap<>();
    private Lock consumerOffsetLock = new ReentrantLock();

    public long getConsumerOffset(Integer queueId) {
        long offset = 0;
        consumerOffsetLock.lock();
        try {
            if (consumerOffsetMap.containsKey(queueId)) {
                offset = consumerOffsetMap.get(queueId);
            }
        } finally {
            consumerOffsetLock.unlock();
        }
        return offset;
    }

    public List<String> getBrokerAddressList(Integer shardingId) {
        List<String> result = new ArrayList<>();
        brokerLock.lock();
        try {
            List<String> brokers = brokerMap.get(shardingId);
            if (brokers != null) {
                result.addAll(brokers);
            }
        } finally {
            brokerLock.unlock();
        }
        return result;
    }

    public void addShardingBrokerAddress(Integer shardingId, String address) {
        brokerLock.lock();
        try {
            brokerMap.get(shardingId).add(address);
        } finally {
            brokerLock.unlock();
        }
    }

    public void removeShardingBrokerAddress(Integer shardingId, String address) {
        brokerLock.lock();
        try {
            brokerMap.get(shardingId).remove(address);
        } finally {
            brokerLock.unlock();
        }
    }

    public void updateBrokerSharding(Integer shardingId, List<String> brokerAddressList) {
        brokerLock.lock();
        try {
            brokerMap.put(shardingId, brokerAddressList);
        } finally {
            brokerLock.unlock();
        }
    }

    public List<String> removeBrokerSharding(Integer shardingId) {
        brokerLock.lock();
        try {
            return brokerMap.remove(Integer.valueOf(shardingId));
        } finally {
            brokerLock.unlock();
        }
    }

    public List<String> getBrokerShardings() {
        List<String> shardings = new ArrayList<>();
        brokerLock.lock();
        try {
            for (Integer shardingId : brokerMap.keySet()) {
                shardings.add(String.valueOf(shardingId));
            }
        } finally {
            brokerLock.unlock();
        }
        return shardings;
    }

    public List<Integer> getBrokerShardingIds() {
        brokerLock.lock();
        try {
            return new ArrayList<>(brokerMap.keySet());
        } finally {
            brokerLock.unlock();
        }
    }

    public List<String> getAllTopics() {
        topicLock.lock();
        try {
            return new ArrayList<>(topicMap.keySet());
        } finally {
            topicLock.unlock();
        }
    }

    public boolean checkTopicExist(String topic) {
        boolean topicExist = false;
        topicLock.lock();
        try {
            Map<Integer, Integer> queueMap = topicMap.get(topic);
            if (queueMap != null && queueMap.size() > 0) {
                topicExist = true;
            }
        } finally {
            topicLock.unlock();
        }
        return topicExist;
    }

    public void updateTopicMap(String topic, Map<Integer, Integer> queueMap) {
        topicLock.lock();
        try {
            topicMap.put(topic, queueMap);
            topicCondition.signalAll();
        } finally {
            topicLock.unlock();
        }
    }

    public void removeTopics(Collection<String> deletedTopics) {
        topicLock.lock();
        try {
            for (String topic : deletedTopics) {
                topicMap.remove(topic);
            }
        } finally {
            topicLock.unlock();
        }
    }

    public Map<Integer, Integer> getTopicQueueMap(String topic) {
        Map<Integer, Integer> result = new HashMap<>();
        topicLock.lock();
        try {
            Map<Integer, Integer> queueMap = topicMap.get(topic);
            if (queueMap != null) {
                result.putAll(queueMap);
            }
        } finally {
            topicLock.unlock();
        }
        return result;
    }

    public List<Integer> getTopicQueues(String topic) {
        topicLock.lock();
        try {
            return new ArrayList<>(topicMap.get(topic).keySet());
        } finally {
            topicLock.unlock();
        }
    }

    public Integer getQueueSharding(String topic, Integer queueId) {
        topicLock.lock();
        try {
            Map<Integer, Integer> queueMap = topicMap.get(topic);
            if (queueMap != null) {
                return queueMap.get(queueId);
            }
        } finally {
            topicLock.unlock();
        }
        return null;
    }

    public void addTopicQueue(String topic, Integer queueId, Integer shardingId) {
        topicLock.lock();
        try {
            topicMap.get(topic).put(queueId, shardingId);
            topicCondition.signalAll();
        } finally {
            topicLock.unlock();
        }
    }

    public void deleteTopicQueue(String topic, Collection<Integer> queueIds) {
        topicLock.lock();
        try {
            for (Integer queueId : queueIds) {
                topicMap.get(topic).remove(queueId);
            }
        } finally {
            topicLock.unlock();
        }
    }

    public Map<Integer, List<String>> getBrokerMap() {
        return brokerMap;
    }

    public void setBrokerMap(Map<Integer, List<String>> brokerMap) {
        this.brokerMap = brokerMap;
    }

    public Lock getBrokerLock() {
        return brokerLock;
    }

    public Map<String, Map<Integer, Integer>> getTopicMap() {
        return topicMap;
    }

    public void setTopicMap(Map<String, Map<Integer, Integer>> topicMap) {
        this.topicMap = topicMap;
    }

    public Lock getTopicLock() {
        return topicLock;
    }

    public Condition getTopicCondition() {
        return topicCondition;
    }

    public List<String> getConsumerIds() {
        consumerIdsLock.lock();
        try {
            return new ArrayList<>(consumerIds);
        } finally {
            consumerIdsLock.unlock();
        }
    }

    public void setConsumerIds(List<String> consumerIds) {
        this.consumerIds = consumerIds;
    }

    public Lock getConsumerIdsLock() {
        return consumerIdsLock;
    }

    public void setConsumerIdsLock(Lock consumerIdsLock) {
        this.consumerIdsLock = consumerIdsLock;
    }

    public Map<Integer, Long> getConsumerOffsetMap() {
        return consumerOffsetMap;
    }

    public Lock getConsumerOffsetLock() {
        return consumerOffsetLock;
    }
}
