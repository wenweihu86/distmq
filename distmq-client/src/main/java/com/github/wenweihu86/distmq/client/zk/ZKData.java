package com.github.wenweihu86.distmq.client.zk;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

/**
 * Created by huwenwei on 2017/6/21.
 */
public class ZKData {
    private static ZKData instance;

    public static ZKData getInstance() {
        if (instance == null) {
            instance = new ZKData();
        }
        return instance;
    }

    // shardingId -> broker address list
    private Map<Integer, List<String>> brokerMap = new HashMap<>();
    private Lock brokerLock = new ReentrantLock();

    // topic -> (queueId -> shardingId)
    private Map<String, Map<Integer, Integer>> topicMap = new HashMap<>();

    private Lock topicLock = new ReentrantLock();
    private Condition topicCondition = topicLock.newCondition();

    public static void setInstance(ZKData instance) {
        ZKData.instance = instance;
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
}
