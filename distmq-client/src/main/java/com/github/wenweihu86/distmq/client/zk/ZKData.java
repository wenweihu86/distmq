package com.github.wenweihu86.distmq.client.zk;

import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

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
    private Map<Integer, List<String>> brokerMap = new ConcurrentHashMap<>();

    // topic -> (queueId -> shardingId)
    private Map<String, Map<Integer, Integer>> topicMap = new ConcurrentHashMap<>();

    public static void setInstance(ZKData instance) {
        ZKData.instance = instance;
    }

    public Map<Integer, List<String>> getBrokerMap() {
        return brokerMap;
    }

    public void setBrokerMap(Map<Integer, List<String>> brokerMap) {
        this.brokerMap = brokerMap;
    }

    public Map<String, Map<Integer, Integer>> getTopicMap() {
        return topicMap;
    }

    public void setTopicMap(Map<String, Map<Integer, Integer>> topicMap) {
        this.topicMap = topicMap;
    }
}
