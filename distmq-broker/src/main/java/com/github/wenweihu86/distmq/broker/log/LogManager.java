package com.github.wenweihu86.distmq.broker.log;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.util.HashMap;
import java.util.Map;

/**
 * Created by wenweihu86 on 2017/6/20.
 */
public class LogManager {
    private static final Logger LOG = LoggerFactory.getLogger(LogManager.class);
    private String logDir;
    private Map<String, Map<Integer, SegmentedLog>> topicLogMap;

    public LogManager(String logDir) {
        this.topicLogMap = new HashMap<>();
        this.logDir = logDir;
        File dirFile = new File(logDir);
        if (!dirFile.exists()) {
            dirFile.mkdirs();
        }
        File[] topicDirs = dirFile.listFiles();
        if (topicDirs != null) {
            for (File topicDir : topicDirs) {
                if (!topicDir.isDirectory()) {
                    LOG.warn("{} is not directory", topicDir.getAbsolutePath());
                    continue;
                }
                LOG.info("Loading log from " + topicDir.getAbsolutePath());
                if (!this.topicLogMap.containsKey(topicDir)) {
                    this.topicLogMap.put(topicDir.getName(), new HashMap<Integer, SegmentedLog>());
                }
                Map<Integer, SegmentedLog> queueMap = this.topicLogMap.get(topicDir.getName());
                File[] queueDirs = topicDir.listFiles();
                if (queueDirs != null) {
                    for (File queueDir : queueDirs) {
                        if (!queueDir.isDirectory()) {
                            LOG.warn("{} is not directory", queueDir.getAbsolutePath());
                            continue;
                        }
                        Integer queueId = Integer.valueOf(queueDir.getName());
                        String fullQueuePath = logDir + File.separator + topicDir + File.separator + queueDir;
                        SegmentedLog queueLog = new SegmentedLog(fullQueuePath);
                        queueMap.put(queueId, queueLog);
                    }
                }
            }
        }
    }

    public SegmentedLog getOrCreateQueueLog(String topic, int queue) {
        // TODO:
        // 需要读取zookeeper中topic/queue信息来判断该queue是否应该存在本broker集群分片
        // zookeeper存储结构为/distmq/topics/topicName/queueId -> brokerShardingId
        return null;
    }

}
