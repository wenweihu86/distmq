package com.github.wenweihu86.distmq.broker.log;

import com.github.wenweihu86.distmq.broker.config.GlobalConf;
import com.github.wenweihu86.distmq.client.api.BrokerMessage;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.*;

/**
 * Created by wenweihu86 on 2017/6/20.
 */
public class LogManager implements Runnable {
    private static final Logger LOG = LoggerFactory.getLogger(LogManager.class);
    private String logDir;
    // topic -> (queueId -> segment log)
    private ConcurrentMap<String, ConcurrentMap<Integer, SegmentedLog>> topicLogMap;
    private ScheduledExecutorService timer = Executors.newScheduledThreadPool(1);

    public LogManager(String logDir) {
        this.topicLogMap = new ConcurrentHashMap<>();
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
                    this.topicLogMap.put(topicDir.getName(), new ConcurrentHashMap<Integer, SegmentedLog>());
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

        timer.scheduleAtFixedRate(this,
                GlobalConf.getInstance().getExpiredLogCheckInterval(),
                GlobalConf.getInstance().getExpiredLogCheckInterval(),
                TimeUnit.SECONDS);
    }

    public SegmentedLog getOrCreateQueueLog(String topic, int queue) {
        ConcurrentMap<Integer, SegmentedLog> queueMap = topicLogMap.get(topic);
        if (queueMap == null) {
            queueMap = new ConcurrentHashMap<>();
            topicLogMap.putIfAbsent(topic, queueMap);
            queueMap = topicLogMap.get(topic);
        }
        SegmentedLog segmentedLog = queueMap.get(queue);
        if (segmentedLog == null) {
            String fullQueuePath = logDir + File.separator + topic + File.separator + queue;
            segmentedLog = new SegmentedLog(fullQueuePath);
            queueMap.putIfAbsent(queue, segmentedLog);
            segmentedLog = queueMap.get(queue);
        }
        return segmentedLog;
    }

    @Override
    public void run() {
        GlobalConf conf = GlobalConf.getInstance();
        Set<String> topicSet = topicLogMap.keySet();
        for (String topic : topicSet) {
            ConcurrentMap<Integer, SegmentedLog> queueLogMap = topicLogMap.get(topic);
            if (queueLogMap != null) {
                Set<Integer> queueSet = queueLogMap.keySet();
                for (Integer queue : queueSet) {
                    try {
                        SegmentedLog log = topicLogMap.get(topic).get(queue);
                        log.getLock().lock();
                        try {
                            Segment lastSegment = null;
                            Iterator<Map.Entry<Long, Segment>> iterator
                                    = log.getStartOffsetSegmentMap().entrySet().iterator();
                            while (iterator.hasNext()){
                                Segment segment = iterator.next().getValue();
                                if (lastSegment == null) {
                                    lastSegment = segment;
                                    continue;
                                }
                                BrokerMessage.MessageContent message = segment.read(segment.getStartOffset());
                                if (System.currentTimeMillis() / 1000
                                        - message.getCreateTime() / 1000
                                        > conf.getExpiredLogDuration()) {
                                    lastSegment.delete();
                                    iterator.remove();
                                } else {
                                    break;
                                }
                                lastSegment = segment;
                            }
                        } finally {
                            log.getLock().unlock();
                        }
                    } catch (Exception ex) {
                        LOG.warn("clear expired log error");
                    }
                }
            }
        }
    }

}
