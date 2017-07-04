package com.github.wenweihu86.distmq.broker.log;

import com.github.wenweihu86.distmq.broker.BrokerStateMachine;
import com.github.wenweihu86.distmq.broker.config.GlobalConf;
import com.github.wenweihu86.distmq.client.api.BrokerMessage;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.util.*;
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
    private BrokerStateMachine stateMachine;

    public LogManager(String logDir, BrokerStateMachine stateMachine) throws IOException {
        this.stateMachine = stateMachine;
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
                        String fullQueuePath = queueDir.getCanonicalPath();
                        SegmentedLog queueLog = new SegmentedLog(fullQueuePath);
                        queueMap.put(queueId, queueLog);
                    }
                }
            }
        }

        timer.scheduleWithFixedDelay(this,
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

    public void close() {
        Collection<ConcurrentMap<Integer, SegmentedLog>> queueLogs = topicLogMap.values();
        List<SegmentedLog> allLogs = new ArrayList<>();
        for (ConcurrentMap<Integer, SegmentedLog> queueLogMap : queueLogs) {
            allLogs.addAll(queueLogMap.values());
        }
        for (SegmentedLog log : allLogs) {
            log.close();
        }
        topicLogMap.clear();
    }

    // 清理过期消息期间，禁止进行snapshot；
    // 清理完过期消息后，需要重新执行take snapshot
    @Override
    public void run() {
        if (stateMachine.getRaftNode().getSnapshot().getIsInstallSnapshot().get()) {
            LOG.info("already in install snapshot, please clear expired messages later");
            return;
        }
        if (!stateMachine.getRaftNode().getSnapshot().getIsTakeSnapshot().compareAndSet(false, true)) {
            LOG.info("already in take snapshot, please clear expired messages later");
            return;
        }
        LOG.info("start to clear expired messages");
        try {
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
                                List<Long> deletedKeyList = new ArrayList<>();
                                Segment lastSegment = null;
                                Iterator<Map.Entry<Long, Segment>> iterator
                                        = log.getStartOffsetSegmentMap().entrySet().iterator();
                                while (iterator.hasNext()) {
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
                                        deletedKeyList.add(lastSegment.getStartOffset());
                                    } else {
                                        break;
                                    }
                                    lastSegment = segment;
                                }
                                for (Long startOffset : deletedKeyList) {
                                    log.getStartOffsetSegmentMap().remove(startOffset);
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
        } finally {
            stateMachine.getRaftNode().getSnapshot().getIsTakeSnapshot().compareAndSet(true, false);
        }
        LOG.info("end to clear expired messages");

        stateMachine.getRaftNode().takeSnapshot();
    }

}
