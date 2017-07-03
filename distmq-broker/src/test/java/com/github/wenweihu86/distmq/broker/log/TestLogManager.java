package com.github.wenweihu86.distmq.broker.log;

import com.github.wenweihu86.distmq.broker.BrokerStateMachine;
import com.github.wenweihu86.distmq.broker.config.GlobalConf;
import com.github.wenweihu86.distmq.client.api.BrokerMessage;
import com.github.wenweihu86.raft.RaftNode;
import com.github.wenweihu86.raft.storage.Snapshot;
import com.google.protobuf.ByteString;
import org.apache.commons.io.FileUtils;
import org.junit.Assert;
import org.junit.Test;
import org.mockito.Mockito;

import java.io.File;
import java.io.IOException;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Created by wenweihu86 on 2017/6/26.
 */
public class TestLogManager {

    private BrokerMessage.MessageContent.Builder createMessage(String topic, Integer queue) {
        BrokerMessage.MessageContent.Builder message = BrokerMessage.MessageContent.newBuilder()
                .setContent(ByteString.copyFrom(UUID.randomUUID().toString().getBytes()))
                .setTopic(topic)
                .setQueue(queue);
        return message;
    }

    @Test
    public void testClearExpiredLog() {
        // mock
        Snapshot snapshot = Mockito.mock(Snapshot.class);
        Mockito.when(snapshot.getIsInstallSnapshot()).thenReturn(new AtomicBoolean(false));
        Mockito.when(snapshot.getIsTakeSnapshot()).thenReturn(new AtomicBoolean(false));
        RaftNode raftNode = Mockito.mock(RaftNode.class);
        Mockito.when(raftNode.getSnapshot()).thenReturn(snapshot);
        Mockito.doNothing().when(raftNode).takeSnapshot();
        BrokerStateMachine stateMachine = new BrokerStateMachine();
        stateMachine.setRaftNode(raftNode);

        GlobalConf conf = GlobalConf.getInstance();
        conf.setMaxSegmentSize(128);
        conf.setExpiredLogDuration(1);
        LogManager logManager = new LogManager(conf.getDataDir(), stateMachine);
        String topic = "test-topic";
        Integer queue = 0;
        SegmentedLog log = logManager.getOrCreateQueueLog(topic, queue);
        for (int i = 0; i < 1000; i++) {
            log.append(createMessage(topic, queue));
        }

        try {
            Thread.sleep(2000);
        } catch (InterruptedException ex) {
            ex.printStackTrace();
        }

        System.out.println("before clear log count=" + log.getStartOffsetSegmentMap().size());
        logManager.run();
        System.out.println("after clear log count=" + log.getStartOffsetSegmentMap().size());
        Assert.assertTrue(log.getStartOffsetSegmentMap().size() == 1);
        File file = new File(conf.getDataDir());
        try {
            FileUtils.deleteDirectory(file);
        } catch (IOException ex) {
            ex.printStackTrace();
        }
    }
}
