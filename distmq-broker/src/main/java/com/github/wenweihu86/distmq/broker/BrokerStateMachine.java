package com.github.wenweihu86.distmq.broker;

import com.github.wenweihu86.distmq.broker.config.GlobalConf;
import com.github.wenweihu86.distmq.broker.log.LogManager;
import com.github.wenweihu86.distmq.broker.log.SegmentedLog;
import com.github.wenweihu86.distmq.client.api.BrokerMessage;
import com.github.wenweihu86.raft.RaftNode;
import com.github.wenweihu86.raft.StateMachine;
import org.apache.commons.io.FileUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.nio.file.FileSystems;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Created by wenweihu86 on 2017/6/17.
 */
public class BrokerStateMachine implements StateMachine {
    private static final Logger LOG = LoggerFactory.getLogger(BrokerStateMachine.class);
    private String messageDir;
    private LogManager logManager;
    // 状态机数据是否可用，当在read snapshot时，状态机数据不可用，主要发生在每次install snapshot时。
    private AtomicBoolean isAvailable = new AtomicBoolean(true);
    private RaftNode raftNode;

    public BrokerStateMachine() {
        String dataDir = GlobalConf.getInstance().getDataDir();
        this.messageDir = dataDir + File.separator + "message";
    }

    @Override
    public void writeSnapshot(String snapshotDir) {
        // 采用软链接形式，提升速度和节省空间
        try {
            File messageDirFile = new File(messageDir);
            File snapshotDirFile = new File(snapshotDir);
            if (snapshotDirFile.exists()) {
                FileUtils.deleteDirectory(snapshotDirFile);
            }
            if (messageDirFile.exists()) {
                Path link = FileSystems.getDefault().getPath(snapshotDir);
                Path target = FileSystems.getDefault().getPath(messageDir).toRealPath();
                Files.createSymbolicLink(link, target);
            }
        } catch (IOException ex) {
            LOG.warn("write snapshot failed, exception:", ex);
        }
    }

    @Override
    public void readSnapshot(String snapshotDir) {
        try {
            isAvailable.compareAndSet(true, false);
            Path link = FileSystems.getDefault().getPath(snapshotDir);
            if (!Files.isSymbolicLink(link)) {
                // 非符号链接，表示从leader节点同步拷贝的
                if (logManager != null) {
                    logManager.close();
                }
                File messageDirFile = new File(messageDir);
                if (messageDirFile.exists()) {
                    FileUtils.deleteDirectory(messageDirFile);
                }
                File snapshotDirFile = new File(snapshotDir);
                if (snapshotDirFile.exists()) {
                    FileUtils.copyDirectory(snapshotDirFile, messageDirFile);
                }
            }
            logManager = new LogManager(messageDir, this);
        } catch (IOException ex) {
            LOG.error("readSnapshot exception:", ex);
            throw new RuntimeException(ex);
        } finally {
            isAvailable.compareAndSet(false,  true);
        }
    }

    @Override
    public void apply(byte[] dataBytes) {
        try {
            if (isAvailable.get()) {
                BrokerMessage.SendMessageRequest request = BrokerMessage.SendMessageRequest.parseFrom(dataBytes);
                BrokerMessage.MessageContent.Builder message = BrokerMessage.MessageContent.newBuilder()
                        .setTopic(request.getTopic())
                        .setQueue(request.getQueue())
                        .setContent(request.getContent());
                SegmentedLog segmentedLog = logManager.getOrCreateQueueLog(request.getTopic(), request.getQueue());
                segmentedLog.append(message);
            }
        } catch (Exception ex) {
            LOG.warn("apply exception:", ex);
        }
    }

    public BrokerMessage.PullMessageResponse pullMessage(BrokerMessage.PullMessageRequest request) {
        BrokerMessage.PullMessageResponse.Builder responseBuilder = BrokerMessage.PullMessageResponse.newBuilder();
        BrokerMessage.BaseResponse.Builder baseResBuilder = BrokerMessage.BaseResponse.newBuilder();
        if (!isAvailable.get()) {
            baseResBuilder.setResCode(BrokerMessage.ResCode.RES_CODE_FAIL);
            baseResBuilder.setResMsg("state machine is busy");
            responseBuilder.setBaseRes(baseResBuilder);
            return responseBuilder.build();
        }
        SegmentedLog segmentedLog = logManager.getOrCreateQueueLog(request.getTopic(), request.getQueue());
        int readCount = 0;
        long offset = request.getOffset();
        while (readCount < request.getMessageCount()) {
            BrokerMessage.MessageContent message = segmentedLog.read(offset);
            if (message == null) {
                break;
            }
            responseBuilder.addContents(message);
            offset = message.getOffset() + message.getSize();
            readCount++;
        }

        baseResBuilder.setResCode(BrokerMessage.ResCode.RES_CODE_SUCCESS);
        responseBuilder.setBaseRes(baseResBuilder);
        return responseBuilder.build();
    }

    public RaftNode getRaftNode() {
        return raftNode;
    }

    public void setRaftNode(RaftNode raftNode) {
        this.raftNode = raftNode;
    }
}
