package com.github.wenweihu86.distmq.broker;

import com.github.wenweihu86.distmq.broker.log.LogManager;
import com.github.wenweihu86.distmq.client.api.BrokerMessage;
import com.github.wenweihu86.raft.StateMachine;
import org.apache.commons.io.FileUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;

/**
 * Created by wenweihu86 on 2017/6/17.
 */
public class BrokerStateMachine implements StateMachine {
    private static final Logger LOG = LoggerFactory.getLogger(BrokerStateMachine.class);
    private String messageDir;
    private LogManager logManager;

    public BrokerStateMachine() {
        String dataDir = GlobalConf.getInstance().getDataDir();
        this.messageDir = dataDir + File.separator + "message";
    }

    @Override
    public void writeSnapshot(String snapshotDir) {
        try {
            File messageDirFile = new File(messageDir);
            File snapshotDirFile = new File(snapshotDir);
            if (snapshotDirFile.exists()) {
                FileUtils.deleteDirectory(snapshotDirFile);
            }
            if (messageDirFile.exists()) {
                FileUtils.copyDirectory(messageDirFile, snapshotDirFile);
            }
        } catch (IOException ex) {
            LOG.warn("snapshot failed");
        }
    }

    @Override
    public void readSnapshot(String snapshotDir) {
        try {
            File mqDirFile = new File(messageDir);
            if (mqDirFile.exists()) {
                FileUtils.deleteDirectory(mqDirFile);
            }
            File snapshotDirFile = new File(snapshotDir);
            if (snapshotDirFile.exists()) {
                FileUtils.copyDirectory(snapshotDirFile, mqDirFile);
            }
            logManager = new LogManager(messageDir);
        } catch (IOException ex) {
            LOG.error("readSnapshot error");
            throw new RuntimeException(ex);
        }
    }

    @Override
    public void apply(byte[] dataBytes) {
        try {
            BrokerMessage.SendMessageRequest request = BrokerMessage.SendMessageRequest.parseFrom(dataBytes);
            // TODO: 找到segment log，写入消息
        } catch (Exception ex) {
            LOG.warn("apply exception:", ex);
        }
    }

}
