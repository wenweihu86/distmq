package com.github.wenweihu86.distmq.broker.log;

import com.github.wenweihu86.distmq.broker.config.GlobalConf;
import com.github.wenweihu86.distmq.client.api.BrokerMessage;
import com.github.wenweihu86.raft.util.RaftFileUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

/**
 * Created by wenweihu86 on 2017/6/19.
 */
public class SegmentedLog {

    private static Logger LOG = LoggerFactory.getLogger(SegmentedLog.class);

    private String segmentDir;
    private TreeMap<Long, Segment> startOffsetSegmentMap = new TreeMap<>();
    private Lock lock = new ReentrantLock();

    public SegmentedLog(String segmentDir) {
        this.segmentDir = segmentDir;
        File file = new File(segmentDir);
        if (!file.exists()) {
            file.mkdirs();
        }
        readSegments();
        validateSegments();
    }

    public boolean append(BrokerMessage.MessageContent.Builder message) {
        boolean isNeedNewSegmentFile = false;
        int segmentSize = startOffsetSegmentMap.size();
        lock.lock();
        try {
            if (segmentSize == 0) {
                isNeedNewSegmentFile = true;
            } else {
                Segment lastSegment = startOffsetSegmentMap.lastEntry().getValue();
                if (!lastSegment.isCanWrite()) {
                    isNeedNewSegmentFile = true;
                } else {
                    int maxSegmentSize = GlobalConf.getInstance().getMaxSegmentSize();
                    if (lastSegment.getFileSize() + message.build().getSerializedSize() > maxSegmentSize) {
                        isNeedNewSegmentFile = true;
                        // 最后一个segment的文件close并改名
                        lastSegment.close();
                        lastSegment.setCanWrite(false);
                        String newFileName = String.format("%020d-%020d",
                                lastSegment.getStartOffset(), lastSegment.getEndOffset());
                        String newFullFileName = segmentDir + File.separator + newFileName;
                        File newFile = new File(newFullFileName);
                        newFile.createNewFile();
                        String oldFullFileName = segmentDir + File.separator + lastSegment.getFileName();
                        File oldFile = new File(oldFullFileName);
                        oldFile.renameTo(newFile);
                        lastSegment.setFileName(newFileName);
                        lastSegment.setRandomAccessFile(RaftFileUtils.openFile(segmentDir, newFileName, "r"));
                        lastSegment.setChannel(lastSegment.getRandomAccessFile().getChannel());
                    }
                }
            }

            Segment newSegment;
            // 新建segment文件
            if (isNeedNewSegmentFile) {
                // open new segment file
                long newStartOffset = getLastEndOffset();
                String newSegmentFileName = String.format("open-%d", newStartOffset);
                String newFullFileName = segmentDir + File.separator + newSegmentFileName;
                File newSegmentFile = new File(newFullFileName);
                if (!newSegmentFile.exists()) {
                    newSegmentFile.createNewFile();
                }
                newSegment = new Segment(segmentDir, newSegmentFileName);
                startOffsetSegmentMap.put(newSegment.getStartOffset(), newSegment);
            } else {
                newSegment = startOffsetSegmentMap.lastEntry().getValue();
            }
            return newSegment.append(message);
        } catch (IOException ex) {
            throw new RuntimeException("meet exception, msg=" + ex.getMessage());
        } finally {
            lock.unlock();
        }
    }

    public BrokerMessage.MessageContent read(long offset) {
        lock.lock();
        try {
            Map.Entry<Long, Segment> entry = startOffsetSegmentMap.floorEntry(offset);
            if (entry == null) {
                LOG.warn("message not found, offset={}", offset);
                return null;
            }
            Segment segment = entry.getValue();
            return segment.read(offset);
        } finally {
            lock.unlock();
        }
    }

    public void close() {
        lock.lock();
        try {
            for (Segment segment : startOffsetSegmentMap.values()) {
                segment.close();
            }
            startOffsetSegmentMap.clear();
        } finally {
            lock.unlock();
        }
    }

    private void readSegments() {
        try {
            List<String> fileNames = RaftFileUtils.getSortedFilesInDirectory(segmentDir, segmentDir);
            for (String fileName : fileNames) {
                Segment segment = new Segment(segmentDir, fileName);
                startOffsetSegmentMap.put(segment.getStartOffset(), segment);
            }
        } catch (IOException ex) {
            LOG.warn("read segments exception:", ex);
            throw new RuntimeException("read segments exception", ex);
        }
    }

    private void validateSegments() {
        long lastEndOffset = 0;
        for (Segment segment : startOffsetSegmentMap.values()) {
            if (lastEndOffset > 0 && segment.getStartOffset() != lastEndOffset) {
                throw new RuntimeException("segment dir not valid:" + segmentDir);
            }
            lastEndOffset = segment.getEndOffset();
        }
    }

    private long getLastEndOffset() {
        if (startOffsetSegmentMap.size() == 0) {
            return 0;
        }
        Segment lastSegment = startOffsetSegmentMap.lastEntry().getValue();
        return lastSegment.getEndOffset();
    }

    public String getSegmentDir() {
        return segmentDir;
    }

    public TreeMap<Long, Segment> getStartOffsetSegmentMap() {
        return startOffsetSegmentMap;
    }

    public Lock getLock() {
        return lock;
    }

}
