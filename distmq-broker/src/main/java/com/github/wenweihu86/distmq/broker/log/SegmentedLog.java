package com.github.wenweihu86.distmq.broker.log;

import com.github.wenweihu86.distmq.broker.GlobalConf;
import com.github.wenweihu86.raft.util.RaftFileUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.util.List;
import java.util.TreeMap;

/**
 * Created by wenweihu86 on 2017/6/19.
 */
public class SegmentedLog {

    private static Logger LOG = LoggerFactory.getLogger(SegmentedLog.class);

    private String segmentDir;
    private TreeMap<Long, Segment> startOffsetSegmentMap = new TreeMap<>();
    // segment log占用的内存大小，用于判断是否需要做snapshot
    private volatile long totalSize;

    public SegmentedLog(String segmentDir) {
        this.segmentDir = segmentDir;
        File file = new File(segmentDir);
        if (!file.exists()) {
            file.mkdirs();
        }
        readSegments();
        validateSegments();
    }

    public long getLastEndOffset() {
        if (startOffsetSegmentMap.size() == 0) {
            return 0;
        }
        Segment lastSegment = startOffsetSegmentMap.lastEntry().getValue();
        return lastSegment.getEndOffset();
    }

    public long append(byte[] messageContent) {
        boolean isNeedNewSegmentFile = false;
        int segmentSize = startOffsetSegmentMap.size();
        try {
            if (segmentSize == 0) {
                isNeedNewSegmentFile = true;
            } else {
                Segment lastSegment = startOffsetSegmentMap.lastEntry().getValue();
                if (!lastSegment.isCanWrite()) {
                    isNeedNewSegmentFile = true;
                } else {
                    int maxSegmentSize = GlobalConf.getInstance().getInt("max_segment_size");
                    if (lastSegment.getFileSize() + messageContent.length > maxSegmentSize) {
                        isNeedNewSegmentFile = true;
                    }
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

            Segment newSegment;
            // 新建segment文件
            if (isNeedNewSegmentFile) {
                // open new segment file
                long newStartOffset = getLastEndOffset() + Segment.HEADER_LENGTH;
                String newSegmentFileName = String.format("open-%d", newStartOffset);
                String newFullFileName = segmentDir + File.separator + newSegmentFileName;
                File newSegmentFile = new File(newFullFileName);
                if (!newSegmentFile.exists()) {
                    newSegmentFile.createNewFile();
                }
                newSegment = new Segment(segmentDir, newSegmentFileName);
            } else {
                newSegment = startOffsetSegmentMap.lastEntry().getValue();
            }
            return newSegment.append(messageContent);
        } catch (IOException ex) {
            throw new RuntimeException("meet exception, msg=" + ex.getMessage());
        }
    }

    public void readSegments() {
        List<String> fileNames = RaftFileUtils.getSortedFilesInDirectory(segmentDir);
        for (String fileName: fileNames) {
            Segment segment = new Segment(segmentDir, fileName);
            startOffsetSegmentMap.put(segment.getStartOffset(), segment);
        }
    }

    public void validateSegments() {
        long lastEndOffset = 0;
        for (Segment segment : startOffsetSegmentMap.values()) {
            if (lastEndOffset > 0 && segment.getStartOffset() != lastEndOffset + Segment.HEADER_LENGTH) {
                throw new RuntimeException("segment dir not valid:" + segmentDir);
            }
            lastEndOffset = segment.getEndOffset();
        }
    }

}
