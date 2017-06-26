package com.github.wenweihu86.distmq.broker.log;

import com.github.wenweihu86.distmq.broker.BrokerUtils;
import com.github.wenweihu86.distmq.client.api.BrokerMessage;
import com.github.wenweihu86.raft.util.RaftFileUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;

/**
 * queue中每个文件用segment表示
 * Created by wenweihu86 on 2017/6/19.
 */
public class Segment {
    public static int MESSAGE_HEADER_LENGTH = (Long.SIZE + Integer.SIZE) / Byte.SIZE;
    private static final Logger LOG = LoggerFactory.getLogger(Segment.class);

    private String dirName;
    private String fileName;
    private boolean canWrite;
    private long startOffset; // 该文件中第一个消息的offset，offset在queue目录下唯一
    private long endOffset; // 该文件中最后一个消息的offset
    private long fileSize;
    private RandomAccessFile randomAccessFile;
    private FileChannel channel;
    private long lastModifiedTime; // 文件最后修改时间

    public Segment(String dirName, String fileName) {
        this.dirName = dirName;
        this.fileName = fileName;
        String[] splitArray = fileName.split("-");
        if (splitArray.length != 2) {
            LOG.error("segment filename is not valid, segmentDir={}, file={}",
                    dirName, fileName);
            throw new RuntimeException("invalid segmentDir=" + dirName);
        }
        try {
            if (splitArray[0].equals("open")) {
                this.setCanWrite(true);
                this.setStartOffset(Long.valueOf(splitArray[1]));
                this.setEndOffset(0);
            } else {
                try {
                    this.setCanWrite(false);
                    this.setStartOffset(Long.parseLong(splitArray[0]));
                    this.setEndOffset(Long.parseLong(splitArray[1]));
                } catch (NumberFormatException ex) {
                    LOG.error("invalid segment file name:{}", fileName);
                    throw new RuntimeException("invalid segmentDir=" + dirName);
                }
            }
            this.setRandomAccessFile(RaftFileUtils.openFile(dirName, fileName, "rw"));
            this.setChannel(this.randomAccessFile.getChannel());
            this.setFileSize(this.randomAccessFile.length());
        } catch (IOException ioException) {
            LOG.warn("open segment file error, file={}, msg={}",
                    fileName, ioException.getMessage());
            throw new RuntimeException("open segment file error");
        }
    }

    public void close() {
        try {
            this.channel.close();
            this.randomAccessFile.close();
        } catch (IOException ex) {
            LOG.warn("close file exception:", ex);
        }
    }

    public boolean append(BrokerMessage.MessageContent.Builder messageBuilder) {
        try {
            if (fileSize == 0) {
                messageBuilder.setOffset(startOffset);
            } else {
                messageBuilder.setOffset(endOffset);
            }
            messageBuilder.setCreateTime(System.currentTimeMillis());
            BrokerMessage.MessageContent message = messageBuilder.build();
            byte[] messageBytes = message.toByteArray();
            int totalSize = Segment.MESSAGE_HEADER_LENGTH + messageBytes.length;
            ByteBuffer byteBuffer = ByteBuffer.allocate(totalSize);
            byteBuffer.putLong(BrokerUtils.getCRC32(messageBytes));
            byteBuffer.putInt(messageBytes.length);
            byteBuffer.put(messageBytes);
            byteBuffer.flip();
            int writeSize = channel.write(byteBuffer);
            channel.force(true);
            if (writeSize != totalSize) {
                LOG.warn("append message failed");
                return false;
            }
            if (fileSize == 0) {
                endOffset = startOffset + writeSize;
            } else {
                endOffset += writeSize;
            }
            fileSize += writeSize;
        } catch (IOException ex) {
            LOG.warn("append message exception:", ex);
            return false;
        }
        return true;
    }

    public BrokerMessage.MessageContent read(long offset) {
        if (offset >= startOffset + fileSize) {
            LOG.debug("invalid offset={}", offset);
            return null;
        }
        try {
            channel.position(offset - startOffset);
            ByteBuffer headerBuffer = ByteBuffer.allocate(MESSAGE_HEADER_LENGTH);
            int readLen = channel.read(headerBuffer);
            if (readLen != MESSAGE_HEADER_LENGTH) {
                LOG.warn("read message error");
                return null;
            }
            headerBuffer.flip();
            long crc32 = headerBuffer.getLong();
            int messageLen = headerBuffer.getInt();
            LOG.debug("messageLen={}", messageLen);
            ByteBuffer messageContentBuffer = ByteBuffer.allocate(messageLen);
            readLen = channel.read(messageContentBuffer);
            if (readLen != messageLen) {
                LOG.warn("read message error");
                return null;
            }
            if (BrokerUtils.getCRC32(messageContentBuffer.array()) != crc32) {
                LOG.warn("read message error: crc32 check failed");
                return null;
            }
            BrokerMessage.MessageContent message
                    = BrokerMessage.MessageContent.parseFrom(messageContentBuffer.array());
            BrokerMessage.MessageContent result = BrokerMessage.MessageContent.newBuilder()
                    .mergeFrom(message).setSize(MESSAGE_HEADER_LENGTH + messageLen).build();
            return result;
        } catch (IOException ex) {
            LOG.warn("read segment error, dir={}, file={}, offset={}",
                    dirName, fileName, offset);
            return null;
        }
    }

    public String getDirName() {
        return dirName;
    }

    public void setDirName(String dirName) {
        this.dirName = dirName;
    }

    public String getFileName() {
        return fileName;
    }

    public void setFileName(String fileName) {
        this.fileName = fileName;
    }

    public boolean isCanWrite() {
        return canWrite;
    }

    public void setCanWrite(boolean canWrite) {
        this.canWrite = canWrite;
    }

    public long getStartOffset() {
        return startOffset;
    }

    public void setStartOffset(long startOffset) {
        this.startOffset = startOffset;
    }

    public long getEndOffset() {
        return endOffset;
    }

    public void setEndOffset(long endOffset) {
        this.endOffset = endOffset;
    }

    public long getFileSize() {
        return fileSize;
    }

    public void setFileSize(long fileSize) {
        this.fileSize = fileSize;
    }

    public RandomAccessFile getRandomAccessFile() {
        return randomAccessFile;
    }

    public void setRandomAccessFile(RandomAccessFile randomAccessFile) {
        this.randomAccessFile = randomAccessFile;
    }

    public FileChannel getChannel() {
        return channel;
    }

    public void setChannel(FileChannel channel) {
        this.channel = channel;
    }

    public long getLastModifiedTime() {
        return lastModifiedTime;
    }

    public void setLastModifiedTime(long lastModifiedTime) {
        this.lastModifiedTime = lastModifiedTime;
    }
}
