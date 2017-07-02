package com.github.wenweihu86.distmq.broker.config;

import com.github.wenweihu86.distmq.broker.BrokerUtils;
import com.github.wenweihu86.distmq.client.zk.ZKConf;
import com.github.wenweihu86.raft.proto.RaftMessage;
import com.moandjiezana.toml.Toml;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.util.ArrayList;
import java.util.List;

/**
 * Created by wenweihu86 on 2017/6/17.
 */
public class GlobalConf {
    private static final Logger LOG = LoggerFactory.getLogger(GlobalConf.class);
    private static GlobalConf instance;

    private Toml toml;
    RaftMessage.Server localServer; // 本机节点
    List<RaftMessage.Server> servers; // 集群所有节点
    private String dataDir; // 数据目录
    private int defaultQueueNumPerTopic; // 每个topic的默认queue个数
    private int maxSegmentSize; // 单个segment文件最大大小
    private int expiredLogCheckInterval; // log检查时间间隔
    private int expiredLogDuration; // log过期时长
    // 该server属于哪个分片集群，每个分片是leader/followers的raft集群
    private int shardingId;
    private ZKConf zkConf;

    public GlobalConf() {
        String fileName = "/broker.toml";
        File file = new File(getClass().getResource(fileName).getFile());
        toml = new Toml().read(file);
        localServer = readLocalServer();
        servers = readServers();
        dataDir = toml.getString("data_dir");
        defaultQueueNumPerTopic = toml.getLong("default_queue_num_per_topic").intValue();
        maxSegmentSize = toml.getLong("max_segment_size").intValue();
        shardingId = toml.getLong("sharding_id").intValue();
        expiredLogCheckInterval = toml.getLong("expired_log_check_interval").intValue();
        expiredLogDuration = toml.getLong("expired_log_duration").intValue();
        zkConf = readZKConf();
    }

    public static GlobalConf getInstance() {
        if (instance == null) {
            instance = new GlobalConf();
        }
        return instance;
    }

    private RaftMessage.Server readLocalServer() {
        RaftMessage.Server.Builder serverBuilder = RaftMessage.Server.newBuilder();
        RaftMessage.EndPoint.Builder endPointBuilder = RaftMessage.EndPoint.newBuilder();
        Toml localServerConf = toml.getTable("local_server");
        endPointBuilder.setHost(localServerConf.getString("ip"));
        endPointBuilder.setPort(localServerConf.getLong("port").intValue());
        serverBuilder.setEndPoint(endPointBuilder);
        serverBuilder.setServerId(localServerConf.getLong("id").intValue());
        RaftMessage.Server localServer = serverBuilder.build();
        LOG.info("read local_server conf={}", BrokerUtils.protoToJson(localServer));
        return localServer;
    }

    private List<RaftMessage.Server> readServers() {
        List<RaftMessage.Server> servers = new ArrayList<>();
        List<Toml> serverConfList = toml.getTables("servers");
        for (Toml serverConf : serverConfList) {
            RaftMessage.EndPoint endPoint = RaftMessage.EndPoint.newBuilder()
                    .setHost(serverConf.getString("ip"))
                    .setPort(serverConf.getLong("port").intValue())
                    .build();
            RaftMessage.Server server = RaftMessage.Server.newBuilder()
                    .setEndPoint(endPoint)
                    .setServerId(serverConf.getLong("id").intValue())
                    .build();
            LOG.info("read conf server={}", BrokerUtils.protoToJson(server));
            servers.add(server);
        }
        return servers;
    }

    private ZKConf readZKConf() {
        Toml zookeeperToml = toml.getTable("zookeeper");
        zkConf = new ZKConf();
        zkConf.setZKServers(zookeeperToml.getString("servers"));
        zkConf.setZKConnectTimeoutMs(zookeeperToml.getLong("connect_timeout_ms").intValue());
        zkConf.setZKSessionTimeoutMs(zookeeperToml.getLong("session_timeout_ms").intValue());
        zkConf.setZKRetryCount(zookeeperToml.getLong("retry_count").intValue());
        zkConf.setZKRetryIntervalMs(zookeeperToml.getLong("retry_interval_ms").intValue());
        zkConf.setZKBasePath(zookeeperToml.getString("base_path"));
        return zkConf;
    }

    public RaftMessage.Server getLocalServer() {
        return localServer;
    }

    public List<RaftMessage.Server> getServers() {
        return servers;
    }

    public String getDataDir() {
        return dataDir;
    }

    public int getDefaultQueueNumPerTopic() {
        return defaultQueueNumPerTopic;
    }

    public int getMaxSegmentSize() {
        return maxSegmentSize;
    }

    public int getShardingId() {
        return shardingId;
    }

    public int getExpiredLogCheckInterval() {
        return expiredLogCheckInterval;
    }

    public int getExpiredLogDuration() {
        return expiredLogDuration;
    }

    public void setMaxSegmentSize(int maxSegmentSize) {
        this.maxSegmentSize = maxSegmentSize;
    }

    public void setExpiredLogDuration(int expiredLogDuration) {
        this.expiredLogDuration = expiredLogDuration;
    }

    public ZKConf getZkConf() {
        return zkConf;
    }

}
