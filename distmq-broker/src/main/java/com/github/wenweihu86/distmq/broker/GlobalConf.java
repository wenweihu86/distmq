package com.github.wenweihu86.distmq.broker;

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

    public GlobalConf() {
        String fileName = "/broker.toml";
        File file = new File(getClass().getResource(fileName).getFile());
        toml = new Toml().read(file);
        localServer = readLocalServer();
        servers = readServers();
        dataDir = toml.getString("data_dir");
        defaultQueueNumPerTopic = toml.getLong("default_queue_num_per_topic").intValue();
        maxSegmentSize = toml.getLong("max_segment_size").intValue();
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

}
