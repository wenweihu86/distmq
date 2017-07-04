package com.github.wenweihu86.distmq.broker;

import com.github.wenweihu86.distmq.broker.config.GlobalConf;
import com.github.wenweihu86.distmq.client.zk.MetadataManager;
import com.github.wenweihu86.distmq.client.zk.ZKConf;
import com.github.wenweihu86.raft.RaftNode;
import com.github.wenweihu86.raft.RaftOptions;
import com.github.wenweihu86.raft.proto.RaftMessage;
import com.github.wenweihu86.raft.service.RaftClientService;
import com.github.wenweihu86.raft.service.RaftConsensusService;
import com.github.wenweihu86.raft.service.impl.RaftClientServiceImpl;
import com.github.wenweihu86.raft.service.impl.RaftConsensusServiceImpl;
import com.github.wenweihu86.raft.util.ConfigurationUtils;
import com.github.wenweihu86.rpc.server.RPCServer;

import java.io.File;
import java.util.List;

/**
 * Created by wenweihu86 on 2017/6/17.
 */
public class BrokerMain {
    public static void main(String[] args) {
        // read conf
        GlobalConf conf = GlobalConf.getInstance();
        RaftMessage.Server localServer = conf.getLocalServer();
        List<RaftMessage.Server> servers = conf.getServers();
        String dataDir = System.getProperty("user.dir") + File.separator + conf.getDataDir();

        // 初始化zookeeper
        ZKConf zkConf = conf.getZkConf();
        MetadataManager metadataManager = new MetadataManager(zkConf);

        // 初始化RPCServer
        RPCServer server = new RPCServer(localServer.getEndPoint().getPort());
        // 应用状态机
        BrokerStateMachine stateMachine = new BrokerStateMachine(dataDir);
        // 设置数据目录
        RaftOptions.dataDir = dataDir;
        // just for test snapshot
        RaftOptions.snapshotMinLogSize = 10 * 1024;
        RaftOptions.snapshotPeriodSeconds = 30;
        RaftOptions.maxSegmentFileSize = 1024 * 1024;
        // 初始化RaftNode
        RaftNode raftNode = new RaftNode(servers, localServer, stateMachine);
        stateMachine.setRaftNode(raftNode);
        // 注册Raft节点之间相互调用的服务
        RaftConsensusService raftConsensusService = new RaftConsensusServiceImpl(raftNode);
        server.registerService(raftConsensusService);
        // 注册给Client调用的Raft服务
        RaftClientService raftClientService = new RaftClientServiceImpl(raftNode);
        server.registerService(raftClientService);
        // 注册应用自己提供的服务
        BrokerAPIImpl brokerAPI = new BrokerAPIImpl(raftNode, stateMachine, metadataManager);
        server.registerService(brokerAPI);
        // 启动RPCServer，初始化Raft节点
        server.start();
        raftNode.init();

        // 订阅topic的变化
        metadataManager.subscribeTopic();
        // 等成为raft集群成员后，才能注册到zk
        while (!ConfigurationUtils.containsServer(
                raftNode.getConfiguration(), conf.getLocalServer().getServerId())) {
            try {
                Thread.sleep(1000);
            } catch (InterruptedException ex) {
                ex.printStackTrace();
            }
        }
        metadataManager.registerBroker(conf.getShardingId(),
                conf.getLocalServer().getEndPoint().getHost(),
                conf.getLocalServer().getEndPoint().getPort());
    }

}
