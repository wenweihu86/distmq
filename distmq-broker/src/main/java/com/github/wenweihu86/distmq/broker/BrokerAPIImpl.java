package com.github.wenweihu86.distmq.broker;

import com.github.wenweihu86.distmq.broker.config.GlobalConf;
import com.github.wenweihu86.distmq.client.api.BrokerAPI;
import com.github.wenweihu86.distmq.client.api.BrokerMessage;
import com.github.wenweihu86.distmq.client.zk.ZKClient;
import com.github.wenweihu86.distmq.client.zk.ZKData;
import com.github.wenweihu86.raft.RaftNode;
import com.github.wenweihu86.raft.proto.RaftMessage;
import com.github.wenweihu86.rpc.client.RPCClient;
import com.github.wenweihu86.rpc.client.RPCProxy;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;

/**
 * Created by wenweihu86 on 2017/6/17.
 */
public class BrokerAPIImpl implements BrokerAPI {

    private static final Logger LOG = LoggerFactory.getLogger(BrokerAPIImpl.class);

    private RaftNode raftNode;
    private BrokerStateMachine stateMachine;
    private ZKClient zkClient;

    public BrokerAPIImpl(RaftNode raftNode, BrokerStateMachine stateMachine, ZKClient zkClient) {
        this.raftNode = raftNode;
        this.stateMachine = stateMachine;
        this.zkClient = zkClient;
    }

    @Override
    public BrokerMessage.SendMessageResponse sendMessage(BrokerMessage.SendMessageRequest request) {
        BrokerMessage.SendMessageResponse.Builder responseBuilder = BrokerMessage.SendMessageResponse.newBuilder();
        BrokerMessage.BaseResponse.Builder baseResBuilder = BrokerMessage.BaseResponse.newBuilder();
        baseResBuilder.setResCode(BrokerMessage.ResCode.RES_CODE_FAIL);

        // 查询topic是否存在
        ZKData zkData = ZKData.getInstance();
        GlobalConf conf = GlobalConf.getInstance();
        boolean topicExist = false;
        boolean shardingValid = false;
        zkData.getTopicLock().lock();
        try {
            Map<Integer, Integer> queueMap = zkData.getTopicMap().get(request.getTopic());
            if (queueMap != null && queueMap.size() > 0) {
                topicExist = true;
                if (queueMap.get(request.getQueue()) == conf.getShardingId()) {
                    shardingValid = true;
                }
            }
        } finally {
            zkData.getTopicLock().unlock();
        }

        // 如果topic尚不存在，请求zookeeper读取
        if (!topicExist) {
            Map<Integer, Integer> queueMap = zkClient.readTopicInfo(request.getTopic());
            zkData.getTopicLock().lock();
            try {
                if (!zkData.getTopicMap().containsKey(request.getTopic())) {
                    zkData.getTopicMap().put(request.getTopic(), queueMap);
                }
                queueMap = zkData.getTopicMap().get(request.getTopic());
                if (queueMap.size() > 0) {
                    topicExist = true;
                }
                if (queueMap.get(request.getQueue()) == conf.getShardingId()) {
                    shardingValid = true;
                }
            } finally {
                zkData.getTopicLock().unlock();
            }
        }

        // 验证queue存在，并且属于该sharding
        if (!topicExist || !shardingValid) {
            String message = "queue not exist or not be included by this sharding";
            baseResBuilder.setResMsg(message);
            responseBuilder.setBaseRes(baseResBuilder.build());
            LOG.info("sendMessage request, topic={}, queue={}, resCode={}, resMsg={}",
                    request.getTopic(), request.getQueue(),
                    responseBuilder.getBaseRes().getResCode(),
                    responseBuilder.getBaseRes().getResMsg());
            return responseBuilder.build();
        }

        // 如果自己不是leader，将写请求转发给leader
        if (raftNode.getLeaderId() <= 0) {
            baseResBuilder.setResMsg("leader not exists");
            responseBuilder.setBaseRes(baseResBuilder);
        } else if (raftNode.getLeaderId() != raftNode.getLocalServer().getServerId()) {
            RPCClient rpcClient = raftNode.getPeerMap().get(raftNode.getLeaderId()).getRpcClient();
            BrokerAPI brokerAPI = RPCProxy.getProxy(rpcClient, BrokerAPI.class);
            BrokerMessage.SendMessageResponse responseFromLeader = brokerAPI.sendMessage(request);
            if (responseFromLeader == null) {
                baseResBuilder.setResMsg("leader timeout");
                responseBuilder.setBaseRes(baseResBuilder);
            } else {
                responseBuilder.mergeFrom(responseFromLeader);
            }
        } else {
            // 数据同步写入raft集群
            byte[] data = request.toByteArray();
            boolean success = raftNode.replicate(data, RaftMessage.EntryType.ENTRY_TYPE_DATA);
            baseResBuilder.setResCode(
                    success ? BrokerMessage.ResCode.RES_CODE_SUCCESS
                            : BrokerMessage.ResCode.RES_CODE_FAIL);
            responseBuilder.setBaseRes(baseResBuilder);
        }

        BrokerMessage.SendMessageResponse response = responseBuilder.build();
        LOG.info("sendMessage request, topic={}, queue={}, resCode={}, resMsg={}",
                request.getTopic(), request.getQueue(),
                responseBuilder.getBaseRes().getResCode(),
                responseBuilder.getBaseRes().getResMsg());
        return response;
    }

    @Override
    public BrokerMessage.PullMessageResponse pullMessage(BrokerMessage.PullMessageRequest request) {
        BrokerMessage.PullMessageResponse response = stateMachine.pullMessage(request);
        LOG.info("pullMessage request, topic={}, queue={}, "
                        + "resCode={}, resSize={}",
                request.getTopic(), request.getQueue(),
                response.getBaseRes().getResCode(),
                response.getContentsCount());
        return response;
    }

}
