package com.github.wenweihu86.distmq.broker;

import com.github.wenweihu86.distmq.broker.config.GlobalConf;
import com.github.wenweihu86.distmq.client.api.BrokerAPI;
import com.github.wenweihu86.distmq.client.api.BrokerMessage;
import com.github.wenweihu86.distmq.client.zk.MetadataManager;
import com.github.wenweihu86.distmq.client.zk.Metadata;
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
    private MetadataManager metadataManager;

    public BrokerAPIImpl(RaftNode raftNode, BrokerStateMachine stateMachine, MetadataManager metadataManager) {
        this.raftNode = raftNode;
        this.stateMachine = stateMachine;
        this.metadataManager = metadataManager;
    }

    @Override
    public BrokerMessage.SendMessageResponse sendMessage(BrokerMessage.SendMessageRequest request) {
        BrokerMessage.SendMessageResponse.Builder responseBuilder = BrokerMessage.SendMessageResponse.newBuilder();
        BrokerMessage.BaseResponse.Builder baseResBuilder = BrokerMessage.BaseResponse.newBuilder();
        baseResBuilder.setResCode(BrokerMessage.ResCode.RES_CODE_FAIL);

        // 查询topic是否存在
        boolean topicExist = metadataManager.checkTopicExist(request.getTopic());
        // 如果topic尚不存在，请求zookeeper读取
        if (!topicExist) {
            Map<Integer, Integer> queueMap = metadataManager.readTopicInfo(request.getTopic());
            if (queueMap.size() > 0) {
                topicExist = true;
            }
            metadataManager.updateTopicMap(request.getTopic(), queueMap);
        }

        // 由producer来保证topic已经存在，broker就不再校验topic是否存在。
        // 验证queue存在，并且属于该sharding
//        boolean shardingValid = false;
//        GlobalConf conf = GlobalConf.getInstance();
//        Integer shardingId = metadataManager.getQueueSharding(request.getTopic(), request.getQueue());
//        if (shardingId != null && shardingId.equals(conf.getShardingId())) {
//            shardingValid = true;
//        }
//        if (!topicExist || !shardingValid) {
//            String message = "queue not exist or not be included by this sharding";
//            baseResBuilder.setResMsg(message);
//            responseBuilder.setBaseRes(baseResBuilder.build());
//            LOG.warn("sendMessage request, topic={}, queue={}, resCode={}, "
//                            + "topicExist={}, shardingValid={}, resMsg={}",
//                    request.getTopic(), request.getQueue(),
//                    responseBuilder.getBaseRes().getResCode(),
//                    topicExist, shardingValid, responseBuilder.getBaseRes().getResMsg());
//            return responseBuilder.build();
//        }

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
