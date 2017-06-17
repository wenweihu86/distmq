package com.github.wenweihu86.distmq.broker;

import com.github.wenweihu86.distmq.client.api.BrokerAPI;
import com.github.wenweihu86.distmq.client.api.BrokerMessage;
import com.github.wenweihu86.raft.RaftNode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Created by wenweihu86 on 2017/6/17.
 */
public class BrokerAPIImpl implements BrokerAPI {

    private static final Logger LOG = LoggerFactory.getLogger(BrokerAPIImpl.class);

    private RaftNode raftNode;
    private BrokerStateMachine stateMachine;

    public BrokerAPIImpl(RaftNode raftNode, BrokerStateMachine stateMachine) {
        this.raftNode = raftNode;
        this.stateMachine = stateMachine;
    }

    @Override
    public BrokerMessage.SendMessageResponse sendMessage(BrokerMessage.SendMessageRequest request) {
        return null;
    }

    @Override
    public BrokerMessage.PullMessageResponse pullMessage(BrokerMessage.PullMessageRequest request) {
        return null;
    }

}
