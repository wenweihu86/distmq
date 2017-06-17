package com.github.wenweihu86.distmq.broker;

import com.github.wenweihu86.raft.StateMachine;

/**
 * Created by wenweihu86 on 2017/6/17.
 */
public class BrokerStateMachine implements StateMachine {

    @Override
    public void writeSnapshot(String snapshotDir) {
    }

    @Override
    public void readSnapshot(String snapshotDir) {
    }

    @Override
    public void apply(byte[] dataBytes) {
    }

}
