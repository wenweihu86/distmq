package com.github.wenweihu86.distmq.client;

import com.github.wenweihu86.distmq.client.zk.ZKConf;
import com.github.wenweihu86.rpc.client.RPCClientOptions;

/**
 * Created by wenweihu86 on 2017/6/24.
 */
public class CommonConfig extends ZKConf {
    private int brokerConnectTimeoutMs = 200;
    private int brokerReadTimeoutMs = 500;
    private int brokerWriteTimeoutMs = 200;

    public RPCClientOptions getRPCClientOptions() {
        RPCClientOptions rpcClientOptions = new RPCClientOptions();
        rpcClientOptions.setConnectTimeoutMillis(brokerConnectTimeoutMs);
        rpcClientOptions.setReadTimeoutMillis(brokerReadTimeoutMs);
        rpcClientOptions.setWriteTimeoutMillis(brokerWriteTimeoutMs);
        return rpcClientOptions;
    }

    public int getBrokerConnectTimeoutMs() {
        return brokerConnectTimeoutMs;
    }

    public void setBrokerConnectTimeoutMs(int brokerConnectTimeoutMs) {
        this.brokerConnectTimeoutMs = brokerConnectTimeoutMs;
    }

    public int getBrokerReadTimeoutMs() {
        return brokerReadTimeoutMs;
    }

    public void setBrokerReadTimeoutMs(int brokerReadTimeoutMs) {
        this.brokerReadTimeoutMs = brokerReadTimeoutMs;
    }

    public int getBrokerWriteTimeoutMs() {
        return brokerWriteTimeoutMs;
    }

    public void setBrokerWriteTimeoutMs(int brokerWriteTimeoutMs) {
        this.brokerWriteTimeoutMs = brokerWriteTimeoutMs;
    }
}
