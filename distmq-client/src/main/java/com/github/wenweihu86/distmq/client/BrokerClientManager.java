package com.github.wenweihu86.distmq.client;

import com.github.wenweihu86.rpc.client.RPCClientOptions;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

/**
 * Created by wenweihu86 on 2017/6/24.
 */
public class BrokerClientManager {
    private static BrokerClientManager instance;
    private static RPCClientOptions rpcClientOptions;
    private ConcurrentMap<String, BrokerClient> brokerClientMap = new ConcurrentHashMap<>();

    public static BrokerClientManager getInstance() {
        if (instance == null) {
            instance = new BrokerClientManager();
        }
        return instance;
    }

    public static RPCClientOptions getRpcClientOptions() {
        return rpcClientOptions;
    }

    public static void setRpcClientOptions(RPCClientOptions rpcClientOptions) {
        BrokerClientManager.rpcClientOptions = rpcClientOptions;
    }

    public ConcurrentMap<String, BrokerClient> getBrokerClientMap() {
        return brokerClientMap;
    }

    public void setBrokerClientMap(ConcurrentMap<String, BrokerClient> brokerClientMap) {
        this.brokerClientMap = brokerClientMap;
    }
}
