package com.github.wenweihu86.distmq.client;

import com.github.wenweihu86.distmq.client.api.BrokerAPI;
import com.github.wenweihu86.rpc.client.RPCClient;
import com.github.wenweihu86.rpc.client.RPCClientOptions;
import com.github.wenweihu86.rpc.client.RPCProxy;
import org.apache.commons.lang.builder.EqualsBuilder;
import org.apache.commons.lang.builder.HashCodeBuilder;

/**
 * Created by wenweihu86 on 2017/6/24.
 */
public class BrokerClient {
    private String address;
    private RPCClient rpcClient;
    private BrokerAPI brokerAPI;

    public BrokerClient(String address, RPCClientOptions options) {
        this.address = address;
        this.rpcClient = new RPCClient(address, options);
        this.brokerAPI = RPCProxy.getProxy(this.rpcClient, BrokerAPI.class);
    }

    @Override
    public boolean equals(Object object) {
        boolean flag = false;
        if (object != null && BrokerClient.class.isAssignableFrom(object.getClass())) {
            BrokerClient rhs = (BrokerClient) object;
            flag = new EqualsBuilder()
                    .append(address, rhs.address)
                    .isEquals();
        }
        return flag;
    }

    @Override
    public int hashCode() {
        return new HashCodeBuilder()
                .append(address)
                .toHashCode();
    }

    public String getAddress() {
        return address;
    }

    public void setAddress(String address) {
        this.address = address;
    }

    public RPCClient getRpcClient() {
        return rpcClient;
    }

    public void setRpcClient(RPCClient rpcClient) {
        this.rpcClient = rpcClient;
    }

    public BrokerAPI getBrokerAPI() {
        return brokerAPI;
    }

    public void setBrokerAPI(BrokerAPI brokerAPI) {
        this.brokerAPI = brokerAPI;
    }
}
