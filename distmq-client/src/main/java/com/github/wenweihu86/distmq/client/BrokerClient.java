package com.github.wenweihu86.distmq.client;

import com.github.wenweihu86.distmq.client.api.BrokerAPI;
import com.github.wenweihu86.rpc.client.RPCClient;
import com.github.wenweihu86.rpc.client.RPCClientOptions;
import com.github.wenweihu86.rpc.client.RPCProxy;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.commons.lang.Validate;
import org.apache.commons.lang.builder.EqualsBuilder;
import org.apache.commons.lang.builder.HashCodeBuilder;

import java.util.Collection;
import java.util.List;

/**
 * Created by wenweihu86 on 2017/6/24.
 */
public class BrokerClient {
    private List<String> addressList;
    private RPCClient rpcClient;
    private BrokerAPI brokerAPI;

    public BrokerClient(List<String> addressList, RPCClientOptions options) {
        this.addressList = addressList;
        String ipPorts = StringUtils.join(addressList, ",");
        this.rpcClient = new RPCClient(ipPorts, options);
        this.brokerAPI = RPCProxy.getProxy(this.rpcClient, BrokerAPI.class);
    }

    public void addEndPoint(Collection<String> ipPortList) {
        addressList.addAll(ipPortList);
        String ipPorts = StringUtils.join(ipPortList, ",");
        rpcClient.addEndPoints(ipPorts);
    }

    public void removeEndPoint(Collection<String> ipPortList) {
        addressList.removeAll(ipPortList);
        String ipPorts = StringUtils.join(ipPortList, ",");
        rpcClient.removeEndPoints(ipPorts);
    }

    @Override
    public boolean equals(Object object) {
        boolean flag = false;
        if (object != null && BrokerClient.class.isAssignableFrom(object.getClass())) {
            BrokerClient rhs = (BrokerClient) object;
            flag = new EqualsBuilder()
                    .append(addressList, rhs.addressList)
                    .isEquals();
        }
        return flag;
    }

    @Override
    public int hashCode() {
        return new HashCodeBuilder()
                .append(addressList)
                .toHashCode();
    }

    public List<String> getAddressList() {
        return addressList;
    }

    public void setAddressList(List<String> addressList) {
        this.addressList = addressList;
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
