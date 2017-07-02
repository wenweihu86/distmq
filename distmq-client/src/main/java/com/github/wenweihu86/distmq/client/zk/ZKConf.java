package com.github.wenweihu86.distmq.client.zk;

/**
 * Created by wenweihu86 on 2017/6/21.
 */
public class ZKConf {
    private String zkServers;
    private int zkConnectTimeoutMs = 500;
    private int zkSessionTimeoutMs = 5000;
    private int zkRetryCount = 3;
    private int zkRetryIntervalMs = 1000;
    private String zkBasePath = "/distmq";

    public String getZKServers() {
        return zkServers;
    }

    public void setZKServers(String zkServers) {
        this.zkServers = zkServers;
    }

    public int getZKConnectTimeoutMs() {
        return zkConnectTimeoutMs;
    }

    public void setZKConnectTimeoutMs(int zkConnectTimeoutMs) {
        this.zkConnectTimeoutMs = zkConnectTimeoutMs;
    }

    public int getZKSessionTimeoutMs() {
        return zkSessionTimeoutMs;
    }

    public void setZKSessionTimeoutMs(int zkSessionTimeoutMs) {
        this.zkSessionTimeoutMs = zkSessionTimeoutMs;
    }

    public int getZKRetryCount() {
        return zkRetryCount;
    }

    public void setZKRetryCount(int zkRetryCount) {
        this.zkRetryCount = zkRetryCount;
    }

    public int getZKRetryIntervalMs() {
        return zkRetryIntervalMs;
    }

    public void setZKRetryIntervalMs(int zkRetryIntervalMs) {
        this.zkRetryIntervalMs = zkRetryIntervalMs;
    }

    public String getZKBasePath() {
        return zkBasePath;
    }

    public void setZKBasePath(String zkBasePath) {
        this.zkBasePath = zkBasePath;
    }

}
