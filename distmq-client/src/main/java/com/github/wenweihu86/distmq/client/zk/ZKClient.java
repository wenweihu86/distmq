package com.github.wenweihu86.distmq.client.zk;

import org.apache.curator.RetryPolicy;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.framework.api.CuratorWatcher;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Iterator;
import java.util.List;
import java.util.Map;

/**
 * Created by wenweihu86 on 2017/6/21.
 */
public class ZKClient {
    private static final Logger LOG = LoggerFactory.getLogger(ZKClient.class);
    private ZKConf zkConf;
    private CuratorFramework zkClient;

    public ZKClient(ZKConf conf) {
        this.zkConf = conf;
        RetryPolicy retryPolicy = new ExponentialBackoffRetry(
                zkConf.getRetryIntervalMs(), zkConf.getRetryCount());
        this.zkClient = CuratorFrameworkFactory.builder()
                .connectString(zkConf.getServers())
                .retryPolicy(retryPolicy)
                .connectionTimeoutMs(zkConf.getConnectTimeoutMs())
                .sessionTimeoutMs(zkConf.getSessionTimeoutMs())
                .build();
        this.zkClient.start();
    }

    public void registerBroker(int shardingId, String ip, int port) {
        String path = String.format("%s/brokers/%d/%s:%d",
                zkConf.getBasePath(), shardingId, ip, port);
        try {
            zkClient.create()
                    .creatingParentsIfNeeded()
                    .withMode(CreateMode.EPHEMERAL)
                    .forPath(path, "".getBytes());
        } catch (Exception ex) {
            LOG.warn("registerBroker exception:", ex);
        }
    }

    public void subscribeBroker() {
        final ZKData zkData = ZKData.getInstance();
        final Map<Integer, List<String>> brokerMap = zkData.getBrokerMap();
        try {
            final String brokerParentPath = zkConf.getBasePath() + "/brokers";
            List<String> shardings = zkClient.getChildren().forPath(brokerParentPath);
            for (String sharding : shardings) {
                final int shardingId = Integer.valueOf(sharding);
                final String shardingPath = brokerParentPath + "/" + sharding;
                List<String> brokerAddressList = zkClient.getChildren().forPath(shardingPath);
                brokerMap.put(shardingId, brokerAddressList);
                zkClient.getChildren().usingWatcher(
                        new BrokerShardingChildrenWather(shardingId))
                        .forPath(shardingPath);
            }
            zkClient.getChildren().usingWatcher(new BrokerChildrenWatcher()).forPath(brokerParentPath);
        } catch (Exception ex) {
            LOG.warn("subscribeBroker exception:", ex);
        }
    }

    public void createTopic(String topic, int queueNum) {
    }

    private class BrokerShardingChildrenWather implements CuratorWatcher {
        private int shardingId;

        public BrokerShardingChildrenWather(int shardingId) {
            this.shardingId = shardingId;
        }

        @Override
        public void process(WatchedEvent event) throws Exception {
            ZKData zkData = ZKData.getInstance();
            Map<Integer, List<String>> brokerMap = zkData.getBrokerMap();
            if (event.getType() == Watcher.Event.EventType.NodeChildrenChanged) {
                String shardingPath = zkConf.getBasePath() + "/brokers/" + shardingId;
                List<String> newBrokerAddressList = zkClient.getChildren().forPath(shardingPath);
                // TODO: 对于client需要关闭被删除节点的连接，以及新建新增节点连接
                brokerMap.put(shardingId, newBrokerAddressList);
            }
        }
    }

    private class BrokerChildrenWatcher implements CuratorWatcher {
        @Override
        public void process(WatchedEvent event) throws Exception {
            ZKData zkData = ZKData.getInstance();
            Map<Integer, List<String>> brokerMap = zkData.getBrokerMap();
            if (event.getType() == Watcher.Event.EventType.NodeChildrenChanged) {
                String brokerPath = zkConf.getBasePath() + "/brokers";
                List<String> newShardings = zkClient.getChildren().forPath(brokerPath);
                Iterator<Map.Entry<Integer, List<String>>> iterator = brokerMap.entrySet().iterator();
                while (iterator.hasNext()){
                    Map.Entry<Integer, List<String>> entry = iterator.next();
                    if (!newShardings.contains(Integer.valueOf(entry.getKey()))) {
                        // TODO:对于client，需要删除对应节点的连接
                        iterator.remove();
                    }
                }
                for (String sharding : newShardings) {
                    int shardingId = Integer.valueOf(sharding);
                    if (!brokerMap.containsKey(shardingId)) {
                        String shardingPath = brokerPath + "/" + sharding;
                        zkClient.getChildren().usingWatcher(
                                new BrokerShardingChildrenWather(shardingId))
                                .forPath(shardingPath);
                    }
                }
            }
        }
    }

}
