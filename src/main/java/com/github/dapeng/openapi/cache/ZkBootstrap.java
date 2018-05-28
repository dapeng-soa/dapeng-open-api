package com.github.dapeng.openapi.cache;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Set;

/**
 * Desc: zk client 初始化
 *
 * @author: maple
 * @Date: 20180112 17:33
 */
public class ZkBootstrap {
    private static final Logger LOGGER = LoggerFactory.getLogger(ZkBootstrap.class);

    private ZookeeperClient zookeeperWatcher;
    private static final String ZK_HOST_VAR = "soa.zookeeper.host";
    private static final String DEFAULT_ZK_HOST = "127.0.0.1:2181";

    public void init() {
        String zkHost = prepareEnv();
        zookeeperWatcher = new ZookeeperClient(zkHost);
        zookeeperWatcher.init();
    }

    /**
     * 指定元信息获取接口
     */
    public void filterInit(Set<String> paths) {
        String zkHost = prepareEnv();
        zookeeperWatcher = new ZookeeperClient(zkHost);
        zookeeperWatcher.filterInit(paths);
    }

    /**
     * 过滤元数据并加载白名单
     *
     * @param services
     */
    public void filterInitWhiteList(Set<String> services) {
        String zkHost = prepareEnv();
        zookeeperWatcher = new ZookeeperClient(zkHost);
        zookeeperWatcher.filterInitWhiteList(services);
    }


    private String prepareEnv() {
        String zkHost = System.getenv(ZK_HOST_VAR.replace('.', '_'));
        if (zkHost == null) {
            zkHost = System.getProperty(ZK_HOST_VAR);
        }
        if (zkHost == null) {
            zkHost = DEFAULT_ZK_HOST;
            LOGGER.error("zk host not found. use default zkHost: {}", DEFAULT_ZK_HOST);
        } else {
            LOGGER.info("zkHost:" + zkHost);
        }
        return zkHost;
    }
}
