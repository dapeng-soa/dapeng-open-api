package com.github.dapeng.openapi.cache;

import com.github.dapeng.openapi.utils.EnvUtil;
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

    private ZookeeperClient zookeeperClient;

    /**
     * open api 启动，加载 urlMapping
     */
    public void openApiInit() {
        String zkHost = EnvUtil.prepareEnv();
        zookeeperClient = new ZookeeperClient(zkHost);
        zookeeperClient.init(true);
    }


    public void init() {
        String zkHost = EnvUtil.prepareEnv();
        zookeeperClient = new ZookeeperClient(zkHost);
        zookeeperClient.init(false);
    }

    /**
     * 指定元信息获取接口
     */
    public void filterInit(Set<String> paths) {
        String zkHost = EnvUtil.prepareEnv();
        zookeeperClient = new ZookeeperClient(zkHost);
        zookeeperClient.filterInit(paths);
    }

    /**
     * 过滤元数据并加载白名单
     *
     * @param services
     */
    public void filterInitWhiteList(Set<String> services) {
        String zkHost = EnvUtil.prepareEnv();
        zookeeperClient = new ZookeeperClient(zkHost);
        zookeeperClient.filterInitWhiteList(services);
    }
}
