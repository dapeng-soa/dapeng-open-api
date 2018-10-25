package com.github.dapeng.openapi.watcher;

import com.github.dapeng.openapi.cache.ZookeeperClient;
import com.github.dapeng.openapi.utils.EnvUtil;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author <a href=mailto:leihuazhe@gmail.com>maple</a>
 * @since 2018-10-25 4:51 PM
 */
public class ServicesWatcher implements Watcher {
    private static final Logger LOGGER = LoggerFactory.getLogger(ServicesWatcher.class);

    private final String serviceName;
    private final boolean needLoadUrl;

    public ServicesWatcher(String serviceName, boolean needLoadUrl) {
        this.serviceName = serviceName;
        this.needLoadUrl = needLoadUrl;
    }

    @Override
    public void process(WatchedEvent event) {
        if (event.getType() == Watcher.Event.EventType.NodeChildrenChanged) {
            LOGGER.info("ServicesWatcher watch 服务path: {} 的子节点发生变化，重新获取信息", event.getPath());
            ZookeeperClient zkClient = ZookeeperClient.getCurrInstance(EnvUtil.prepareEnv());
            zkClient.getServiceInfoByServiceName(serviceName, needLoadUrl);
        }
    }
}
