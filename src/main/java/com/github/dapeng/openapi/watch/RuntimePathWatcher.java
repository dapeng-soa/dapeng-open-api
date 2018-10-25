package com.github.dapeng.openapi.watch;

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
public class RuntimePathWatcher implements Watcher {
    private static final Logger LOGGER = LoggerFactory.getLogger(RuntimePathWatcher.class);

    private final boolean needLoadUrl;

    public RuntimePathWatcher(boolean needLoadUrl) {
        this.needLoadUrl = needLoadUrl;
    }

    @Override
    public void process(WatchedEvent event) {
        if (event.getType() == Watcher.Event.EventType.NodeChildrenChanged) {
            //Children发生变化，则重新获取最新的services列表
            if (event.getType() == Watcher.Event.EventType.NodeChildrenChanged) {
                LOGGER.info("RuntimePathWatcher::services的子节点发生变化, 重新获取子节点。具体path: {}", event.getPath());
                ZookeeperClient zkClient = ZookeeperClient.getCurrInstance(EnvUtil.prepareEnv());
                zkClient.getServersList(needLoadUrl);
            }
        }
    }
}
