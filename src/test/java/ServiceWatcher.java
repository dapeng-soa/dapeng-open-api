import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author <a href=mailto:leihuazhe@gmail.com>maple</a>
 * @since 2018-10-25 4:38 PM
 */
public class ServiceWatcher implements Watcher {
    private static final Logger LOGGER = LoggerFactory.getLogger(ServiceWatcher.class);
    private String serviceName;

    public ServiceWatcher(String serviceName) {
        this.serviceName = serviceName;
    }

    @Override
    public void process(WatchedEvent event) {
        if (event.getType() == ServiceWatcher.Event.EventType.NodeChildrenChanged) {
            LOGGER.info("{} service 子节点发生变化，重新获取信息", event.getPath());
            ZkClientTest.getInstance().getServiceInfoByServiceName(serviceName);
        }
    }
}
