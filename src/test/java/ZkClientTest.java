import org.apache.zookeeper.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.concurrent.*;

import static org.apache.zookeeper.ZooKeeper.States.CONNECTED;

/**
 * ZookeeperClient zookeeper注册服务信息获取
 *
 * @author maple.lei
 * @date 2018/1/12 17:26
 */
public class ZkClientTest {

    private Map<String, ServiceWatcher> serviceWatcherMap = new ConcurrentHashMap<>(16);

    public static ZkClientTest getInstance() {
        return zkClientTest;
    }

    private final String parentPath = "/test/services";
    private final String childPath = "";

    private static final Logger LOGGER = LoggerFactory.getLogger(ZkClientTest.class);
    private ZooKeeper zk;
    private String zkHost;
    private static ZkClientTest zkClientTest = new ZkClientTest("127.0.0.1:2181");

    private ZkClientTest(String host) {
        this.zkHost = host;
        init();
    }

    private synchronized void init() {
        connect(null, null);
        LOGGER.info("wait for lock");
    }


    private synchronized void reset() {
        connect(null, null);
    }


    private synchronized void destroy() {
        try {
            if (zk != null) {
                zk.close();
            }
        } catch (InterruptedException e) {
            LOGGER.error(e.getMessage(), e);
        }
    }

    public synchronized void disconnect() {
        try {
            if (zk != null) {
                zk.close();
            }
        } catch (InterruptedException e) {
            LOGGER.error(e.getMessage(), e);
        }

        LOGGER.info("关闭当前zk连接");
    }


    /**
     * 获取zookeeper中的services节点的子节点，并设置监听器
     * <p>
     * 取消每次都重置所有服务信息，采用 增量 和 减量 形式
     *
     * @return
     * @author maple.lei
     */
    private synchronized void getServersList() {
        try {
            List<String> children = zk.getChildren(parentPath, watchedEvent -> {
                //Children发生变化，则重新获取最新的services列表
                if (watchedEvent.getType() == ServiceWatcher.Event.EventType.NodeChildrenChanged) {
                    LOGGER.info("{}子节点发生变化，重新获取子节点...", watchedEvent.getPath());
                    getServersList();
                }
            });

            //线程池并行操作
//            ExecutorService executorService = Executors.newFixedThreadPool(1);
            LOGGER.info("获取所有runtime下面的节点信息，开始解析服务元信息,处理线程数量 {}", 2);

            long beginTime = System.currentTimeMillis();
            children.forEach(serviceName -> {
                /*  executorService.execute(() -> {*/
                LOGGER.info("<-------> 子线程解析服务:{} 元数据信息", serviceName);

                getServiceInfoByServiceName(serviceName);
//                });
            });
//            executorService.shutdown();
//            executorService.awaitTermination(1, TimeUnit.HOURS);
            //主线程继续
            LOGGER.info("<<<<<<<<<< 子线程解析服务元数据结束,耗时:{} ms.  主线程继续执行 >>>>>>>>>>", (System.currentTimeMillis() - beginTime));
        } catch (KeeperException | InterruptedException e) {
            LOGGER.error(e.getMessage(), e);
        }
    }

    /**
     * 根据serviceName节点的路径，获取下面的子节点，并监听子节点变化
     *
     * @param serviceName
     */
    public void getServiceInfoByServiceName(String serviceName) {
        String servicePath = parentPath + "/" + serviceName;
        try {
            if (zk == null) {
                init();
            }

            /*List<String> children = zk.getChildren(servicePath, watchedEvent -> {
                if (watchedEvent.getType() == ServiceWatcher.Event.EventType.NodeChildrenChanged) {
                    LOGGER.info("{} service 子节点发生变化，重新获取信息", watchedEvent.getPath());
                    getServiceInfoByServiceName(serviceName);
                }
            });*/
            ServiceWatcher serviceWatcher = serviceWatcherMap.get(servicePath);
            if (serviceWatcher == null) {
                serviceWatcher = new ServiceWatcher(serviceName);
                serviceWatcherMap.putIfAbsent(servicePath, serviceWatcher);
            }

            List<String> children = zk.getChildren(servicePath, serviceWatcher);

            if (children.size() == 0) {
                LOGGER.info("{} 节点下面没有serviceInfo 信息，当前服务没有运行实例...", servicePath);
            } else {
                LOGGER.info("获取 {} 的子节点成功, childPath 服务 {} 元数据信息结束", servicePath, serviceName);
            }
        } catch (KeeperException | InterruptedException e) {
            LOGGER.error(e.getMessage(), e);
        }
    }

    /**
     * 连接zookeeper
     * <p>
     * 需要加锁
     */
    private synchronized void connect(String caseParams, Object o) {
        try {
            if (zk != null && zk.getState() == CONNECTED) {
                return;
            }

            CountDownLatch semaphore = new CountDownLatch(1);

            zk = new ZooKeeper(zkHost, 15000, e -> {

                switch (e.getState()) {
                    case Expired:
                        LOGGER.info("zookeeper ServiceWatcher 到zookeeper Server的session过期，重连");
                        break;

                    case SyncConnected:
                        semaphore.countDown();
                        LOGGER.info("Zookeeper 已连接, Zookeeper host: {}", zkHost);
                        break;

                    case Disconnected:
                        LOGGER.info("Zookeeper ServiceWatcher 连接不上了");
                        break;

                    case AuthFailed:
                        LOGGER.info("Zookeeper connection auth failed ...");
                        destroy();
                        break;

                    default:
                        break;
                }

            });
            semaphore.await();

        } catch (Exception e) {
            LOGGER.info(e.getMessage(), e);
        }
    }

    public static void main(String[] args) throws InterruptedException {
        ZkClientTest instance = ZkClientTest.getInstance();

        instance.getServersList();

        Thread.sleep(Long.MAX_VALUE);
    }

}
