package com.github.dapeng.openapi.cache;

import com.github.dapeng.openapi.utils.Constants;
import com.github.dapeng.registry.ServiceInfo;
import com.github.dapeng.registry.zookeeper.WatcherUtils;
import org.apache.zookeeper.*;
import org.apache.zookeeper.data.Stat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.stream.Collectors;

import static org.apache.zookeeper.ZooKeeper.States.CONNECTED;

/**
 * ZookeeperClient zookeeper注册服务信息获取
 *
 * @author maple.lei
 * @date 2018/1/12 17:26
 */
public class ZookeeperClient {

    private static final Logger LOGGER = LoggerFactory.getLogger(ZookeeperClient.class);

    private final static Map<String, List<ServiceInfo>> caches = new ConcurrentHashMap<>();

    private ExecutorService executorService = Executors.newFixedThreadPool(Runtime.getRuntime().availableProcessors());

    private String zookeeperHost;

    private ZooKeeper zk;

    private static ZookeeperClient zookeeperClient;

    private static Set<String> whitelist = Collections.synchronizedSet(new HashSet<>());

    private ZookeeperClient(String zookeeperHost) {
        this.zookeeperHost = zookeeperHost;
    }

    public synchronized void init() {
        connect(null, null);
        LOGGER.info("wait for lock");
        getServersList();
    }

    public static ZookeeperClient getCurrInstance(String zookeeperHost) {
        if (null == zookeeperClient) {
            zookeeperClient = new ZookeeperClient(zookeeperHost);
        }
        return zookeeperClient;
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


        caches.clear();
        LOGGER.info("关闭连接，清空service info caches");
    }

    public static Map<String, List<ServiceInfo>> getServices() {
        return caches;
    }

    public static Set<String> getWhitelist() {
        return whitelist;
    }


    /**
     * 获取zookeeper中的services节点的子节点，并设置监听器
     * <p>
     * 取消每次都重制所有服务信息，采用 增量 和 减量 形式
     *
     * @return
     * @author maple.lei
     */
    private void getServersList() {
        caches.clear();
        try {
            List<String> children = zk.getChildren(Constants.SERVICE_RUNTIME_PATH, watchedEvent -> {
                //Children发生变化，则重新获取最新的services列表
                if (watchedEvent.getType() == Watcher.Event.EventType.NodeChildrenChanged) {
                    LOGGER.info("{}子节点发生变化，重新获取子节点...", watchedEvent.getPath());
                    getServersList();
                }
            });

            children.forEach(serviceName -> getServiceInfoByServiceName(serviceName));

        } catch (KeeperException | InterruptedException e) {
            LOGGER.error(e.getMessage(), e);
        }
    }

    /**
     * 根据serviceName节点的路径，获取下面的子节点，并监听子节点变化
     *
     * @param serviceName
     */
    private void getServiceInfoByServiceName(String serviceName) {

        String servicePath = Constants.SERVICE_RUNTIME_PATH + "/" + serviceName;
        try {

            if (zk == null) {
                init();
            }

            List<String> children = zk.getChildren(servicePath, watchedEvent -> {
                if (watchedEvent.getType() == Watcher.Event.EventType.NodeChildrenChanged) {
                    LOGGER.info("{}子节点发生变化，重新获取信息", watchedEvent.getPath());
                    getServiceInfoByServiceName(serviceName);
//                    getServersList();
                }
            });

            if (children.size() == 0) {
                //移除这个没有运行服务的相关信息...
                ServiceCache.removeServiceCache(servicePath);
                LOGGER.info("{} 节点下面没有serviceInfo 信息，当前服务没有运行实例...", servicePath);
            } else {
                LOGGER.info("获取{}的子节点成功", servicePath);
                WatcherUtils.resetServiceInfoByName(serviceName, servicePath, children, caches);

                LOGGER.info("拿到服务 {} 地址，开始解析服务元信息,处理线程数量 {}", servicePath, Runtime.getRuntime().availableProcessors());
                executorService.execute(() -> {
                    LOGGER.info("开启线程开始解析元数据信息");
                    ServiceCache.loadServicesMetadata(serviceName, caches.get(serviceName));
                });

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

            zk = new ZooKeeper(zookeeperHost, 15000, e -> {

                switch (e.getState()) {
                    case Expired:
                        LOGGER.info("zookeeper Watcher 到zookeeper Server的session过期，重连");
//                        destroy();
                        reset();
                        break;

                    case SyncConnected:
                        semaphore.countDown();
                        LOGGER.info("Zookeeper Watcher 已连接 zookeeper Server,Zookeeper host: {}", zookeeperHost);
                        if (null != caseParams)
                            switch (caseParams) {
                                case Constants.SERVICE_WITHELIST_PATH:
                                    if (null != o) this.registerServiceWhiteList((Set<String>) o);
                                    break;
                                default:
                                    break;
                            }
                        break;

                    case Disconnected:
                        LOGGER.info("Zookeeper Watcher 连接不上了");
                        //zk断了之后, 会自动重连
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

    // ------------------------ ---- --- --- --- --- --- --- --- --- --- --- --- --

    /**
     * 针对指定的从服务过滤，只获取指定的服务元信息
     */
    public synchronized void filterInit(Set<String> paths) {
        connect(null, null);
        LOGGER.info("wait for lock");
        filterServersList(paths);
    }

    /**
     * 只获取指定的元数据信息
     *
     * @param childrenPath
     */
    private void filterServersList(Set<String> childrenPath) {
        caches.clear();
        try {
            List<String> children = zk.getChildren(Constants.SERVICE_RUNTIME_PATH, watchedEvent -> {
                //Children发生变化，则重新获取最新的services列表
                if (watchedEvent.getType() == Watcher.Event.EventType.NodeChildrenChanged) {
                    LOGGER.info("{}子节点发生变化，重新获取子节点...", watchedEvent.getPath());
                    filterServersList(childrenPath);
                }
            });

            List<String> result = children.stream().filter(path -> childrenPath.contains(path)).collect(Collectors.toList());
            LOGGER.info("[filter service]:过滤元数据信息结果:" + result.toString());
            result.forEach(serviceName -> getServiceByNameSync(serviceName));
        } catch (KeeperException | InterruptedException e) {
            LOGGER.error(e.getMessage(), e);
        }
    }

    /**
     * 同步解析 元数据信息,针对 kafka-agent 代理部分
     *
     * @param serviceName
     */
    private void getServiceByNameSync(String serviceName) {
        String servicePath = Constants.SERVICE_RUNTIME_PATH + "/" + serviceName;
        try {
            if (zk == null) {
                init();
            }
            List<String> children = zk.getChildren(servicePath, watchedEvent -> {
                if (watchedEvent.getType() == Watcher.Event.EventType.NodeChildrenChanged) {
                    LOGGER.info("{}子节点发生变化，重新获取信息", watchedEvent.getPath());
                    getServiceInfoByServiceName(serviceName);
//                    getServersList();
                }
            });

            if (children.size() == 0) {
                //移除这个没有运行服务的相关信息...
                ServiceCache.removeServiceCache(servicePath);
                LOGGER.info("{} 节点下面没有serviceInfo 信息，当前服务没有运行实例...", servicePath);
            } else {
                LOGGER.info("获取{}的子节点成功", servicePath);
                WatcherUtils.resetServiceInfoByName(serviceName, servicePath, children, caches);

                LOGGER.info("拿到服务 {} 地址，开始解析服务元信息", servicePath);
                ServiceCache.loadServicesMetadata(serviceName, caches.get(serviceName));

            }
        } catch (KeeperException | InterruptedException e) {
            LOGGER.error(e.getMessage(), e);
        }
    }

    //=======针对服务白名单=======================================//

    /**
     * 注册白名单列表,并获取指定的服务元数据
     *
     * @param services 用于过滤的服务列表
     */
    public synchronized void filterInitWhiteList(Set<String> services) {
        connect(Constants.SERVICE_WITHELIST_PATH, services);
        LOGGER.info("wait for lock");
        filterServersList(services);
    }

    /**
     * 注册服务白名单到zookeeper
     *
     * @param services
     */
    private synchronized void registerServiceWhiteList(Set<String> services) {
        if (null != services) {
            services.forEach(s -> {
                create(Constants.SERVICE_WITHELIST_PATH + "/" + s, false);
            });
            whitelist.addAll(services);
            watchInstanceChange();
        }
    }

    /**
     * @param path
     * @param ephemeral
     */
    private void create(String path, boolean ephemeral) {

        int i = path.lastIndexOf("/");
        if (i > 0) {
            String parentPath = path.substring(0, i);
            //判断父节点是否存在...
            if (!checkExists(parentPath)) {
                create(parentPath, false);
            }
        }
        if (ephemeral) {
            // 创建临时节点
        } else {
            createPersistent(path, "");
        }
    }

    /**
     * 异步添加持久化的节点
     *
     * @param path
     * @param data
     */
    private void createPersistent(String path, String data) {
        Stat stat = exists(path);

        if (stat == null) {
            zk.create(path, data.getBytes(), ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT, persistNodeCreateCallback, data);
        }
    }

    private Stat exists(String path) {
        Stat stat = null;
        try {
            stat = zk.exists(path, false);
        } catch (KeeperException | InterruptedException e) {
        }
        return stat;
    }

    /**
     * 检查节点是否存在
     */
    private boolean checkExists(String path) {
        try {
            Stat exists = zk.exists(path, false);
            if (exists != null) {
                return true;
            }
            return false;
        } catch (Throwable t) {
        }
        return false;
    }

    /**
     * 异步添加持久化节点回调方法
     */
    private AsyncCallback.StringCallback persistNodeCreateCallback = (rc, path, ctx, name) -> {
        switch (KeeperException.Code.get(rc)) {
            case CONNECTIONLOSS:
                LOGGER.info("创建节点:{},连接断开，重新创建", path);
                createPersistent(path, (String) ctx);
                break;
            case OK:
                LOGGER.info("创建节点:{},成功", path);
                // 添加watcher
                if (path.equals(Constants.SERVICE_WITHELIST_PATH)) {
                    watchInstanceChange();
                }
                break;
            case NODEEXISTS:
                LOGGER.info("创建节点:{},已存在", path);
                break;
            default:
                LOGGER.info("创建节点:{},失败", path);
        }
    };


    /**
     * watch 白名单节点变化
     */
    private void watchInstanceChange() {

        try {
            List<String> children = zk.getChildren(Constants.SERVICE_WITHELIST_PATH, event -> {
                //Children发生变化，则重新获取最新的services列表
                if (event.getType() == Watcher.Event.EventType.NodeChildrenChanged) {
                    LOGGER.info("[{}] 服务白名单发生变化，重新获取...", event.getPath());
                    whitelist.clear();
                    watchInstanceChange();
                }
            });
            whitelist.addAll(children);
            filterServersList(whitelist);
            LOGGER.info("当前白名单个数:[{}]", whitelist.size());
            LOGGER.info(">>>>>>>>>>>>>>>>>>");
            whitelist.forEach(w -> {
                LOGGER.info(w);
            });
            LOGGER.info(">>>>>>>>>>>>>>>>>>");
        } catch (Exception e) {
            LOGGER.error("获取服务白名单失败");
        }

    }

    //==============================================添加节点data

    public synchronized void createData(String path, String data) {
        if (zk == null) {
            connect(null, null);
        }
        create(path, false);
        if (checkExists(path)) {
            LOGGER.info(" start to set data from: " + path);
            zk.setData(path, data.getBytes(), -1, null, data);
        }
    }

}
