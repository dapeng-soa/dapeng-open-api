package com.github.dapeng.openapi.cache;

import com.github.dapeng.openapi.utils.Constants;
import com.github.dapeng.registry.ServiceInfo;
import com.github.dapeng.registry.zookeeper.ZkUtils;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooKeeper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.concurrent.*;
import java.util.stream.Collectors;

import static org.apache.zookeeper.ZooKeeper.States.CONNECTED;

/**
 * ZookeeperClient zookeeper注册服务信息获取
 *
 * @author maple.lei
 * @date 2018/1/12 17:26
 */
public class ZookeeperClient implements Watcher {
    private static final Logger LOGGER = LoggerFactory.getLogger(ZookeeperClient.class);

    private final static Map<String, List<ServiceInfo>> caches = new ConcurrentHashMap<>();

    private final String zookeeperHost;

    private ZooKeeper zk;

    protected boolean needLoadUrl = false;

    private static Set<String> whitelist = Collections.synchronizedSet(new HashSet<>());

    ZookeeperClient(final String zookeeperHost) {
        this.zookeeperHost = zookeeperHost;
    }

    public synchronized void init(boolean needLoadUrl) {
        this.needLoadUrl = needLoadUrl;
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


        caches.clear();
        LOGGER.info("关闭连接，清空service info caches");
    }

    public synchronized void disconnect() {
        try {
            if (zk != null) {
                zk.close();
            }
        } catch (InterruptedException e) {
            LOGGER.error(e.getMessage(), e);
        }

        zk = null;

        LOGGER.info("关闭当前zk连接");
    }

    public static Map<String, List<ServiceInfo>> getServices() {
        return caches;
    }

    public static Set<String> getWhitelist() {
        return whitelist;
    }

    /**
     * 针对指定的从服务过滤，只获取指定的服务元信息
     */
    public synchronized void filterInit(Set<String> paths) {
        whitelist.addAll(paths);
        connect(null, null);
        LOGGER.info("wait for lock");
    }

    /**
     * 注册白名单列表,并获取指定的服务元数据
     *
     * @param services 用于过滤的服务列表
     */
    public synchronized void filterInitWhiteList(Set<String> services) {
        whitelist.addAll(services);
        connect(Constants.SERVICE_WITHELIST_PATH, services);
        LOGGER.info("api-gate-way service load successful");
    }


    @Override
    public void process(WatchedEvent event) {
        LOGGER.warn("ZookeeperClient::process zkEvent: " + event);
        if (event.getPath() == null) {
            LOGGER.warn("ZookeeperClient::process just ignore this event: " + event);
            return;
        }
        switch (event.getType()) {
            case NodeChildrenChanged:
                if (event.getPath().equals(Constants.SERVICE_WITHELIST_PATH)) {
                    LOGGER.info("[{}] 服务白名单发生变化，重新获取...", event.getPath());
                    whitelist.clear();
                    syncWhiteList();
                } else if (event.getPath().equals(Constants.SERVICE_RUNTIME_PATH)) {
                    LOGGER.info("ZookeeperClient::process 服务runtime子节点发生变化, 重新获取子节点");
                    filterServersList();
                } else if (event.getPath().startsWith(Constants.SERVICE_RUNTIME_PATH + "/")) {
                    LOGGER.info("ZookeeperClient::process 服务path: " + event.getPath() + " 的子节点发生变化，重新获取信息");
                    String serviceName = event.getPath().substring(event.getPath().lastIndexOf('/') + 1);
                    syncServiceRuntimeInfo(serviceName);
                }
                break;
            default:
                LOGGER.info("just ignore");
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
                LOGGER.warn("ZookeeperClient::connect zkEvent: " + e);
                switch (e.getState()) {
                    case Expired:
                        LOGGER.info("ZookeeperClient::connect zookeeper Watcher 到zookeeper Server的session过期，重连");
                        disconnect();
                        connect(caseParams, o);
                        break;

                    case SyncConnected:
                        LOGGER.info("ZookeeperClient::connect Zookeeper Watcher 已连接 zookeeper Server,Zookeeper host: {}", zookeeperHost);
                        if (null != caseParams) {
                            switch (caseParams) {
                                case Constants.SERVICE_WITHELIST_PATH:
                                    if (null != o) this.registerServiceWhiteList((Set<String>) o);
                                    break;
                                default:
                                    break;
                            }
                        } else {
                            filterServersList();
                        }
                        semaphore.countDown();
                        break;

                    case Disconnected:
                        LOGGER.info("Zookeeper Watcher 连接不上了");
                        // zk断了之后, 会自动重连, 一般不需要对该事件做处理
                        // 但是zk服务端如果重建的话，自动重连是永远都连不上的。这时候需要重建zk客户端
                        disconnect();
                        connect(caseParams, o);
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

    /**
     * 根据serviceName节点的路径，获取下面的子节点，并监听子节点变化
     *
     * @param
     */
    private void syncServiceRuntimeInfo(String serviceName) {
        String servicePath = Constants.SERVICE_RUNTIME_PATH + "/" + serviceName;
        try {
            if (zk == null) {
                init(needLoadUrl);
            }

            List<String> children = zk.getChildren(servicePath, this);

            if (children.size() == 0) {
                //移除这个没有运行服务的相关信息...
                ServiceCache.removeServiceCache(servicePath, needLoadUrl);
                LOGGER.info("{} 节点下面没有serviceInfo 信息，当前服务没有运行实例...", servicePath);
            } else {
                LOGGER.info("获取{}的子节点成功", servicePath);
                resetServiceInfoByName(serviceName, servicePath, children, caches);
                ServiceCache.loadServicesMetadata(serviceName, caches.get(serviceName), needLoadUrl);
                LOGGER.info("syncServiceRuntimeInfo 解析服务 {} 元数据信息结束", serviceName);
            }
        } catch (KeeperException | InterruptedException e) {
            LOGGER.error(e.getMessage(), e);
        }

    }

    /**
     * 只获取指定的元数据信息
     */
    private void filterServersList() {
        caches.clear();
        try {
            List<String> children = zk.getChildren(Constants.SERVICE_RUNTIME_PATH, this);

            List<String> result = whitelist.isEmpty() ? children : children.stream().filter(whitelist::contains).collect(Collectors.toList());
            LOGGER.info("[filter service]:过滤元数据信息结果:" + result.toString());

            //线程池并行操作
            int processor = Runtime.getRuntime().availableProcessors() >= 4 ? Runtime.getRuntime().availableProcessors() : 4;
            ExecutorService executorService = Executors.newFixedThreadPool(processor);
            LOGGER.info("获取所有runtime下面的节点信息，开始解析服务元信息,处理线程数量 {}", processor);

            long beginTime = System.currentTimeMillis();
            result.forEach(serviceName -> {
                executorService.execute(() -> {
                    LOGGER.info("子线程开始解析服务:{} 元数据信息", serviceName);
                    syncServiceRuntimeInfo(serviceName);
                });
            });
            executorService.shutdown();
            executorService.awaitTermination(1, TimeUnit.HOURS);
            //主线程继续
            LOGGER.info("<<<<<<<<<< 子线程解析服务元数据结束,耗时:{} ms.  主线程继续执行 >>>>>>>>>>", (System.currentTimeMillis() - beginTime));
        } catch (KeeperException.NoNodeException e) {
            ZkUtils.createPersistNodeOnly(Constants.SERVICE_RUNTIME_PATH, zk);
            filterServersList();
        } catch (KeeperException | InterruptedException e) {
            LOGGER.error(e.getMessage(), e);
        }
    }

    /**
     * 注册服务白名单到zookeeper
     *
     * @param services
     */
    private void registerServiceWhiteList(Set<String> services) {
        if (null != services) {
            services.forEach(s -> {
                ZkUtils.createPersistNodeOnly(Constants.SERVICE_WITHELIST_PATH + "/" + s, zk);
            });
            whitelist.addAll(services);
            syncWhiteList();
        }
    }


    /**
     * watch 白名单节点变化
     */
    private void syncWhiteList() {

        try {
            List<String> children = zk.getChildren(Constants.SERVICE_WITHELIST_PATH, this);
            whitelist.addAll(children);
            filterServersList();
            LOGGER.info("当前白名单个数:[{}]", whitelist.size());
            LOGGER.info(">>>>>>>>>>>>>>>>>>");
            StringBuilder sb = new StringBuilder(256);
            whitelist.forEach(w -> {
                sb.append(w).append("\r");
            });
            LOGGER.info(sb.toString());
            LOGGER.info(">>>>>>>>>>>>>>>>>>");
        } catch (Exception e) {
            LOGGER.error("获取服务白名单失败");
        }

    }

    /**
     * serviceName下子节点列表即可用服务地址列表
     * 子节点命名为：host:port:versionName
     *
     * @param serviceName
     * @param path
     * @param infos
     */
    private void resetServiceInfoByName(String serviceName, String path, List<String> infos, Map<String, List<ServiceInfo>> caches) {
        LOGGER.info(serviceName + "   " + infos);
        List<ServiceInfo> sinfos = new ArrayList<>();

        for (String info : infos) {
            String[] serviceInfo = info.split(":");
            ServiceInfo sinfo = new ServiceInfo(serviceInfo[0], Integer.valueOf(serviceInfo[1]), serviceInfo[2]);
            sinfos.add(sinfo);
        }

        if (caches.containsKey(serviceName)) {
            List<ServiceInfo> currentInfos = caches.get(serviceName);

            for (ServiceInfo sinfo : sinfos) {
                for (ServiceInfo currentSinfo : currentInfos) {
                    if (sinfo.equalTo(currentSinfo)) {
                        sinfo.setActiveCount(currentSinfo.getActiveCount());
                        break;
                    }
                }
            }
        }
        caches.put(serviceName, sinfos);
    }
}
