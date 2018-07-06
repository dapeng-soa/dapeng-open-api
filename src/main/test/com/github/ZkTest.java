package com.github;

import com.github.dapeng.openapi.cache.ServiceCache;
import com.github.dapeng.openapi.cache.ZkBootstrap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.stream.Collectors;

/**
 * zk test
 *
 * @author huyj
 * @Created 2018/5/11 9:46
 */
public class ZkTest {
    private static ZkBootstrap zkBootstrap = null;
    private static final Logger logger = LoggerFactory.getLogger(ZkTest.class);
    private static final String KEY_SOA_ZOO_KEEPER_HOST = "soa.zookeeper.host";

    public static void main(String[] arg0) {
        System.setProperty(KEY_SOA_ZOO_KEEPER_HOST, "192.168.4.154:2181");
        if (zkBootstrap == null) {
            zkBootstrap = new ZkBootstrap();
            zkBootstrap.init();
        }


        /*while (ServiceCache.getServices().size() < 33) {
            System.out.println("wait for zk sync " + ServiceCache.getServices().size());
            try {
                Thread.sleep(1000 * 2);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }*/

        List<String> services = ServiceCache.getServices().entrySet().stream().map(i -> i.getValue().getNamespace() + "." + i.getKey()).collect(Collectors.toList());
        logger.info("[main] ==>services=[{}}", services);

        resetZk();
        List<String> services1 = ServiceCache.getServices().entrySet().stream().map(i -> i.getValue().getNamespace() + "." + i.getKey()).collect(Collectors.toList());
        logger.info("[main] ==> reset services=[{}}", services1);

    }


    private static void resetZk() {
        System.setProperty(KEY_SOA_ZOO_KEEPER_HOST, "10.10.10.45:2181");
        ServiceCache.resetCache();
        try {
            /*******先连接连接 zkBootstrap*************************************************/
            if (zkBootstrap != null) {
                zkBootstrap = null;
            }
            zkBootstrap = new ZkBootstrap();
            zkBootstrap.init();
        } catch (Exception e) {
            System.out.println(" Failed to wait for zookeeper init..");
        }
    }
}
