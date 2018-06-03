package com.github.dapeng.openapi.utils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author with struy.
 * Create by 2018/6/3 18:00
 * email :yq1724555319@gmail.com
 */

public class EnvUtil {
    private static final Logger LOGGER = LoggerFactory.getLogger(EnvUtil.class);
    private static final String ZK_HOST_VAR = "soa.zookeeper.host";
    private static final String DEFAULT_ZK_HOST = "127.0.0.1:2181";

    public static String prepareEnv() {
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
