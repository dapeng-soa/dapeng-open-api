package com.github.dapeng.openapi.cache;


import com.github.dapeng.core.SoaException;
import com.github.dapeng.core.metadata.*;
import com.github.dapeng.metadata.*;
import com.github.dapeng.registry.ServiceInfo;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.xml.bind.JAXB;
import java.io.StringReader;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;

/**
 * Service Cache
 *
 * @author maple.lei
 * @date 2018/1/12 17:26
 */
@SuppressWarnings("AlibabaUndefineMagicConstant")
public class ServiceCache {

    private static final Logger LOGGER = LoggerFactory.getLogger(ServiceCache.class);

    private static Map<String, Service> services = Collections.synchronizedMap(new TreeMap<>());

    private static Map<String, ServiceInfo> serverInfoMap = Collections.synchronizedMap(new TreeMap<>());

    private static Map<String, Service> fullNameService = Collections.synchronizedMap(new TreeMap<>());

    public static Map<String, String> urlMappings = new ConcurrentHashMap<>();


    public static void resetCache() {
        services.clear();
        fullNameService.clear();
        urlMappings.clear();
        serverInfoMap.clear();
    }

    public static void removeServiceCache(String servicePath) {
        String serviceName = servicePath.substring(servicePath.lastIndexOf(".") + 1);
        removeByServiceKey(serviceName, services);
        removeByServiceKey(serviceName, fullNameService);
        removeByServiceKeyValue(serviceName, urlMappings);
        removeByServiceKey(serviceName, serverInfoMap);
    }

    /**
     * 根据服务简名，移除掉每一个map里的与之相关的服务信息
     *
     * @param serviceName 模糊  HelloService
     * @param map
     */
    public static <T> void removeByServiceKey(String serviceName, Map<String, T> map) {
        try {
            if (map.size() == 0) {
                throw new RuntimeException("map 为空，不需要进行移除");
            }

            LOGGER.info("移除不可用实例信息value {}，移除前，map size： {}", map.values(), map.size());
            Iterator<String> it = map.keySet().iterator();
            while (it.hasNext()) {
                String serviceKey = it.next();
                if (serviceKey.contains(serviceName)) {
                    it.remove();
                    LOGGER.info("移除不可用实例信息key {}", serviceKey);
                }
            }

            LOGGER.info("移除不可用实例信息，移除后，map size： {}", map.size());
        } catch (RuntimeException e) {
            LOGGER.info(e.getMessage(), "map size 为 0 ，不需要进行移除 ...");
        }

    }

    /**
     * 根据服务简名，移除掉每一个map里的与之相关的服务信息
     *
     * @param serviceName 模糊  HelloService
     * @param map
     */
    public static <T> void removeByServiceKeyValue(String serviceName, Map<String, T> map) {
        try {
            if (map.size() == 0) {
                throw new RuntimeException("map 为空，不需要进行移除");
            }

            LOGGER.info("移除不可用实例信息value {}，移除前，map size： {}", map.values(), map.size());
            Iterator<String> it = map.keySet().iterator();
            while (it.hasNext()) {
                String serviceValue = (String) map.get(it.next());
                if (serviceValue.contains(serviceName)) {
                    it.remove();
                    LOGGER.info("移除不可用实例信息 struct {}", serviceValue.toString());
                }
            }

            LOGGER.info("移除不可用实例信息，移除后，map size： {}", map.size());
        } catch (RuntimeException e) {
            LOGGER.info(e.getMessage(), "map size 为 0 ，不需要进行移除 ...");
        }

    }

    public static void loadServicesMetadata(String serviceName, List<ServiceInfo> infos) {
        LOGGER.info("access loadServicesMetadata");
        Map<String, ServiceInfo> diffVersionServices = new HashMap<>();
        for (ServiceInfo info : infos) {
            diffVersionServices.put(info.versionName, info);
        }

        for (ServiceInfo info : diffVersionServices.values()) {
            String version = info.versionName;
            String metadata = "";
            int tryCount = 1;

            while (tryCount <= 3) {
                try {
                    TimeUnit.SECONDS.sleep(2);

                    MetadataClient metadataClient = new MetadataClient(serviceName, version);
                    metadata = metadataClient.getServiceMetadata();


                    if (metadata != null) {
                        try (StringReader reader = new StringReader(metadata)) {
                            Service serviceData = JAXB.unmarshal(reader, Service.class);
                            String serviceKey = getKey(serviceData);
                            String fullNameKey = getFullNameKey(serviceData);

                            services.put(serviceKey, serviceData);

                            serverInfoMap.put(fullNameKey, info);

                            LOGGER.info("----------------- service size :  " + services.size());

                            StringBuilder logBuilder = new StringBuilder();
                            services.forEach((k, v) -> logBuilder.append(k + ",  "));
                            LOGGER.info("zk 服务实例列表: {}", logBuilder);

                            fullNameService.put(fullNameKey, serviceData);

                            loadServiceUrl(serviceData);

                        } catch (Exception e) {
                            LOGGER.error("{}:{} metadata解析出错, {}", serviceName, version);
                            LOGGER.error(e.getMessage(), e);

                            LOGGER.info(metadata);
                        }
                    }

                    LOGGER.info("{}:{} metadata获取成功，尝试次数 {}", serviceName, version, tryCount);
                    break;

                } catch (SoaException e) {
                    LOGGER.error("{}:{} metadata获取出错,已尝试 {} 次", serviceName, version, tryCount);
                    LOGGER.error(e.getMessage(), e);
                    LOGGER.error("metadata获取出错", e);
                    tryCount++;

                } catch (Exception e) {
                    LOGGER.error("{}:{} 触发 Exception ,已尝试 {} 次", serviceName, version, tryCount);
                    LOGGER.error(e.getMessage(), e);
                    tryCount++;
                }

            }
        }

    }

    /**
     * 将service和service中的方法、结构体、枚举和字段名分别设置对应的url，以方便搜索
     *
     * @param service
     */
    private static void loadServiceUrl(Service service) {
        //将service和service中的方法、结构体、枚举和字段名分别设置对应的url，以方便搜索
        urlMappings.put(service.getName(), "api/service/" + service.name + "/" + service.meta.version + ".htm");
        List<Method> methods = service.getMethods();
        for (int i = 0; i < methods.size(); i++) {
            Method method = methods.get(i);
            urlMappings.put(method.name, "api/method/" + service.name + "/" + service.meta.version + "/" + method.name + ".htm");
        }

        List<Struct> structs = service.getStructDefinitions();
        for (int i = 0; i < structs.size(); i++) {
            Struct struct = structs.get(i);
            urlMappings.put(struct.name, "api/struct/" + service.name + "/" + service.meta.version + "/" + struct.namespace + "." + struct.name + ".htm");

            List<Field> fields = struct.getFields();
            for (int j = 0; j < fields.size(); j++) {
                Field field = fields.get(j);
                urlMappings.put(field.name, "api/struct/" + service.name + "/" + service.meta.version + "/" + struct.namespace + "." + struct.name + ".htm");
            }
        }

        List<TEnum> tEnums = service.getEnumDefinitions();
        for (int i = 0; i < tEnums.size(); i++) {
            TEnum tEnum = tEnums.get(i);
            urlMappings.put(tEnum.name, "api/enum/" + service.name + "/" + service.meta.version + "/" + tEnum.namespace + "." + tEnum.name + ".htm");
        }


    }


    public void destory() {
        services.clear();
    }


    public static Service getService(String name, String version) {

        if (name.contains(".")) {
            return fullNameService.get(getKey(name, version));
        } else {
            return services.get(getKey(name, version));
        }
    }

    private static String getKey(Service service) {
        return getKey(service.getName(), service.getMeta().version);
    }

    private static String getFullNameKey(Service service) {
        return getKey(service.getNamespace() + "." + service.getName(), service.getMeta().version);
    }

    private static String getKey(String name, String version) {
        return name + ":" + version;
    }

    public static Map<String, Service> getServices() {
        return services;
    }

    public static ServiceInfo getServerInfoMap(String name, String version) {
        return serverInfoMap.get(getKey(name, version));
    }
}
