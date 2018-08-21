package com.github.dapeng.openapi.utils;

import com.github.dapeng.core.*;
import com.github.dapeng.core.helper.DapengUtil;
import com.github.dapeng.core.helper.SoaSystemEnvProperties;
import com.github.dapeng.core.metadata.Method;
import com.github.dapeng.core.metadata.Service;
import com.github.dapeng.json.JsonSerializer;
import com.github.dapeng.util.DumpUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.MDC;

import java.util.List;
import java.util.ServiceLoader;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Future;
import java.util.stream.Collectors;

/**
 * desc: AsyncJsonPost
 *
 * @author hz.lei
 * @since 2018年08月21日 下午3:10
 */
public class AsyncJsonPost {

    private static final Logger LOGGER = LoggerFactory.getLogger(AsyncJsonPost.class);

    private boolean doNotThrowError = false;

    private final static SoaConnectionPoolFactory factory = ServiceLoader.load(SoaConnectionPoolFactory.class, AsyncJsonPost.class.getClassLoader()).iterator().next();


    private SoaConnectionPool pool;
    private final SoaConnectionPool.ClientInfo clientInfo;
    private final String methodName;

    public AsyncJsonPost(final String serviceName, final String version, final String methodName) {
        this.methodName = methodName;
        this.pool = factory.getPool();
        this.clientInfo = this.pool.registerClientInfo(serviceName, version);
    }

    public AsyncJsonPost(final String serviceName, final String version, final String methodName, boolean doNotThrowError) {
        this(serviceName, version, methodName);
        this.doNotThrowError = doNotThrowError;
    }

    /**
     * 异步调用远程服务
     *
     * @param jsonParameter
     * @param service
     * @return
     * @throws Exception
     */
    public Future<String> callServiceMethodAsync(final String jsonParameter,
                                                 final Service service) throws Exception {
        List<Method> targetMethods = service.getMethods().stream().filter(element ->
                element.name.equals(methodName))
                .collect(Collectors.toList());

        if (targetMethods.isEmpty()) {
            String resp = String.format("{\"responseCode\":\"%s\", \"responseMsg\":\"%s\", \"success\":\"{}\", \"status\":0}",
                    SoaCode.NoMatchedMethod,
                    "method:" + methodName + " for service:" + clientInfo.serviceName + " not found");
            return CompletableFuture.completedFuture(resp);
        }
        try {
            String sessionTid = InvocationContextImpl.Factory.currentInstance().sessionTid().map(DapengUtil::longToHexStr).orElse("0");
            MDC.put(SoaSystemEnvProperties.KEY_LOGGER_SESSION_TID, sessionTid);

            Method method = targetMethods.get(0);


            JsonSerializer jsonEncoder = new JsonSerializer(service, method, clientInfo.version, method.request);
            JsonSerializer jsonDecoder = new JsonSerializer(service, method, clientInfo.version, method.response);

            final long beginTime = System.currentTimeMillis();

            LOGGER.info("soa-request: service:[" + service.namespace + "." + service.name
                    + ":" + service.meta.version + "], method:" + methodName + ", param:"
                    + jsonParameter);

            Future<String> jsonResponse = postAsync(clientInfo.serviceName, clientInfo.version,
                    methodName, jsonParameter, jsonEncoder, jsonDecoder);
            //MDC will be remove by client filter
            MDC.put(SoaSystemEnvProperties.KEY_LOGGER_SESSION_TID, sessionTid);

          /*  if (LOGGER.isDebugEnabled()) {
                LOGGER.debug("soa-response: " + jsonResponse + " cost:" + (System.currentTimeMillis() - beginTime) + "ms");
            } else {
                LOGGER.info("soa-response: " + DumpUtil.formatToString(jsonResponse) + " cost:" + (System.currentTimeMillis() - beginTime) + "ms");
            }*/

            return jsonResponse;
        } finally {
            MDC.remove(SoaSystemEnvProperties.KEY_LOGGER_SESSION_TID);
        }


    }

    /**
     * 构建客户端，发送和接收异步请求
     *
     * @return
     */
    private Future<String> postAsync(String serviceName, String version, String method, String requestJson, JsonSerializer jsonEncoder, JsonSerializer jsonDecoder) throws Exception {
        Future<String> jsonResponse;
        try {
            jsonResponse = this.pool.sendAsync(serviceName, version, method, requestJson, jsonEncoder, jsonDecoder);
//            jsonResponse = result.equals("{}") ? "{\"status\":1}" : result.substring(0, result.lastIndexOf('}')) + ",\"status\":1}";
        } catch (SoaException e) {
            if (DapengUtil.isDapengCoreException(e)) {
                LOGGER.error(e.getMsg(), e);
            } else {
                LOGGER.error(e.getMsg());
            }
            if (doNotThrowError) {
                jsonResponse = CompletableFuture.completedFuture(String.format("{\"responseCode\":\"%s\", \"responseMsg\":\"%s\", \"success\":\"%s\", \"status\":0}", e.getCode(), e.getMsg(), "{}"));
            } else {
                throw e;
            }

        } catch (Exception e) {

            LOGGER.error(e.getMessage(), e);
            if (doNotThrowError) {
                jsonResponse = CompletableFuture.completedFuture(String.format("{\"responseCode\":\"%s\", \"responseMsg\":\"%s\", \"success\":\"%s\", \"status\":0}", "9999", "系统繁忙，请稍后再试[9999]！", "{}"));
            } else {
                throw e;
            }
        }
        return jsonResponse;
    }
}
