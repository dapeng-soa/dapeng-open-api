package com.github.dapeng.openapi.utils;

import com.github.dapeng.client.netty.JsonPost;
import com.github.dapeng.core.InvocationContext;
import com.github.dapeng.core.InvocationContextImpl;
import com.github.dapeng.core.SoaException;
import com.github.dapeng.core.metadata.Service;
import com.github.dapeng.openapi.cache.ServiceCache;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.servlet.http.HttpServletRequest;
import java.util.Optional;
import java.util.Set;

/**
 * @author ever
 */
public class PostUtil {
    private static final Logger LOGGER = LoggerFactory.getLogger(PostUtil.class);

    public static String post(String service,
                              String version,
                              String method,
                              String parameter,
                              HttpServletRequest req) {
        InvocationContext invocationCtx = InvocationContextImpl.Factory.getCurrentInstance();
        invocationCtx.setServiceName(service);
        invocationCtx.setVersionName(version);
        invocationCtx.setMethodName(method);
        invocationCtx.setCallerFrom(Optional.of("JsonCaller"));
        //设置请求超时时间 10 s
        invocationCtx.setTimeout(Optional.of(120000L));
        LOGGER.info("<=========>   timeout:" + invocationCtx.getTimeout());

        Service bizService = ServiceCache.getService(service, version);

        fillInvocationCtx(invocationCtx, req);

        JsonPost jsonPost = new JsonPost(service, method, true);

        try {
            return jsonPost.callServiceMethod(invocationCtx, parameter, bizService);
        } catch (SoaException e) {

            LOGGER.error(e.getMsg(), e);
            return String.format("{\"responseCode\":\"%s\", \"responseMsg\":\"%s\", \"success\":\"%s\", \"status\":0}", e.getCode(), e.getMsg(), "{}");

        } catch (Exception e) {

            LOGGER.error(e.getMessage(), e);
            return String.format("{\"responseCode\":\"%s\", \"responseMsg\":\"%s\", \"success\":\"%s\", \"status\":0}", "9999", "系统繁忙，请稍后再试[9999]！", "{}");
        } finally {
            InvocationContextImpl.Factory.removeCurrentInstance();
        }
    }

    private static void fillInvocationCtx(InvocationContext invocationCtx, HttpServletRequest req) {
        Set<String> parameters = req.getParameterMap().keySet();
        if (parameters.contains("calleeIp")) {
            invocationCtx.setCalleeIp(Optional.of(req.getParameter("calleeIp")));
        }

        if (parameters.contains("calleePort")) {
            invocationCtx.setCalleePort(Optional.of(Integer.valueOf(req.getParameter("calleePort"))));
        }

        if (parameters.contains("callerIp")) {
            invocationCtx.setCallerIp(Optional.of(req.getParameter("callerIp")));
        }

        if (parameters.contains("callerFrom")) {
            invocationCtx.setCallerFrom(Optional.of(req.getParameter("callerFrom")));
        }

        if (parameters.contains("customerName")) {
            invocationCtx.setCustomerName(Optional.of(req.getParameter("customerName")));
        }

        if (parameters.contains("customerId")) {
            invocationCtx.setCustomerId(Optional.of(Integer.valueOf(req.getParameter("customerId"))));
        }

        if (parameters.contains("operatorId")) {
            invocationCtx.setOperatorId(Optional.of(Integer.valueOf(req.getParameter("operatorId"))));
        }
    }
}
