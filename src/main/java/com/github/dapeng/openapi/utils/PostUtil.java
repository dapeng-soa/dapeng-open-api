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
        InvocationContextImpl invocationCtx = (InvocationContextImpl) InvocationContextImpl.Factory.currentInstance();
        invocationCtx.serviceName(service);
        invocationCtx.versionName(version);
        invocationCtx.methodName(method);
        invocationCtx.callerMid(req.getRequestURI());
        //设置请求超时时间 10 s
        invocationCtx.timeout(120000);
        LOGGER.info("<=========>   timeout:" + invocationCtx.timeout());

        Service bizService = ServiceCache.getService(service, version);

        fillInvocationCtx(invocationCtx, req);

        JsonPost jsonPost = new JsonPost(service, version, method, true);

        try {
            return jsonPost.callServiceMethod(parameter, bizService);
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
            invocationCtx.calleeIp(req.getParameter("calleeIp"));
        }

        if (parameters.contains("calleePort")) {
            invocationCtx.calleePort(Integer.valueOf(req.getParameter("calleePort")));
        }

        if (parameters.contains("callerMid")) {
            invocationCtx.callerMid(req.getParameter("callerMid"));
        }

        if (parameters.contains("userId")) {
            invocationCtx.userId(Long.valueOf(req.getParameter("userId")));
        }

        if (parameters.contains("operatorId")) {
            invocationCtx.operatorId(Long.valueOf(req.getParameter("operatorId")));
        }
    }
}
