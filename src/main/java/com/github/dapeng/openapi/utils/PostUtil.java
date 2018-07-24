package com.github.dapeng.openapi.utils;

import com.github.dapeng.client.netty.JsonPost;
import com.github.dapeng.core.InvocationContext;
import com.github.dapeng.core.InvocationContextImpl;
import com.github.dapeng.core.SoaCode;
import com.github.dapeng.core.SoaException;
import com.github.dapeng.core.enums.CodecProtocol;
import com.github.dapeng.core.helper.DapengUtil;
import com.github.dapeng.core.helper.IPUtils;
import com.github.dapeng.core.helper.SoaSystemEnvProperties;
import com.github.dapeng.core.metadata.Service;
import com.github.dapeng.openapi.cache.ServiceCache;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.MDC;

import javax.servlet.http.HttpServletRequest;
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
        return post(service, version, method, parameter, req, true);
    }

    /**
     *
     * @param service
     * @param version
     * @param method
     * @param parameter
     * @param req
     * @param clearInvocationContext 是否清理InvocationContext. 如果不清理, 调用端负责清理
     * @return
     */
    public static String post(String service,
                              String version,
                              String method,
                              String parameter,
                              HttpServletRequest req,
                              boolean clearInvocationContext) {
        InvocationContextImpl invocationCtx = (InvocationContextImpl) InvocationContextImpl.Factory.currentInstance();
        invocationCtx.serviceName(service);
        invocationCtx.versionName(version);
        invocationCtx.methodName(method);
        invocationCtx.callerMid(req.getRequestURI());
        if (!invocationCtx.sessionTid().isPresent()) {
            invocationCtx.sessionTid(DapengUtil.generateTid());
        }
        if (!invocationCtx.timeout().isPresent()) {
            //设置请求超时时间,从环境变量获取
            int timeOut = getEnvTimeOut();
            if (timeOut > 0) {
                invocationCtx.timeout(timeOut);
            }
        }

        invocationCtx.codecProtocol(CodecProtocol.CompressedBinary);

        Service bizService = ServiceCache.getService(service, version);

        if (bizService == null) {
            LOGGER.error("bizService not found[service:" + service + ", version:" + version + "]");
            return String.format("{\"responseCode\":\"%s\", \"responseMsg\":\"%s\", \"success\":\"%s\", \"status\":0}", SoaCode.NoMatchedService.getCode(), SoaCode.NoMatchedService.getMsg(), "{}");
        }

        MDC.put(SoaSystemEnvProperties.KEY_LOGGER_SESSION_TID, invocationCtx.sessionTid().map(DapengUtil::longToHexStr).orElse("0"));

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
            if (clearInvocationContext) {
                InvocationContextImpl.Factory.removeCurrentInstance();
                MDC.put(SoaSystemEnvProperties.KEY_LOGGER_SESSION_TID, invocationCtx.sessionTid().map(DapengUtil::longToHexStr).orElse("0"));
            }
        }
    }

    private static void fillInvocationCtx(InvocationContext invocationCtx, HttpServletRequest req) {
        Set<String> parameters = req.getParameterMap().keySet();
        if (parameters.contains("calleeIp")) {
            invocationCtx.calleeIp(IPUtils.transferIp(req.getParameter("calleeIp")));
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

        InvocationContextImpl.InvocationContextProxy invocationCtxProxy = InvocationContextImpl.Factory.getInvocationContextProxy();
        invocationCtx.cookies(invocationCtxProxy.cookies());
    }


    private static int getEnvTimeOut() {
        return SoaSystemEnvProperties.SOA_SERVICE_TIMEOUT.intValue();
    }
}
