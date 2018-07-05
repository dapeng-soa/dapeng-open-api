package com.github.dapeng.client.netty;

import com.github.dapeng.core.InvocationContextImpl;
import com.github.dapeng.core.helper.DapengUtil;
import com.github.dapeng.metadata.GetServiceMetadata_argsSerializer;
import com.github.dapeng.metadata.GetServiceMetadata_resultSerializer;
import com.github.dapeng.metadata.getServiceMetadata_args;
import com.github.dapeng.metadata.getServiceMetadata_result;

/**
 * 获取 远程服务matedata的方法
 *
 * @author huyj
 * @Created 2018/5/11 10:57
 */
public class MetadataUtils {

    private static final long TIME_OUT = 500;
    private static final String METADATA_METHOD = "getServiceMetadata";

    /**
     * getRomoteServiceMetadata
     **/
    public static String getRomoteServiceMetadata(String romoteIp, Integer remotePort,String serviceName,String version) throws Exception {
        InvocationContextImpl.Factory.currentInstance().sessionTid(DapengUtil.generateTid()).callerMid("InnerApiSite");

        SubPool subPool = new SubPool(romoteIp, remotePort);
        getServiceMetadata_result result = subPool.getConnection().send(serviceName, version, METADATA_METHOD,
                new getServiceMetadata_args(),
                new GetServiceMetadata_argsSerializer(),
                new GetServiceMetadata_resultSerializer(),TIME_OUT);
        return result.getSuccess();
    }
}
