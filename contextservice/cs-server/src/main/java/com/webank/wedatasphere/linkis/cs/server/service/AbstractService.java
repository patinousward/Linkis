package com.webank.wedatasphere.linkis.cs.server.service;

import com.webank.wedatasphere.linkis.cs.common.exception.CSErrorException;
import com.webank.wedatasphere.linkis.cs.common.exception.CSWarnException;
import com.webank.wedatasphere.linkis.cs.server.protocol.HttpRequestProtocol;
import com.webank.wedatasphere.linkis.cs.server.protocol.HttpResponseProtocol;
import com.webank.wedatasphere.linkis.cs.server.protocol.RestResponseProtocol;
import com.webank.wedatasphere.linkis.cs.server.scheduler.HttpAnswerJob;
import com.webank.wedatasphere.linkis.cs.server.scheduler.HttpJob;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.lang.reflect.Method;
import java.util.Arrays;
import java.util.Optional;

/**
 * Created by patinousward on 2020/2/18.
 */
public abstract class AbstractService implements Service {

    protected final Logger logger = LoggerFactory.getLogger(this.getClass());

    @Override
    public boolean ifAccept(HttpJob job) {
        return getName().equals(job.getRequestProtocol().getServiceName());
    }

    @Override
    public void accept(HttpJob job) throws CSWarnException {
        try {
            //根据参数类型和方法名,选出方法进行调用
            HttpRequestProtocol protocol = job.getRequestProtocol();
            Object[] params = protocol.getRequestObjects();
            String method = protocol.getServiceMethod().name().toUpperCase();
            Method[] methods = this.getClass().getMethods();
            Optional<Method> first = Arrays.stream(methods).filter(f -> f.getName().toUpperCase().contains(method))
                    .filter(f -> f.getParameterTypes().length == params.length)
                    .filter(f -> judgeMethod(f, params)).findFirst();
            Object response = first.orElseThrow(() -> new CSErrorException(97000, "can not find a method to invoke")).invoke(this, params);
            if (job instanceof HttpAnswerJob) {
                HttpResponseProtocol responseProtocol = ((HttpAnswerJob) job).getResponseProtocol();
                if (responseProtocol instanceof RestResponseProtocol) {
                    ((RestResponseProtocol) responseProtocol).ok(null);
                }
                responseProtocol.setResponseData(response);
            }
        } catch (Exception e) {
            logger.error(String.format("execute %s service failed:", getName()), e);
            throw new CSWarnException(97000, e.getMessage());
        }
    }

    private boolean judgeMethod(Method method, Object... objects) {
        boolean flag = true;
        //传入参数类型是否是方法参数的子类
        Class<?>[] parameterTypes = method.getParameterTypes();
        for (int i = 0; i < parameterTypes.length; i++) {
            if (!parameterTypes[i].isAssignableFrom(objects[i].getClass())) {
                flag = false;
                break;
            }
        }
        return flag;
    }

    @Override
    public void init() {

    }

    @Override
    public void start() {

    }

    @Override
    public void close() throws IOException {

    }
}
