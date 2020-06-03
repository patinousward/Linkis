package com.webank.wedatasphere.linkis.cs.persistence;

import com.webank.wedatasphere.linkis.cs.common.exception.CSErrorException;
import net.sf.cglib.proxy.Callback;
import net.sf.cglib.proxy.Enhancer;

/**
 * Created by patinousward on 2020/2/23.
 */
public class ProxyMethodA {

    public <T> T getContextHAProxy(T persistence) throws CSErrorException {
        System.out.println("invoke proxy");
        return persistence;
    }
}
