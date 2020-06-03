package com.webank.wedatasphere.linkis.cs.persistence;

import com.webank.wedatasphere.linkis.cs.common.entity.listener.ContextIDListenerDomain;
import com.webank.wedatasphere.linkis.cs.common.entity.source.ContextID;

/**
 * Created by patinousward on 2020/2/17.
 */
public class AContextIDListener implements ContextIDListenerDomain {

    private String source;

    private ContextID contextID;

    @Override
    public String getSource() {
        return this.source;
    }

    @Override
    public void setSource(String source) {
        this.source = source;
    }

    @Override
    public ContextID getContextID() {
        return this.contextID;
    }

    @Override
    public void setContextID(ContextID contextID) {
        this.contextID = contextID;
    }
}
