package com.webank.wedatasphere.linkis.cs.persistence;

import com.webank.wedatasphere.linkis.cs.common.entity.enumeration.ExpireType;
import com.webank.wedatasphere.linkis.cs.common.entity.source.HAContextID;
import com.webank.wedatasphere.linkis.cs.common.entity.source.UserContextID;

import java.util.Date;
import java.util.Random;

/**
 * Created by patinousward on 2020/2/13.
 */
public class AContextID implements UserContextID, HAContextID {

    private String contextId;

    private String user = "neiljianliu";

    private String instance = "instance";

    private String backupInstance = "backup";

    private String application = "spark";

    private ExpireType expireType = ExpireType.NEVER;

    private Date expireTime = new Date();

    private String project = "project1";

    private String flow = "flow1";

    public String getProject() {
        return project;
    }

    public void setProject(String project) {
        this.project = project;
    }

    public String getFlow() {
        return flow;
    }

    public void setFlow(String flow) {
        this.flow = flow;
    }

    @Override
    public String getContextId() {
        return this.contextId;
    }

    @Override
    public void setContextId(String contextId) {
        this.contextId = contextId;
    }

    @Override
    public void setUser(String user) {
        this.user = user;
    }

    @Override
    public String getUser() {
        return this.user;
    }

    @Override
    public String getInstance() {
        return this.instance;
    }

    @Override
    public void setInstance(String instance) {
        this.instance = instance;
    }

    @Override
    public String getBackupInstance() {
        return this.backupInstance;
    }

    @Override
    public void setBackupInstance(String backupInstance) {
        this.backupInstance = backupInstance;
    }

    public String getApplication() {
        return application;
    }

    public void setApplication(String application) {
        this.application = application;
    }

    public ExpireType getExpireType() {
        return expireType;
    }

    public void setExpireType(ExpireType expireType) {
        this.expireType = expireType;
    }

    public Date getExpireTime() {
        return expireTime;
    }

    public void setExpireTime(Date expireTime) {
        this.expireTime = expireTime;
    }

    @Override
    public int getContextIDType() {
        return 0;
    }

    @Override
    public void setContextIDType(int contextIDType) {

    }
}
