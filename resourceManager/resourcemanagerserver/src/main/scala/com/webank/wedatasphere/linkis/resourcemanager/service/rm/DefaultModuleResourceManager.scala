/*
 * Copyright 2019 WeBank
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.webank.wedatasphere.linkis.resourcemanager.service.rm

import com.webank.wedatasphere.linkis.common.ServiceInstance
import com.webank.wedatasphere.linkis.common.listener.Event
import com.webank.wedatasphere.linkis.common.utils.{Logging, Utils}
import com.webank.wedatasphere.linkis.resourcemanager.domain.{EmResourceMetaData, ModuleResourceInfo}
import com.webank.wedatasphere.linkis.resourcemanager.event.notify.{ModuleRegisterEvent, ModuleUnregisterEvent, NotifyRMEvent}
import com.webank.wedatasphere.linkis.resourcemanager.exception.RMErrorException
import com.webank.wedatasphere.linkis.resourcemanager.service.metadata.{ModuleResourceRecordService, UserResourceRecordService}
import com.webank.wedatasphere.linkis.resourcemanager.{ModuleResourceManager, Resource}
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.stereotype.Component

/**
  * Created by johnnwang on 2018/9/14.
  */
@Component
class DefaultModuleResourceManager extends ModuleResourceManager with Logging {

  @Autowired
  var moduleResourceRecordService: ModuleResourceRecordService = _
  @Autowired
  var userResourceRecordService: UserResourceRecordService = _

  override def getModuleResourceInfo(moduleInstance: ServiceInstance): Array[ModuleResourceInfo] = {
    if (moduleInstance.getInstance == null) {
      moduleResourceRecordService.getModuleResourceRecords(moduleInstance.getApplicationName).map { m =>
        ModuleResourceInfo(
          ServiceInstance(m.getEmApplicationName, m.getEmInstance),
          moduleResourceRecordService.deserialize(m.getTotalResource),
          moduleResourceRecordService.deserialize(m.getUsedResource)
        )

      }
    } else {
      val m = moduleResourceRecordService.getModuleResourceRecord(moduleInstance)
      Array(ModuleResourceInfo(
        ServiceInstance(m.getEmApplicationName, m.getEmInstance),
        moduleResourceRecordService.deserialize(m.getTotalResource),
        moduleResourceRecordService.deserialize(m.getUsedResource)
      ))
    }
  }

  override def getModuleResources(moduleName: String): Array[Resource] = {
    moduleResourceRecordService.getModuleResourceRecords(moduleName).map(_.getTotalResource).map(moduleResourceRecordService.deserialize)
  }

  override def getModuleTotalResources(moduleName: String): Resource = {
    val records = moduleResourceRecordService.getModuleResourceRecords(moduleName)
    if (records == null || records.length < 1) {
      null
    } else {
      records.map(_.getTotalResource).map(moduleResourceRecordService.deserialize).reduce((r1, r2) => r1 + r2)
    }
  }


  override def getModuleUsedResources(moduleName: String): Resource = {
    val records = moduleResourceRecordService.getModuleResourceRecords(moduleName)
    if (records == null || records.length < 1) {
      null
    } else {
      records.map(_.getUsedResource).map(moduleResourceRecordService.deserialize).reduce((r1, r2) => r1 + r2)
    }
  }

  override def getModuleLockedResources(moduleName: String): Resource = {
    val records = moduleResourceRecordService.getModuleResourceRecords(moduleName)
    if (records == null || records.length < 1) {
      null
    } else {
      records.map(_.getLockedResource).map(moduleResourceRecordService.deserialize).reduce((r1, r2) => r1 + r2)
    }
  }

  override def getModuleLeftResources(moduleName: String): Resource = {
    val records = moduleResourceRecordService.getModuleResourceRecords(moduleName)
    if (records == null || records.length < 1) {
      null
    } else {
      records.map(_.getLeftResource).map(moduleResourceRecordService.deserialize).reduce((r1, r2) => r1 + r2)
    }
  }

  override def getInstanceResource(moduleInstance: ServiceInstance): Resource = {
    moduleResourceRecordService.deserialize(moduleResourceRecordService.getModuleResourceRecord(moduleInstance).getTotalResource)
  }

  override def getInstanceUsedResource(moduleInstance: ServiceInstance): Resource = {
    moduleResourceRecordService.deserialize(moduleResourceRecordService.getModuleResourceRecord(moduleInstance).getUsedResource)
  }

  override def getInstanceLockedResource(moduleInstance: ServiceInstance): Resource = {
    moduleResourceRecordService.deserialize(moduleResourceRecordService.getModuleResourceRecord(moduleInstance).getLockedResource)
  }

  override def getInstanceLeftResource(moduleInstance: ServiceInstance): Resource = {
    moduleResourceRecordService.deserialize(moduleResourceRecordService.getModuleResourceRecord(moduleInstance).getLeftResource)
  }


  override def onNotifyRMEvent(event: NotifyRMEvent): Unit = event match {
    case moduleRegisterEvent: ModuleRegisterEvent => dealModuleRegisterEvent(moduleRegisterEvent)
    case moduleUnregisterEvent: ModuleUnregisterEvent => dealModuleUnregisterEvent(moduleUnregisterEvent)
    case _ =>
  }

  override def onEventError(event: Event, t: Throwable): Unit = {}

  override def dealModuleRegisterEvent(event: ModuleRegisterEvent): Unit = synchronized {
    //RMListenerBus.getRMListenerBusInstance.post(event)
    info(s"Start processing  registration event of module：${event.moduleInfo}")
    val moduleInfo = event.moduleInfo
    //linkis_em_meta_data 表,EngineManager的applicationName为key,policy为value,不存在就插入记录,存在就update
    //虽然做了synchronized,但是如果有2个RM 服务,并且是并发请求的,就会有问题
    moduleResourceRecordService.putModulePolicy(moduleInfo.moduleInstance.getApplicationName, moduleInfo.resourceRequestPolicy)
    //linkis_em_resource_meta_data表中插入记录
    moduleResourceRecordService.putModuleRegisterRecord(moduleInfo)
    info(s"End processing registration events ${event.moduleInfo.moduleInstance} success")
  }

  /**
    *
    * @param event
    */
  override def dealModuleUnregisterEvent(event: ModuleUnregisterEvent): Unit = synchronized {
    //RMListenerBus.getRMListenerBusInstance.post(event)
    info(s"Start processing  logout event of module：${event.moduleInstance}")
    val moduleInstance = event.moduleInstance
    //删除linkis_user_resource_meta_data 表中 em_application_name 和engineMangerinsrance信息和unregis相同的所有记录
    //这个表主要存放user-引擎和enginemanagre的关系表,每个引擎都会在这里留下记录
    Utils.tryQuietly(userResourceRecordService.clearModuleResourceRecord(moduleInstance))
    //查看下linkis_rm_resource_meta_data 表中是否有数据,没有抛出异常
    val record = moduleResourceRecordService.getModuleResourceRecord(moduleInstance)
    //这里的null判断多余了,上面的方法返回的时候就已经判断了
    if (record == null) throw new RMErrorException(110005, s"Failed to process logout event of module : $moduleInstance Not registered")
    //根据id删除这条记录
    moduleResourceRecordService.delete(record.getId)
    info(s"End processing  logout event of module：${event.moduleInstance} success")
  }

  override def getModuleNames(): Array[String] = {
    moduleResourceRecordService.getModuleName
  }

  override def getModuleInstancesByName(moduleName: String): Array[EmResourceMetaData] = {
    moduleResourceRecordService.getModuleResourceRecords(moduleName)
  }
}
