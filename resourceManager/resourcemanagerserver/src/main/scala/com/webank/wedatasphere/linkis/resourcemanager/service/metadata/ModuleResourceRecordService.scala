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

package com.webank.wedatasphere.linkis.resourcemanager.service.metadata

import java.util
import java.util.{Date, List}

import com.webank.wedatasphere.linkis.common.ServiceInstance
import com.webank.wedatasphere.linkis.common.utils.Logging
import com.webank.wedatasphere.linkis.resourcemanager.{Resource, ResourceRequestPolicy, ResourceSerializer}
import com.webank.wedatasphere.linkis.resourcemanager.dao.{EmMetaDataDao, EmResourceMetaDataDao}
import com.webank.wedatasphere.linkis.resourcemanager.domain.{EmMetaData, EmResourceMetaData, ModuleInfo, ModuleResourceRecord}
import com.webank.wedatasphere.linkis.resourcemanager.exception.RMErrorException
import org.json4s.DefaultFormats
import org.json4s.jackson.Serialization.{read, write}
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.stereotype.Component

import collection.JavaConversions._
import scala.collection.JavaConversions

/**
  * Created by shanhuang on 9/11/18.
  */
@Component
class ModuleResourceRecordService extends Logging {

  implicit val formats = DefaultFormats + ResourceSerializer
  @Autowired
  var emResourceMetaDataDao: EmResourceMetaDataDao = _
  @Autowired
  var emMetaDataDao: EmMetaDataDao = _

  def putModulePolicy(moduleName: String, policy: ResourceRequestPolicy.ResourceRequestPolicy): Unit = {
    var emMetaData = emMetaDataDao.getByEmName(moduleName)
    if (emMetaData == null) {
      emMetaData = new EmMetaData(moduleName, policy.toString)
      emMetaDataDao.insert(emMetaData)
    } else {
      emMetaData.setResourceRequestPolicy(policy.toString)
      emMetaDataDao.update(emMetaData)
    }
  }
  //从linkis_em_meta_data 中 映射的enginemanager的applcationName 和资源请求政策 的映射关系，获取相应的请求政策
  def getModulePolicy(moduleName: String): ResourceRequestPolicy.ResourceRequestPolicy = {
    val emMetaData = emMetaDataDao.getByEmName(moduleName)
    if (emMetaData == null) throw new RMErrorException(110005, s"Module(模块): $moduleName Not registered in the resource manager(并没有在资源管理器进行注册)") else ResourceRequestPolicy.withName(emMetaData.getResourceRequestPolicy)
  }

  def getModuleName: Array[String] = JavaConversions.asScalaBuffer(emMetaDataDao.getAll).toArray.map(_.getEmName)

  /**
    * Obtain resource usage records for the same type of module by module name
    * 通过模块名获得同一类模块的资源使用记录
    *
    * @param moduleName
    * @return
    */
  def getModuleResourceRecords(moduleName: String): Array[EmResourceMetaData] = {
    val records = getByEmName(moduleName)
    if (records.size() < 1) throw new RMErrorException(110005, s"Module(模块): $moduleName Not registered in the resource manager(并没有在资源管理器进行注册)")
    JavaConversions.asScalaBuffer(records).toArray
  }

  def getModuleResourceRecord(moduleInstance: ServiceInstance): EmResourceMetaData = {
    val m = getByEmInstance(moduleInstance.getApplicationName, moduleInstance.getInstance)
    if (m == null)
      throw new RMErrorException(110005, s"Module instance(模块实例): $moduleInstance Not registered in the resource manager(并没有在资源管理器进行注册)")
    m
  }

  def putModuleRegisterRecord(moduleInfo: ModuleInfo): Unit = synchronized {
    //从linkis_em_resource_meta_data  根据engineManger的applicationName和instance信息(ip端口)获取existing
    //linkis_em_resource_meta_data  这个表接受多个比如sparkEngineManger的注册
    val existing = getByEmInstance(moduleInfo.moduleInstance.getApplicationName, moduleInfo.moduleInstance.getInstance)
    if (existing != null)
      //已经存在抛出异常
      throw new RMErrorException(110005, s"Module instance(模块实例): ${moduleInfo.moduleInstance} Already registered, if you need to re-register, please log out and then register(已经注册，如果需要重新注册请注销后再进行注册)")
    ModuleResourceRecord(moduleInfo, Resource.initResource(moduleInfo.resourceRequestPolicy),
      moduleInfo.totalResource, Resource.initResource(moduleInfo.resourceRequestPolicy))
    val newRecord = new EmResourceMetaData(
      moduleInfo.moduleInstance.getApplicationName,
      moduleInfo.moduleInstance.getInstance,
      serialize(moduleInfo.totalResource),//总资源
      serialize(moduleInfo.protectedResource),//保护资源
      moduleInfo.resourceRequestPolicy.toString,//资源策略
      serialize(Resource.initResource(moduleInfo.resourceRequestPolicy)),//已经使用资源,刚注册当然全是0
      serialize(moduleInfo.totalResource),//剩余资源,刚注册就是总资源
      serialize(Resource.initResource(moduleInfo.resourceRequestPolicy)),//锁定资源,刚注册当然是0
      System.currentTimeMillis()//注册时间
    )
    insert(newRecord) //linkis_em_resource_meta_data 表中插入记录
    info(s"Succeed to  register module ${moduleInfo.moduleInstance}")
  }

  def moduleLockedUserResource(moduleInstance: ServiceInstance, resource: Resource): Unit = synchronized {
    val existing = getByEmInstance(moduleInstance.getApplicationName, moduleInstance.getInstance)
    if (existing == null)
      throw new RMErrorException(110005, s"Module instance(模块实例): $moduleInstance Not registered in the resource manager(并没有在资源管理器进行注册)")
    //lock资源增加，剩余资源减少
    existing.setLeftResource(serialize(deserialize(existing.getLeftResource) - resource))
    existing.setLockedResource(serialize(deserialize(existing.getLockedResource) + resource))
    update(existing)
  }

  def moduleUsedUserResource(moduleInstance: ServiceInstance, usedResource: Resource, lockedResource: Resource): Unit = synchronized {
    val existing = getByEmInstance(moduleInstance.getApplicationName, moduleInstance.getInstance)
    if (existing == null)
      throw new RMErrorException(110005, s"Module instance(模块实例): $moduleInstance Not registered in the resource manager(并没有在资源管理器进行注册)")
    //模块使用资源增加
    existing.setUsedResource(serialize(deserialize(existing.getUsedResource) + usedResource))
    //锁定资源减少，所以把锁定资源加到剩余资源上面了
    existing.setLeftResource(serialize(deserialize(existing.getLeftResource) + lockedResource - usedResource))
    existing.setLockedResource(serialize(deserialize(existing.getLockedResource) - lockedResource))
    update(existing)
  }

  def moduleReleasedUserResource(moduleInstance: ServiceInstance, usedResource: Resource): Unit = synchronized {
    val existing = getByEmInstance(moduleInstance.getApplicationName, moduleInstance.getInstance)
    if (existing == null)
      throw new RMErrorException(110005, s"Module instance(模块实例): $moduleInstance Not registered in the resource manager(并没有在资源管理器进行注册)")
    //减去已经使用的资源，增加剩余资源
    existing.setUsedResource(serialize(deserialize(existing.getUsedResource) - usedResource))
    existing.setLeftResource(serialize(deserialize(existing.getLeftResource) + usedResource))
    update(existing)
  }

  def moduleClearLockedResource(moduleInstance: ServiceInstance, resource: Resource): Unit = synchronized {
    val existing = getByEmInstance(moduleInstance.getApplicationName, moduleInstance.getInstance)
    if (existing == null)
      throw new RMErrorException(110005, s"Module instance(模块实例): $moduleInstance Not registered in the resource manager(并没有在资源管理器进行注册)")
    //增加剩余资源,去除 锁定的资源
    existing.setLeftResource(serialize(deserialize(existing.getLeftResource) + resource))
    existing.setLockedResource(serialize(deserialize(existing.getLockedResource) - resource))
    update(existing)
  }

  def moduleClearUsedResource(moduleInstance: ServiceInstance, resource: Resource): Unit = synchronized {
    val existing = getByEmInstance(moduleInstance.getApplicationName, moduleInstance.getInstance)
    if (existing == null)
      throw new RMErrorException(110005, s"Module instance(模块实例): $moduleInstance Not registered in the resource manager(并没有在资源管理器进行注册)")
    existing.setUsedResource(serialize(deserialize(existing.getUsedResource) - resource))
    existing.setLeftResource(serialize(deserialize(existing.getLeftResource) + resource))
  }

  def serialize(resource: Resource) = write(resource)

  def deserialize(plainData: String) = read[Resource](plainData)

  def getAll(): util.List[EmResourceMetaData] = emResourceMetaDataDao.getAll

  def getByEmName(emApplicationName: String): util.List[EmResourceMetaData] = emResourceMetaDataDao.getByEmName(emApplicationName)

  def getByEmInstance(emApplicationName: String, emInstance: String): EmResourceMetaData = emResourceMetaDataDao.getByEmInstance(emApplicationName, emInstance)

  def insert(emResourceMetaData: EmResourceMetaData): Unit = {
    emResourceMetaDataDao.insert(emResourceMetaData)
  }

  def update(emResourceMetaData: EmResourceMetaData): Unit = {
    emResourceMetaDataDao.update(emResourceMetaData)
  }

  def delete(id: Integer) = {
    emResourceMetaDataDao.deleteById(id)
  }

}
