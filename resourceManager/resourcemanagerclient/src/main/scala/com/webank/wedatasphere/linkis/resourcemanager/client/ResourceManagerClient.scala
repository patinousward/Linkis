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

package com.webank.wedatasphere.linkis.resourcemanager.client

import com.webank.wedatasphere.linkis.common.ServiceInstance
import com.webank.wedatasphere.linkis.common.utils.Logging
import com.webank.wedatasphere.linkis.resourcemanager.ResourceRequestPolicy._
import com.webank.wedatasphere.linkis.resourcemanager.domain.ModuleInfo
import com.webank.wedatasphere.linkis.resourcemanager.utils.RMConfiguration
import com.webank.wedatasphere.linkis.resourcemanager.{ResourceRequestPolicy => _, _}
import com.webank.wedatasphere.linkis.rpc.Sender
import com.webank.wedatasphere.linkis.rpc.transform.RPCProduct
import javax.annotation.PostConstruct
import org.springframework.stereotype.Component

/**
  * Created by shanhuang on 9/13/18.
  */

@Component
class ResourceManagerClient(private var moduleInstance: ServiceInstance) extends Logging {

  private val sender = Sender.getSender(RMConfiguration.RM_MODEL_APPLICATION_NAME.getValue)

  def this() = this(null)

  @PostConstruct
  def init(): Unit = {
    info("ResourceManagerClient init")
    if (moduleInstance == null) moduleInstance = Sender.getThisServiceInstance
  }
  //注册服务器资源的地方
  def register(moduleInfo: ModuleInfo): Unit = sender.send(moduleInfo)

  //目前其实没有用到
  def register(totalResource: Resource, protectedResource: Resource, resourceRequestPolicy: ResourceRequestPolicy): Unit =
    sender.send(ModuleInfo(moduleInstance, totalResource, protectedResource, resourceRequestPolicy))

  //engineManger 服务关闭的时候去unregister
  def unregister(): Unit = sender.send(moduleInstance)

  //目前其实没有用到
  def requestResource(user: String, creator: String, resource: Resource): ResultResource = sender.ask(RequestResource(moduleInstance, user, creator, resource)).asInstanceOf[ResultResource]

  //engineManger请求的引擎初始化资源/引擎启动资源
  def requestResource(user: String, creator: String, resource: Resource, wait: Long) = sender.ask(RequestResourceAndWait(moduleInstance, user, creator, resource, wait)).asInstanceOf[ResultResource]

  //engine上报实际使用的资源
  def resourceInited(resource: ResultResource, realUsed: Resource): Unit = {
    info("ResourceManagerClient init")
    sender.send(ResourceInited(resource, moduleInstance, realUsed))
  }
  //engine被kill(无论用户主动还是因为失去心跳被动) resultResource:UserResultResource(ticketid,username)
  def resourceReleased(resultResource: ResultResource): Unit = {
    sender.send(ResourceReleased(resultResource, moduleInstance))
  }

  def isInstanceCanService(): Boolean = sender.ask(ResourceOverload(moduleInstance)).asInstanceOf[Boolean]

  def getModuleResourceInfo: ResourceInfo = sender.ask(moduleInstance).asInstanceOf[ResourceInfo]

}