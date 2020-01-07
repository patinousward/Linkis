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

package com.webank.wedatasphere.linkis.resourcemanager.domain

import org.json4s.{CustomSerializer, DefaultFormats, Extraction}
import org.json4s.JsonAST.JObject
import com.webank.wedatasphere.linkis.resourcemanager.{Resource, ResourceRequestPolicy, ResourceSerializer}
import com.webank.wedatasphere.linkis.resourcemanager.ResourceRequestPolicy.ResourceRequestPolicy
import com.webank.wedatasphere.linkis.common.ServiceInstance

/**
  * Created by shanhuang on 9/11/18.
  */
/**
  *
  * @param moduleInstance
  * @param totalResource
  * @param protectedResource Less than this value enters protection(小于该值就进入保护)
  * @param resourceRequestPolicy
  */
case class ModuleInfo(moduleInstance: ServiceInstance,
                      totalResource: Resource,
                      protectedResource: Resource, //(当资源剩下多少时，进入保护模式)
                      resourceRequestPolicy: ResourceRequestPolicy//资源策略val Default, Memory, CPU, Load, Instance, LoadInstance, Yarn, DriverAndYarn, Special = Value
                     )
//json4s中自定义ModuleInfo的序列化
object ModuleInfoSerializer extends CustomSerializer[ModuleInfo](implicit formats => ( {
  case JObject(List(("moduleInstance", moduleInstance), ("totalResource", totalResource), ("protectedResource", protectedResource), ("resourceRequestPolicy", resourceRequestPolicy))) =>
    implicit val formats = DefaultFormats + ResourceSerializer + ModuleInstanceSerializer
    new ModuleInfo(moduleInstance.extract[ServiceInstance], totalResource.extract[Resource], protectedResource.extract[Resource], ResourceRequestPolicy.withName(resourceRequestPolicy.extract[String]))
}, {
  case i: ModuleInfo =>
    implicit val formats = DefaultFormats + ResourceSerializer + ModuleInstanceSerializer
    val policy = Extraction.decompose(i.resourceRequestPolicy.toString)
    new JObject(List(("moduleInstance", Extraction.decompose(i.moduleInstance)), ("totalResource", Extraction.decompose(i.totalResource)), ("protectedResource", Extraction.decompose(i.protectedResource)), ("resourceRequestPolicy", policy)))
})
)