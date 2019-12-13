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

package com.webank.wedatasphere.linkis.gateway.route

import com.webank.wedatasphere.linkis.common.ServiceInstance
import com.webank.wedatasphere.linkis.common.utils.{Logging, Utils}
import com.webank.wedatasphere.linkis.gateway.exception.TooManyServiceException
import com.webank.wedatasphere.linkis.gateway.http.GatewayContext
import com.webank.wedatasphere.linkis.rpc.interceptor.ServiceInstanceUtils
import com.webank.wedatasphere.linkis.rpc.sender.SpringCloudFeignConfigurationCache
import com.webank.wedatasphere.linkis.server.Message
import com.webank.wedatasphere.linkis.server.exception.NoApplicationExistsException
import org.apache.commons.lang.StringUtils
import org.apache.commons.lang.exception.ExceptionUtils

/**
  * created by cooperyang on 2019/1/9.
  */
trait GatewayRouter {

  def route(gatewayContext: GatewayContext): ServiceInstance

}
abstract class AbstractGatewayRouter extends GatewayRouter with Logging {
  import scala.collection.JavaConversions._
  protected def findAndRefreshIfNotExists(serviceId: String, findService: => Option[String]): Option[String] = {
    var service = findService
    if(service.isEmpty) {
      val applicationNotExists = new NoApplicationExistsException(10050, "Application " +
        serviceId + " is not exists any instances.")
      //如果没发现server，refreshServiceInstances  直到超时
      Utils.tryThrow(Utils.waitUntil(() =>{
        //主要是调用eureka的刷新
        ServiceInstanceUtils.refreshServiceInstances()
        service = findService
        service.nonEmpty
      }, ServiceInstanceUtils.serviceRefreshMaxWaitTime(), 500, 2000)){ t =>
        warn(s"Need a random $serviceId instance, but no one can find in Discovery refresh.", t)
        applicationNotExists.initCause(t)
        applicationNotExists
      }
    }
    service
  }
  //parsedServiceId 被解析后的serviceId，一般是url中的名字，publicSercice除外
  protected def findService(parsedServiceId: String, tooManyDeal: List[String] => Option[String]): Option[String] = {
    val services = SpringCloudFeignConfigurationCache.getDiscoveryClient
      .getServices.filter(_.toLowerCase.contains(parsedServiceId.toLowerCase)).toList
    if(services.length == 1) Some(services.head)
    else if(services.length > 1) tooManyDeal(services) //如果发现service的数量>1则抛出异常。。所以多sparkEM 应该是名字不同的
    else None
  }
}
//GatewayRouter 的默认实现
class DefaultGatewayRouter(gatewayRouters: Array[GatewayRouter]) extends AbstractGatewayRouter{

  private def findCommonService(parsedServiceId: String) = findService(parsedServiceId, services => {
    val errorMsg = new TooManyServiceException(s"Cannot find a correct serviceId for parsedServiceId $parsedServiceId, service list is: " + services)
    warn("", errorMsg)
    throw errorMsg
  })
  //rout-------------》findAndRefreshIfNotExists---》findReallyService-》findCommonService--》findService
  protected def findReallyService(gatewayContext: GatewayContext): ServiceInstance = {
    var serviceInstance: ServiceInstance = null
    for (router <- gatewayRouters if serviceInstance == null) {
      serviceInstance = router.route(gatewayContext)
    }
    if(serviceInstance == null) serviceInstance = gatewayContext.getGatewayRoute.getServiceInstance
    val service = findAndRefreshIfNotExists(serviceInstance.getApplicationName,
      findCommonService(serviceInstance.getApplicationName))
    service.map { applicationName =>
      if(StringUtils.isNotBlank(serviceInstance.getInstance)) {
        val _serviceInstance = ServiceInstance(applicationName, serviceInstance.getInstance)
        ServiceInstanceUtils.getRPCServerLoader.getOrRefreshServiceInstance(_serviceInstance)
        _serviceInstance
      } else ServiceInstance(applicationName, null)
    }.get
  }
  //主要作用是根据getApplicationName  找到相应的server服务，接着返回ServiceInstance
  override def route(gatewayContext: GatewayContext): ServiceInstance = if(gatewayContext.getGatewayRoute.getServiceInstance != null) {
    //经过GatewayParser 后getServiceInstance应该是有值的，applicationname
    val parsedService = gatewayContext.getGatewayRoute.getServiceInstance.getApplicationName
    val serviceInstance = Utils.tryCatch(findReallyService(gatewayContext)){ t =>
      val message = Message.error(ExceptionUtils.getRootCauseMessage(t)) << gatewayContext.getRequest.getRequestURI
      message.data("data", gatewayContext.getRequest.getRequestBody)
      warn("", t)
      if(gatewayContext.isWebSocketRequest) gatewayContext.getResponse.writeWebSocket(message) else gatewayContext.getResponse.write(message)
      gatewayContext.getResponse.sendResponse()
      return null
    }
    info("GatewayRouter route requestUri " + gatewayContext.getGatewayRoute.getRequestURI + " with parsedService " + parsedService + " to " + serviceInstance)
    serviceInstance
  } else null
}