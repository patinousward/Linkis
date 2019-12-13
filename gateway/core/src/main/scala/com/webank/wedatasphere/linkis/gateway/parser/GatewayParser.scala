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

package com.webank.wedatasphere.linkis.gateway.parser

import com.webank.wedatasphere.linkis.DataWorkCloudApplication
import com.webank.wedatasphere.linkis.common.ServiceInstance
import com.webank.wedatasphere.linkis.common.utils.Logging
import com.webank.wedatasphere.linkis.gateway.http.{GatewayContext, GatewayRoute}
import com.webank.wedatasphere.linkis.rpc.conf.RPCConfiguration
import com.webank.wedatasphere.linkis.rpc.interceptor.ServiceInstanceUtils
import com.webank.wedatasphere.linkis.server.Message
import com.webank.wedatasphere.linkis.server.conf.ServerConfiguration
import org.apache.commons.lang.StringUtils

/**
  * created by cooperyang on 2019/1/9.
  */
trait GatewayParser {

  def shouldContainRequestBody(gatewayContext: GatewayContext): Boolean

  def parse(gatewayContext: GatewayContext): Unit

}
abstract class AbstractGatewayParser extends GatewayParser with Logging {

  //请求不满足version要求的时候进行返回错误信息
  protected def sendResponseWhenNotMatchVersion(gatewayContext: GatewayContext, version: String): Boolean = if(version != ServerConfiguration.BDP_SERVER_VERSION) {
    warn(s"Version not match. The gateway(${ServerConfiguration.BDP_SERVER_VERSION}) not support requestUri ${gatewayContext.getRequest.getRequestURI} from remoteAddress ${gatewayContext.getRequest.getRemoteAddress.getAddress.getHostAddress}.")
    sendErrorResponse(s"The gateway${ServerConfiguration.BDP_SERVER_VERSION} not support version $version.", gatewayContext)
    true
  } else false

  protected def sendErrorResponse(errorMsg: String, gatewayContext: GatewayContext): Unit = sendMessageResponse(Message.error(errorMsg), gatewayContext)
  //返回的主要调用的方法
  protected def sendMessageResponse(dataMsg: Message, gatewayContext: GatewayContext): Unit = {
    gatewayContext.setGatewayRoute(new GatewayRoute)
    gatewayContext.getGatewayRoute.setRequestURI(gatewayContext.getRequest.getRequestURI)
    dataMsg << gatewayContext.getRequest.getRequestURI//设置返回json中method的值，就是请求地址
    if(dataMsg.getStatus != 0) warn(dataMsg.getMessage)
    //ws 请求的话，writeWebSocket只是把message写在缓存中，而且sendResponse并没有发送信息
    //如果是http请求的话，write(dataMsg)将信息写到缓存中，
    if(gatewayContext.isWebSocketRequest) gatewayContext.getResponse.writeWebSocket(dataMsg)
    else gatewayContext.getResponse.write(dataMsg)
    //.sendResponse()  http请求的话，将message写给浏览器，而wbsoket则啥都没做
    gatewayContext.getResponse.sendResponse()
  }

  /**
    * Return to the gateway list information(返回gateway列表信息)
    */
  protected def responseHeartbeat(gatewayContext: GatewayContext): Unit = {
    //获取全部名为gateway的服务的instance，然后返回gateway的列表，并且标注是健康的
    val gatewayServiceInstances = ServiceInstanceUtils.getRPCServerLoader.getServiceInstances(DataWorkCloudApplication.getApplicationName)
    val msg = Message.ok("Gateway heartbeat ok!").data("gatewayList", gatewayServiceInstances.map(_.getInstance)).data("isHealthy", true)
    sendMessageResponse(msg, gatewayContext)
  }
}
object AbstractGatewayParser {
  val GATEWAY_HEART_BEAT_URL = Array("gateway", "heartbeat")
}
class DefaultGatewayParser(gatewayParsers: Array[GatewayParser]) extends AbstractGatewayParser {

  private val COMMON_REGEX = "/api/rest_[a-zA-Z]+/(v\\d+)/([^/]+)/.+".r
  private val CLIENT_HEARTBEAT_REGEX = s"/api/rest_[a-zA-Z]+/(v\\d+)/${AbstractGatewayParser.GATEWAY_HEART_BEAT_URL.mkString("/")}".r

  /**
    * 是否包含请求体，如果是get请求，直接返回false
    * 如果是其他post，put等请求，还需要判断，如果是以/user/。。开头的路径，返回true
    * @param gatewayContext
    * @return
    */
  override def shouldContainRequestBody(gatewayContext: GatewayContext): Boolean = gatewayContext.getRequest.getMethod.toUpperCase != "GET" &&
    (gatewayContext.getRequest.getRequestURI match {
    case uri if uri.startsWith(ServerConfiguration.BDP_SERVER_USER_URI.getValue) => true
    case _ => gatewayParsers.exists(_.shouldContainRequestBody(gatewayContext))
  })
  //对请求的url进行解析
  override def parse(gatewayContext: GatewayContext): Unit = {
    val path = gatewayContext.getRequest.getRequestURI
    if(gatewayContext.getGatewayRoute == null) {
      gatewayContext.setGatewayRoute(new GatewayRoute)
      gatewayContext.getGatewayRoute.setRequestURI(path)
    }
    gatewayParsers.foreach(_.parse(gatewayContext))
    if(gatewayContext.getGatewayRoute.getServiceInstance == null) path match {
      case CLIENT_HEARTBEAT_REGEX(version) =>  //如果是gateway之间的心跳
        if(sendResponseWhenNotMatchVersion(gatewayContext, version)) return
        info(gatewayContext.getRequest.getRemoteAddress + " try to heartbeat.")
        responseHeartbeat(gatewayContext)  //返回建康的gateway列表
      case COMMON_REGEX(version, serviceId) => //如果是普通的请求，只是设置一个Instance到GatewayRoute
        if(sendResponseWhenNotMatchVersion(gatewayContext, version)) return
        val applicationName = if(RPCConfiguration.ENABLE_PUBLIC_SERVICE.getValue && RPCConfiguration.PUBLIC_SERVICE_LIST.contains(serviceId))
          RPCConfiguration.PUBLIC_SERVICE_APPLICATION_NAME.getValue else serviceId
        gatewayContext.getGatewayRoute.setServiceInstance(ServiceInstance(applicationName, null))
      case p if p.startsWith("/dws/") =>
        //TODO add version support
        val params = gatewayContext.getGatewayRoute.getParams
        params.put("proxyId", "dws")
        val secondaryProxyId = StringUtils.substringBetween(p, "/dws/", "/")
        if(StringUtils.isNotBlank(secondaryProxyId)){
          params.put("proxyId", "dws/" + secondaryProxyId)
        }
        gatewayContext.getGatewayRoute.setParams(params)
      case _ =>
        sendErrorResponse(s"Cannot find a service to deal $path, ignore it.", gatewayContext)
    }
  }
}