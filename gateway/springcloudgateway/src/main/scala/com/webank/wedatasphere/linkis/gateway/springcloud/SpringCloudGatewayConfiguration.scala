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

package com.webank.wedatasphere.linkis.gateway.springcloud

import com.netflix.loadbalancer.Server
import com.webank.wedatasphere.linkis.common.ServiceInstance
import com.webank.wedatasphere.linkis.common.conf.CommonVars
import com.webank.wedatasphere.linkis.common.utils.Logging
import com.webank.wedatasphere.linkis.gateway.config.GatewaySpringConfiguration
import com.webank.wedatasphere.linkis.gateway.parser.{DefaultGatewayParser, GatewayParser}
import com.webank.wedatasphere.linkis.gateway.route.{DefaultGatewayRouter, GatewayRouter}
import com.webank.wedatasphere.linkis.gateway.springcloud.http.GatewayAuthorizationFilter
import com.webank.wedatasphere.linkis.gateway.springcloud.websocket.SpringCloudGatewayWebsocketFilter
import com.webank.wedatasphere.linkis.rpc.Sender
import com.webank.wedatasphere.linkis.server.conf.ServerConfiguration
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.boot.autoconfigure.AutoConfigureAfter
import org.springframework.cloud.client.loadbalancer.LoadBalancerClient
import org.springframework.cloud.gateway.config.{GatewayAutoConfiguration, GatewayProperties}
import org.springframework.cloud.gateway.filter._
import org.springframework.cloud.gateway.route.builder.{PredicateSpec, RouteLocatorBuilder}
import org.springframework.cloud.gateway.route.{Route, RouteLocator}
import org.springframework.cloud.netflix.ribbon.{RibbonLoadBalancerClient, SpringClientFactory}
import org.springframework.context.annotation.{Bean, Configuration}
import org.springframework.web.reactive.socket.client.WebSocketClient
import org.springframework.web.reactive.socket.server.WebSocketService

import scala.collection.JavaConversions._


//---代表依赖
// GatewayAutoConfiguration-->GatewayLoadBalancerClientAutoConfiguration--> RibbonAutoConfiguration
//gateway 走向  RouteLocator--->filter
//推测: RouteLocator 由id,url,断言器(有path断言(这里用的),header断言等等)组成
//当请求进来的时候,会由RouteLocator断言进行判断,然后走不同的url,然后会将这个请求封装为ServerWebExchange,然后将RouteLocator中的
//Route也封装进入ServerWebExchange中

/**
  * created by cooperyang on 2019/1/9.
  */
@Configuration
//GatewayAutoConfiguration  中会创建@bean GatewayProperties 所以可以直接autowire注入
//
@AutoConfigureAfter(Array(classOf[GatewaySpringConfiguration], classOf[GatewayAutoConfiguration]))
class SpringCloudGatewayConfiguration {
  //import 伴生类,可以调用其中的常量
  import SpringCloudGatewayConfiguration._
  @Autowired(required = false)
  private var gatewayParsers: Array[GatewayParser] = _
  @Autowired(required = false)
  private var gatewayRouters: Array[GatewayRouter] = _
  @Autowired
  private var gatewayProperties: GatewayProperties = _  //GatewayProperties主要是读取application.yam中的配置

  @Bean
  def authorizationFilter: GlobalFilter = new GatewayAuthorizationFilter(new DefaultGatewayParser(gatewayParsers), new DefaultGatewayRouter(gatewayRouters), gatewayProperties)

  //WebsocketRoutingFilter(gateway自己的类) 顺序是Int.MAX
  // SpringCloudGatewayWebsocketFilter  的顺序是WebsocketRoutingFilter -1  比WebsocketRoutingFilter早
  //GatewayAuthorizationFilter 的顺序是1
  //WebsocketRoutingFilter,SpringCloudGatewayWebsocketFilter 均implementGlobalFilter, Ordered


  //WebsocketRoutingFilter  bean在GatewayAutoConfiguration中有注入,写在参数中并不需要@autowire就可以注入
  //WebSocketClient         bean在GatewayAutoConfiguration中有注入   (ReactorNettyWebSocketClient)
  //WebSocketService        bean在GatewayAutoConfiguration中有注入
  //LoadBalancerClient      beab 在RibbonAutoConfiguration 中有注入,但是被本类中的createLoadBalancerClient 给覆盖了
  @Bean
  def websocketFilter(websocketRoutingFilter: WebsocketRoutingFilter,
                      webSocketClient: WebSocketClient, webSocketService: WebSocketService,
                      loadBalancer: LoadBalancerClient): GlobalFilter = new SpringCloudGatewayWebsocketFilter(websocketRoutingFilter,
    webSocketClient, webSocketService, loadBalancer, new DefaultGatewayParser(gatewayParsers), new DefaultGatewayRouter(gatewayRouters))

  //RouteLocatorBuilder     bean在GatewayAutoConfiguration中有注入
  //GatewayAutoConfiguration中已经有RouteLocator了(但是从下面的源码看出,可以接受List的RouteLocator),然后最后还是以@primary为主
  //但是确实是注入进去了,只是实际使用的时候是以这个primary的为主
/*  @Bean
  @Primary
  public RouteLocator cachedCompositeRouteLocator(List<RouteLocator> routeLocators) {
    return new CachingRouteLocator(new CompositeRouteLocator(Flux.fromIterable(routeLocators)));
  }*/
  @Bean
  def createRouteLocator(builder: RouteLocatorBuilder): RouteLocator = builder.routes()
      .route("api", new java.util.function.Function[PredicateSpec, Route.AsyncBuilder] {
        //一般restful请求走这里 lb://
        override def apply(t: PredicateSpec): Route.AsyncBuilder = t.path(API_URL_PREFIX + "**")
          .uri(ROUTE_URI_FOR_HTTP_HEADER + Sender.getThisServiceInstance.getApplicationName)
      })
      .route("dws", new java.util.function.Function[PredicateSpec, Route.AsyncBuilder] {
        override def apply(t: PredicateSpec): Route.AsyncBuilder = t.path(PROXY_URL_PREFIX + "**")
          .uri(ROUTE_URI_FOR_HTTP_HEADER + Sender.getThisServiceInstance.getApplicationName)
      })
      .route("ws_http", new java.util.function.Function[PredicateSpec, Route.AsyncBuilder] {
      override def apply(t: PredicateSpec): Route.AsyncBuilder = t.path(SpringCloudGatewayConfiguration.WEBSOCKET_URI + "info/**")
        .uri(ROUTE_URI_FOR_HTTP_HEADER + Sender.getThisServiceInstance.getApplicationName)
      })
      .route("ws", new java.util.function.Function[PredicateSpec, Route.AsyncBuilder] {
        //一般ws走这里lb://ws
        override def apply(t: PredicateSpec): Route.AsyncBuilder = t.path(SpringCloudGatewayConfiguration.WEBSOCKET_URI + "**")
          .uri(ROUTE_URI_FOR_WEB_SOCKET_HEADER + Sender.getThisServiceInstance.getApplicationName)
      }).build()

  //SpringClientFactory  在RibbonAutoConfiguration中有注入
  @Bean
  def createLoadBalancerClient(springClientFactory: SpringClientFactory) = new RibbonLoadBalancerClient(springClientFactory) {
    //重写了getServer的方法
    override def getServer(serviceId: String): Server = if(isMergeModuleInstance(serviceId)) {
      //如果serviceId 是合并ModuleInstance(??什么时候会合并)
      //解析serviceId,获取appName,返回一个serviceInstance对象
      val serviceInstance = getServiceInstance(serviceId)
      info("redirect to " + serviceInstance)  //TODO test,wait for delete
      val lb = this.getLoadBalancer(serviceInstance.getApplicationName)  //通过服务名获取loadbanlance
      lb.getAllServers.find(_.getHostPort == serviceInstance.getInstance).get //找到一个符合applicationName的存在
    } else super.getServer(serviceId)  //其余走父类
  }

}
object SpringCloudGatewayConfiguration extends Logging {
  private val MERGE_MODULE_INSTANCE_HEADER = "merge-gw-"
  val ROUTE_URI_FOR_HTTP_HEADER = "lb://"
  val ROUTE_URI_FOR_WEB_SOCKET_HEADER = "lb:ws://"                  //lb应该是loadbalance?
  val PROXY_URL_PREFIX = "/dws/"
  val API_URL_PREFIX = "/api/"
  val PROXY_ID = "proxyId"

  val WEBSOCKET_URI = normalPath(ServerConfiguration.BDP_SERVER_SOCKET_URI.getValue)
  def normalPath(path: String): String = if(path.endsWith("/")) path else path + "/"

  def isMergeModuleInstance(serviceId: String): Boolean = serviceId.startsWith(MERGE_MODULE_INSTANCE_HEADER)

  private val regex = "(\\d+).+".r
  //解析serviceId
  def getServiceInstance(serviceId: String): ServiceInstance = {
    var serviceInstanceString = serviceId.substring(MERGE_MODULE_INSTANCE_HEADER.length)
    serviceInstanceString match {
      case regex(num) =>
        serviceInstanceString = serviceInstanceString.substring(num.length)
        ServiceInstance(serviceInstanceString.substring(0, num.toInt), serviceInstanceString.substring(num.toInt).replaceAll("---", ":"))
    }
  }
  //加密serviceId
  def mergeServiceInstance(serviceInstance: ServiceInstance): String = MERGE_MODULE_INSTANCE_HEADER + serviceInstance.getApplicationName.length +
    serviceInstance.getApplicationName + serviceInstance.getInstance.replaceAll(":", "---")
}