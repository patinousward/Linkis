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

package com.webank.wedatasphere.linkis.rpc.sender

import java.lang.reflect.Field

import com.netflix.client.ClientRequest
import com.netflix.client.config.IClientConfig
import com.netflix.loadbalancer.reactive.LoadBalancerCommand
import com.webank.wedatasphere.linkis.common.ServiceInstance
import com.webank.wedatasphere.linkis.common.conf.{Configuration => DWCConfiguration}
import com.webank.wedatasphere.linkis.protocol.Protocol
import com.webank.wedatasphere.linkis.rpc.interceptor.{BaseRPCInterceptorChain, RPCInterceptor, RPCLoadBalancer, ServiceInstanceRPCInterceptorChain}
import com.webank.wedatasphere.linkis.rpc.transform.RPCConsumer
import com.webank.wedatasphere.linkis.rpc.{BaseRPCSender, RPCMessageEvent, RPCSpringBeanCache}
import com.webank.wedatasphere.linkis.server.{BDPJettyServerHelper, Message}
import feign._
import org.apache.commons.lang.StringUtils
import org.springframework.cloud.netflix.ribbon.ServerIntrospector
import org.springframework.cloud.openfeign.ribbon.{CachingSpringLoadBalancerFactory, FeignLoadBalancer, LoadBalancerFeignClient}

/**
  * Created by enjoyyin on 2018/8/28.
  */
private[rpc] class SpringMVCRPCSender private[rpc](private[rpc] val serviceInstance: ServiceInstance)
  extends BaseRPCSender(serviceInstance.getApplicationName) {

  import SpringCloudFeignConfigurationCache._

  override protected def getRPCInterceptors: Array[RPCInterceptor] = RPCSpringBeanCache.getRPCInterceptors

  override protected def createRPCInterceptorChain() = new ServiceInstanceRPCInterceptorChain(0, getRPCInterceptors, serviceInstance)
  //getRPCLoadBalancers 这里是自己定义的rpc负载均衡，默认有2个
  protected def getRPCLoadBalancers: Array[RPCLoadBalancer] = RPCSpringBeanCache.getRPCLoadBalancers

  override protected def doBuilder(builder: Feign.Builder): Unit = {
    val client = getClient.asInstanceOf[LoadBalancerFeignClient]
    //用旧的client的Delegate重新封装一个LoadBalancerFeignClient
    val newClient = new LoadBalancerFeignClient(client.getDelegate, new CachingSpringLoadBalancerFactory(getClientFactory) {
      //CachingSpringLoadBalancerFactory中的create方法用于创建FeignLoadBalancer
      override def create(clientName: String): FeignLoadBalancer = {
        //获取一个内省器
        val serverIntrospector = getClientFactory.getInstance(clientName, classOf[ServerIntrospector])
        //这个FeignLoadBalancer的匿名实现就是最后返回的对象
        new FeignLoadBalancer(getClientFactory.getLoadBalancer(clientName), getClientFactory.getClientConfig(clientName), serverIntrospector) {
          override def customizeLoadBalancerCommandBuilder(request: FeignLoadBalancer.RibbonRequest, config: IClientConfig,
                                                           builder: LoadBalancerCommand.Builder[FeignLoadBalancer.RibbonResponse]): Unit = {
            //getRPCLoadBalancers 这里是自己定义的rpc负载均衡，默认有2个，InstanceRPCLoader和SingleInstanceRPCLoadBalancer
            val instance = if(getRPCLoadBalancers.isEmpty) None else {
              val requestBody = SpringMVCRPCSender.getRequest(request).body()
              val requestStr = new String(requestBody, DWCConfiguration.BDP_ENCODING.getValue)
              //拿到Message并且反序列化成protocol对象
              val obj = RPCConsumer.getRPCConsumer.toObject(BDPJettyServerHelper.gson.fromJson(requestStr, classOf[Message]))
              obj match {
                case protocol: Protocol =>
                  var serviceInstance: Option[ServiceInstance] = None
                  for (lb <- getRPCLoadBalancers if serviceInstance.isEmpty)
                    //这里的getLoadBalancer 是netflix自己的lb
                    serviceInstance = lb.choose(protocol, SpringMVCRPCSender.this.serviceInstance, getLoadBalancer)
                  serviceInstance.foreach(f =>
                    info("origin serviceInstance: " + SpringMVCRPCSender.this.serviceInstance + ", chose serviceInstance: " + f)) //TODO just for test
                  serviceInstance
                case _ => None//如果protocol实体bean不继承Protocol类，一般就走这一步，那看来发送给谁是netflix自己处理的
              }
            }
            //serviceInstance  一般情况下instance信息为null，表示可以使用Ribbon默认的轮询，这里如果instance不为null，代表
            //1.刷新clinet的服务器列表，拿到servers的ip和端口，如果instance不为null，就找到这个server去请求，只会有一个，因为ip + 端口能确认一个server
            //至于哪里使用了这个方法，就可以看哪个地方使用了Sender.getSender(serviceInstance: ServiceInstance) 这个方法
            instance.orElse(Option(SpringMVCRPCSender.this.serviceInstance)).filter(s => StringUtils.isNotBlank(s.getInstance))
              .foreach { serviceInstance =>
                val server = RPCSpringBeanCache.getRPCServerLoader.getServer(getLoadBalancer, serviceInstance)
                builder.withServer(server)//如果有指定server，就覆盖掉相应的
              }
          }
        }
      }
    }, getClientFactory)
    super.doBuilder(builder)
    builder.contract(getContract)
      .encoder(getEncoder).decoder(getDecoder)
      .client(newClient).requestInterceptor(getRPCTicketIdRequestInterceptor)
  }


  /**
    * Deliver is an asynchronous method that requests the target microservice asynchronously, ensuring that the target microservice is requested once,
    * but does not guarantee that the target microservice will successfully receive the request.
    * deliver是一个异步方法，该方法异步请求目标微服务，确保一定会请求目标微服务一次，但不保证目标微服务一定能成功接收到本次请求。
    * @param message Requested parameters(请求的参数)
    */
  override def deliver(message: Any): Unit = getRPCSenderListenerBus.post(RPCMessageEvent(message, serviceInstance))

  override def equals(obj: Any): Boolean = if(obj == null) false
    else obj match {
      case sender: SpringMVCRPCSender => sender.serviceInstance == serviceInstance
      case _ => false
    }

  override def hashCode(): Int = serviceInstance.hashCode()

  override val toString: String = if(StringUtils.isBlank(serviceInstance.getInstance)) s"RPCSender(${serviceInstance.getApplicationName})"
    else s"RPCSender($getApplicationName, ${serviceInstance.getInstance})"
}
private object SpringMVCRPCSender {
  private var requestField: Field = _
  //反射获取ClientRequest中的feign.Request对象
  def getRequest(req: ClientRequest): Request = {
    if(requestField == null) synchronized {
      if(requestField == null) {
        requestField = req.getClass.getDeclaredField("request")
        requestField.setAccessible(true)
      }
    }
    requestField.get(req).asInstanceOf[Request]
  }
}