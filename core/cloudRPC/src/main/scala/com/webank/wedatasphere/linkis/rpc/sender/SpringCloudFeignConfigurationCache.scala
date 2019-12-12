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

import com.webank.wedatasphere.linkis.DataWorkCloudApplication
import com.webank.wedatasphere.linkis.rpc.{RPCReceiveRestful, RPCSpringBeanCache, Receiver}
import feign.codec.{Decoder, Encoder}
import feign.{Client, Contract}
import javax.annotation.PostConstruct
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.boot.autoconfigure.{AutoConfigureAfter, AutoConfigureBefore}
import org.springframework.cloud.client.discovery.DiscoveryClient
import org.springframework.cloud.client.loadbalancer.LoadBalancedRetryFactory
import org.springframework.cloud.netflix.ribbon.SpringClientFactory
import org.springframework.cloud.openfeign.FeignClientsConfiguration
import org.springframework.context.annotation.{Configuration, Import}

/**
  * Created by enjoyyin on 2019/1/14.
  */
@Import(Array(classOf[FeignClientsConfiguration]))
//import 后，可以使用这个configuration的单例类，通过autowire的方式（所以这里构造器要标注autowire）或者getBean（class）的方式，主要是获取client（LoadBalancerFeignClient），Encoder，Decoder和Contract
//？？LoadBalancerFeignClient 可能在openFeigen中有自己的单例，这里要看下feign的源码
@Autowired//其实是类似标注在构造器上，为了注入构造器中的所有参数
@Configuration
@AutoConfigureBefore(Array(classOf[Receiver], classOf[RPCReceiveRestful]))//在这两个单例前加载这个类
class SpringCloudFeignConfigurationCache(encoder: Encoder, decoder: Decoder,
                                         contract: Contract, client: Client) {

  @Autowired
  private var discoveryClient: DiscoveryClient = _
  @Autowired
  private var clientFactory: SpringClientFactory = _
  @Autowired(required = false)
  private var loadBalancedRetryFactory: LoadBalancedRetryFactory = _

  @PostConstruct
  def storeFeignConfiguration(): Unit = {
    SpringCloudFeignConfigurationCache.client = client
    SpringCloudFeignConfigurationCache.clientFactory = clientFactory
    SpringCloudFeignConfigurationCache.loadBalancedRetryFactory = loadBalancedRetryFactory
    SpringCloudFeignConfigurationCache.contract = contract
    SpringCloudFeignConfigurationCache.decoder = decoder
    SpringCloudFeignConfigurationCache.encoder = encoder
    SpringCloudFeignConfigurationCache.discoveryClient = discoveryClient
  }

}
private[linkis] object SpringCloudFeignConfigurationCache {
  private[SpringCloudFeignConfigurationCache] var encoder: Encoder = _
  private[SpringCloudFeignConfigurationCache] var decoder: Decoder = _
  private[SpringCloudFeignConfigurationCache] var contract: Contract = _
  private[SpringCloudFeignConfigurationCache] var client: Client = _
  private[SpringCloudFeignConfigurationCache] var clientFactory: SpringClientFactory = _
  private[SpringCloudFeignConfigurationCache] var loadBalancedRetryFactory: LoadBalancedRetryFactory = _
  private[SpringCloudFeignConfigurationCache] var discoveryClient: DiscoveryClient = _
  private val rpcTicketIdRequestInterceptor = new FeignClientRequestInterceptor

  private[rpc] def getEncoder = encoder
  private[rpc] def getDecoder = decoder
  private[rpc] def getContract = contract
  private[rpc] def getClient = {
    //静态类先于对象加载，这个判断空是为了避免在容器生成前直接使用，下面的也同理
    if(client == null) DataWorkCloudApplication.getApplicationContext.getBean(classOf[SpringCloudFeignConfigurationCache])
    client
  }
  private[rpc] def getClientFactory = clientFactory
  private[rpc] def getLoadBalancedRetryFactory = loadBalancedRetryFactory
  private[linkis] def getDiscoveryClient = {
    if(discoveryClient == null) DataWorkCloudApplication.getApplicationContext.getBean(classOf[SpringCloudFeignConfigurationCache])
    discoveryClient
  }
  private[rpc] def getRPCTicketIdRequestInterceptor = rpcTicketIdRequestInterceptor
}