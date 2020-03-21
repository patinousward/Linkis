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

package com.webank.wedatasphere.linkis.rpc.conf

import com.netflix.discovery.EurekaClient
import com.webank.wedatasphere.linkis.DataWorkCloudApplication
import com.webank.wedatasphere.linkis.common.utils.Logging
import com.webank.wedatasphere.linkis.rpc.RPCReceiveRestful
import com.webank.wedatasphere.linkis.rpc.interceptor.RPCServerLoader
import com.webank.wedatasphere.linkis.rpc.sender.eureka.EurekaRPCServerLoader
import com.webank.wedatasphere.linkis.server.conf.ServerConfiguration
import org.apache.commons.lang.StringUtils
import org.springframework.boot.autoconfigure.condition.{ConditionalOnClass, ConditionalOnMissingBean}
import org.springframework.boot.context.event.ApplicationPreparedEvent
import org.springframework.cloud.openfeign.EnableFeignClients
import org.springframework.context.annotation.{Bean, Configuration}
import org.springframework.context.event.EventListener

/**
  * Created by enjoyyin on 2019/1/14.
  */
@Configuration
@EnableFeignClients//去掉这个注解ok吗？不行。。因为rpc发送需要依靠openfein
class RPCSpringConfiguration extends Logging {

  @Bean(Array("rpcServerLoader"))
  @ConditionalOnClass(Array(classOf[EurekaClient]))
  @ConditionalOnMissingBean
  def createRPCServerLoader(): RPCServerLoader = new EurekaRPCServerLoader //gateway会用上这个bean

  @EventListener
  //ApplicationPreparedEvent
/*  SpringBoot Application共支持6种事件监听，按顺序分别是：

  ApplicationStartingEvent：在Spring最开始启动的时候触发
  ApplicationEnvironmentPreparedEvent：在Spring已经准备好上下文但是上下文尚未创建的时候触发
  ApplicationPreparedEvent：在Bean定义加载之后、刷新上下文之前触发
  ApplicationStartedEvent：在刷新上下文之后、调用application命令之前触发
  ApplicationReadyEvent：在调用applicaiton命令之后触发
  ApplicationFailedEvent：在启动Spring发生异常时触发*/
  def completeInitialize(applicationPreparedEvent: ApplicationPreparedEvent): Unit = {
    val restfulClasses = ServerConfiguration.BDP_SERVER_RESTFUL_REGISTER_CLASSES.getValue
    if(StringUtils.isEmpty(restfulClasses))
      DataWorkCloudApplication.setProperty(ServerConfiguration.BDP_SERVER_RESTFUL_REGISTER_CLASSES.key, classOf[RPCReceiveRestful].getName)
    else
      DataWorkCloudApplication.setProperty(ServerConfiguration.BDP_SERVER_RESTFUL_REGISTER_CLASSES.key, restfulClasses +
        "," + classOf[RPCReceiveRestful].getName)
    info("DataWorkCloud RPC need register RPCReceiveRestful, now add it to configuration.")
  }

}
