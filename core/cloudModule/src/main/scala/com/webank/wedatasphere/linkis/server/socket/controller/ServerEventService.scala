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

package com.webank.wedatasphere.linkis.server.socket.controller

import com.google.gson.Gson
import com.webank.wedatasphere.linkis.common.listener.{Event, EventListener}
import com.webank.wedatasphere.linkis.common.utils.Logging
import com.webank.wedatasphere.linkis.server.{BDPJettyServerHelper, Message, catchIt}

/**
  * Created by enjoyyin on 2018/1/11.
  */
abstract class ServerEventService extends EventListener with Logging {
  //这个类被 设计为初始化的时候才注册到ServerListenerEventBus中
  //ServerlistenerEvnetBus作爲单例存在ControllerServerr和BDPJettyServerHelper中
  //ControllerServer也在 BDPJettyServerHelper中

  protected val gson: Gson = BDPJettyServerHelper.gson
//所以后台推送给前台的信息主要有2种途径，1是这里的方法sendMessage进行推送
  //2是前台使用websoket请求execute的接口的时候，从ControllerServer的onMessage  -->entrance提交job返回taskId--> 推送回前台
  protected def sendMessage(id: Int, message: Message) = BDPJettyServerHelper.getControllerServer.sendMessage(id, message)

  protected def sendMessageToUser(user: String, message: Message): Unit = BDPJettyServerHelper.getControllerServer.sendMessageToUser(user, message)

  protected def sendMessageToAll(message: Message): Unit = BDPJettyServerHelper.getControllerServer.sendMessageToAll(message)

  val serviceName: String

  info("add a socket ServerEventService: " + getClass.getName)
  //类创建对象的时候就将实现类对象注入listenerBus
  BDPJettyServerHelper.addServerEventService(this)

  def onEvent(event: ServerEvent): Message

  def onEventError(event: Event, t: Throwable): Unit = event match {
    case e: SocketServerEvent => onEventError(e, t)
    case _ => error(s"cannot recognize the event type $event.", t)
  }

  def onEventError(event: SocketServerEvent, t: Throwable): Unit = {
    val message = catchIt(throw t)
    event.socket.sendMessage(message << event.serverEvent.getMethod)
  }

}
