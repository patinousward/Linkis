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

package com.webank.wedatasphere.linkis.server.socket

import java.util
import java.util.concurrent.atomic.AtomicInteger

import com.webank.wedatasphere.linkis.common.conf.Configuration.DEFAULT_DATE_PATTERN
import com.webank.wedatasphere.linkis.common.listener.Event
import com.webank.wedatasphere.linkis.common.utils.{Logging, Utils}
import com.webank.wedatasphere.linkis.server.Message
import com.webank.wedatasphere.linkis.server.conf.ServerConfiguration._
import com.webank.wedatasphere.linkis.server.exception.BDPServerErrorException
import com.webank.wedatasphere.linkis.server.socket.controller.{ServerListenerEventBus, SocketServerEvent}
import org.apache.commons.lang.StringUtils
import org.apache.commons.lang.exception.ExceptionUtils
import org.apache.commons.lang.time.DateFormatUtils
import org.eclipse.jetty.websocket.servlet._

import scala.collection.JavaConversions._

/**
  * Created by enjoyyin on 2018/1/9.
  */
private[server] class ControllerServer(serverListenerEventBus: ServerListenerEventBus)
  extends WebSocketServlet with SocketListener
   with Event with Logging {

  private val socketList = new util.HashMap[Int, ServerSocket](BDP_SERVER_SOCKET_QUEUE_SIZE.getValue)
  private val idGenerator = new AtomicInteger(0)

  override def configure(webSocketServletFactory: WebSocketServletFactory): Unit = {
    webSocketServletFactory.setCreator(new WebSocketCreator {
      override def createWebSocket(servletUpgradeRequest: ServletUpgradeRequest,
                                   servletUpgradeResponse: ServletUpgradeResponse): AnyRef =
        ServerSocket(servletUpgradeRequest.getHttpServletRequest, ControllerServer.this)//外部类.this 访问外部类的域
    })
  }

  def sendMessage(id: Int, message: Message): Unit = {
    val socket = socketList.get(id)
    if(socket == null) throw new BDPServerErrorException(11004, s"ServerSocket($id) does not exist!(ServerSocket($id)不存在！)")
    socket.sendMessage(message)
  }

  def sendMessageToAll(message: Message): Unit =
    socketList.values().foreach(_.sendMessage(message))

  def sendMessageToUser(user: String, message: Message): Unit =
    socketList.values().filter(s => s != null && s.user.contains(user)).foreach(_.sendMessage(message))

  override def onClose(socket: ServerSocket, code: Int, message: String): Unit = {
    val date = DateFormatUtils.format(socket.createTime, DEFAULT_DATE_PATTERN.getValue)
    if(!socketList.containsKey(socket.id))
      warn(s"$socket created at $date has expired, ignore the close function!")
    else {
      info(s"$socket closed at $date with code $code and message: " + message)
      socketList synchronized {
        if(socketList.containsKey(socket.id)) socketList.remove(socket.id)
      }
    }
  }

  override def onOpen(socket: ServerSocket): Unit = socketList synchronized {
    val index = idGenerator.getAndIncrement()
    socket.id = index
    socketList.put(index, socket)
    info(s"open a new $socket with id $index for user ${socket.user.orNull}!")
  }

  override def onMessage(socket: ServerSocket, message: String): Unit = {
    if(StringUtils.isBlank(message)) {
      socket.sendMessage(Message.error("Empty message!"))
      return
    }
    //将前台请求的message封装为ServerEvent，再封装为SocketServerEvent
    val socketServerEvent = Utils.tryCatch(new SocketServerEvent(socket, message)){ t =>
      warn("parse message failed!", t)
      socket.sendMessage(Message.error(ExceptionUtils.getRootCauseMessage(t), t))
      return
    }
    if(socket.user.isEmpty && socketServerEvent.serverEvent.getMethod != BDP_SERVER_SOCKET_LOGIN_URI.getValue) {
      socket.sendMessage(Message.noLogin("You are not logged in, please login first!(您尚未登录，请先登录!)").data("websocketTag", socketServerEvent.serverEvent.getWebsocketTag) << socketServerEvent.serverEvent.getMethod)
    } else Utils.tryCatch(serverListenerEventBus.post(socketServerEvent)){
      //推送信息到listenerbus，最终还是调用socket.sendMessage 方法推送到前台？意义何在，还不如直接在这个方法中
      //进行，因为这个方法也能拿到socket
      //但是这里并不能拿到返回得taskID，所以要提交到listenerBus中，由注册得listener进行提交job，并返回数据
      case t: BDPServerErrorException => Message.error(t.getMessage, t).data("websocketTag", socketServerEvent.serverEvent.getWebsocketTag) << socketServerEvent.serverEvent.getMethod
    }
  }
}
