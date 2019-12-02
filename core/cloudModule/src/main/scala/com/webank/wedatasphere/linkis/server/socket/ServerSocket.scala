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

import java.util.concurrent.TimeUnit

import com.webank.wedatasphere.linkis.common.collection.BlockingLoopArray
import com.webank.wedatasphere.linkis.common.utils.Utils
import javax.servlet.http.HttpServletRequest
import com.webank.wedatasphere.linkis.server.security.SecurityFilter
import org.eclipse.jetty.websocket.api.{Session, WebSocketAdapter}

/**
  * Created by enjoyyin on 2018/1/9.
  */
//这个对象在websoket连接创建的时候都会创建一个这个对象，在ControllerServer中的configure方法中的webSocketServletFactory
//中进行创建
case class ServerSocket(request: HttpServletRequest, socketListener: SocketListener, protocol: String = "")
  extends WebSocketAdapter {
  private var session: Session = _
  private[socket] var id: Int = _   //原子操作自增
  val createTime = System.currentTimeMillis
  def user = SecurityFilter.getLoginUser(request)//从这可以看出自己封装一层的好处
  //Add a queue to do buffering, can not directly sendMessage back, will lead to the connection can not stand
  //加一个队列做缓冲，不能直接sendMessage回去，会导致连接受不住
  private val cacheMessages = new BlockingLoopArray[String](100)
  Utils.defaultScheduler.scheduleAtFixedRate(new Runnable {
    override def run(): Unit = {
      var message = cacheMessages.poll()
      while(message.isDefined) {
        message.foreach(session.getRemote.sendString)//正真发送数据给浏览器的api
        message = cacheMessages.poll()
      }
    }
  }, 1000, 1000, TimeUnit.MILLISECONDS)

  override def onWebSocketClose(statusCode: Int, reason: String): Unit = socketListener.onClose(this, statusCode, reason)

  override def onWebSocketConnect(sess: Session): Unit = {
    session = sess
    socketListener.onOpen(this)
  }
//接受websoket请求的时候触发A WebSocket Text frame was received. message是请求参数，就是定义的executionCode等等
  override def onWebSocketText(message: String): Unit = socketListener.onMessage(this, message)

  def sendMessage(message: String): Unit ={
    cacheMessages.put(message)
  }

  override def toString: String = s"ServerSocket($id, $user)"
}
