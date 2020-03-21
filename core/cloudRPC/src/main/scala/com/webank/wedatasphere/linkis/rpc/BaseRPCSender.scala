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

package com.webank.wedatasphere.linkis.rpc

import java.util

import com.webank.wedatasphere.linkis.DataWorkCloudApplication
import com.webank.wedatasphere.linkis.common.ServiceInstance
import com.webank.wedatasphere.linkis.common.exception.WarnException
import com.webank.wedatasphere.linkis.common.utils.Logging
import com.webank.wedatasphere.linkis.protocol.Protocol
import com.webank.wedatasphere.linkis.rpc.conf.RPCConfiguration.{BDP_RPC_SENDER_ASYN_CONSUMER_THREAD_FREE_TIME_MAX, BDP_RPC_SENDER_ASYN_CONSUMER_THREAD_MAX, BDP_RPC_SENDER_ASYN_QUEUE_CAPACITY}
import com.webank.wedatasphere.linkis.rpc.interceptor._
import com.webank.wedatasphere.linkis.rpc.transform.{RPCConsumer, RPCProduct}
import com.webank.wedatasphere.linkis.server.Message
import com.webank.wedatasphere.linkis.server.conf.ServerConfiguration
import feign.{Feign, Retryer}

import scala.concurrent.duration.Duration
import scala.runtime.BoxedUnit

/**
  * Created by enjoyyin on 2018/8/29.
  */
private[rpc] class BaseRPCSender extends Sender with Logging {
  private var name: String = _
  private var rpc: RPCReceiveRemote = _

  protected def getRPCInterceptors: Array[RPCInterceptor] = Array.empty

  protected def createRPCInterceptorChain(): RPCInterceptorChain = new BaseRPCInterceptorChain(0, getRPCInterceptors, getApplicationName)
  protected def createRPCInterceptorExchange(protocol: Protocol, op: => Any): RPCInterceptorExchange =
    new BaseRPCInterceptorExchange(protocol, () => op)

  def this(applicationName: String) {
    this()
    name = applicationName
  }

  private def getRPC: RPCReceiveRemote = {
    if (rpc == null) this synchronized {
      if (rpc == null) rpc = newRPC
    }
    rpc
  }

  private[rpc] def getApplicationName = name


  protected def doBuilder(builder: Feign.Builder): Unit =
    builder.retryer(Retryer.NEVER_RETRY)

  private def newRPC: RPCReceiveRemote = {
    val builder = Feign.builder
    doBuilder(builder)//这里doBuilder用的是子类SpringMVCSender中的实现
    var url = if(name.startsWith("http://")) name else "http://" + name
    if(url.endsWith("/")) url = url.substring(0, url.length - 1)
    url += ServerConfiguration.BDP_SERVER_RESTFUL_URI.getValue
    //封装url，指定类，那么进行方法调用的时候，就会发送到那个类上的相应的请求地址上
    builder.target(classOf[RPCReceiveRemote], url)
    //builder.target，会返回RPCReceiveRemote这个类的代理类，当进行方法调用的时候。。就会找到这个方法上面的@requestMapping，找到上面的路径
    //和上面的url进行拼接。。进行http请求

    //builder中client正常情况下是使用@FeignClient注解实现的
    //target方法可能是返回一个RPCReceiveRemote的代理对象
    //直接使用服务名就可以访问的原因  https://www.jianshu.com/p/2a3965049f77
  }

  private def execute(message: Any)(op: => Any): Any = message match {
      //如果实体是Protocol，则其返回结果还需要经过interceptor的处理
    case protocol: Protocol if getRPCInterceptors.nonEmpty =>
      val rpcInterceptorChain = createRPCInterceptorChain()
      rpcInterceptorChain.handle(createRPCInterceptorExchange(protocol, op))
    case _ => op
  }

  override def ask(message: Any): Any = execute(message){
    //将message 转成json，并且标记类名，封装为Message对象
    val msg = RPCProduct.getRPCProduct.toMessage(message)
    //message 中继续放入服务名（application-name 和instance对象）
    BaseRPCSender.addInstanceInfo(msg.getData)
    //获取一个RPCReceiveRemote对象（单例）
    val response = getRPC.receiveAndReply(msg)
    //response反序列化
    RPCConsumer.getRPCConsumer.toObject(response)
  }

  override def ask(message: Any, timeout: Duration): Any = execute(message){
    val msg = RPCProduct.getRPCProduct.toMessage(message)
    //比起无超时的对象，多了一步放入duration时间
    msg.data("duration", timeout.toMillis)
    BaseRPCSender.addInstanceInfo(msg.getData)
    val response = getRPC.receiveAndReply(msg)
    RPCConsumer.getRPCConsumer.toObject(response)
  }

  private def sendIt(message: Any, op: Message => Message): Unit = execute(message){
    val msg = RPCProduct.getRPCProduct.toMessage(message)
    BaseRPCSender.addInstanceInfo(msg.getData)
    RPCConsumer.getRPCConsumer.toObject(op(msg)) match {
      case w: WarnException => warn("RPC requests an alarm!(RPC请求出现告警！)", w)
      case _: BoxedUnit =>
    }
  }
//getRPC.receive直接让feign发送请求即可
  override def send(message: Any): Unit = sendIt(message, getRPC.receive)


  /**
    * Deliver is an asynchronous method that requests the target microservice asynchronously, ensuring that the target microservice is requested once,
    * but does not guarantee that the target microservice will successfully receive the request.
    * deliver是一个异步方法，该方法异步请求目标微服务，确保一定会请求目标微服务一次，但不保证目标微服务一定能成功接收到本次请求。
    * @param message 请求的参数
    */
  override def deliver(message: Any): Unit =
    BaseRPCSender.rpcSenderListenerBus.post(RPCMessageEvent(message, ServiceInstance(name, null)))

  protected def getRPCSenderListenerBus = BaseRPCSender.rpcSenderListenerBus

  override def equals(obj: scala.Any): Boolean = if(obj == null) false
    else obj match {
      case sender: BaseRPCSender => name == sender.name
      case _ => false
    }

  override def hashCode(): Int = if(name == null) 0 else name.hashCode

  override def toString: String = s"RPCSender($name)"
}

private[rpc] object BaseRPCSender extends Logging {
  private val rpcSenderListenerBus = new AsynRPCMessageBus(BDP_RPC_SENDER_ASYN_QUEUE_CAPACITY.getValue,
    "RPC-Sender-Asyn-Thread")(BDP_RPC_SENDER_ASYN_CONSUMER_THREAD_MAX.getValue,
    BDP_RPC_SENDER_ASYN_CONSUMER_THREAD_FREE_TIME_MAX.getValue.toLong)
  rpcSenderListenerBus.addListener(new RPCMessageEventListener {
    override def onEvent(event: RPCMessageEvent): Unit = Sender.getSender(event.serviceInstance).send(event.message)

    override def onMessageEventError(event: RPCMessageEvent, t: Throwable): Unit =
      warn(s"${event.serviceInstance} deliver RPC message failed! Message: " + event.message, t)
  })
  def addInstanceInfo[T](map: util.Map[String, T]): Unit ={
    map.put("name", DataWorkCloudApplication.getApplicationName.asInstanceOf[T])
    map.put("instance", DataWorkCloudApplication.getInstance.asInstanceOf[T])
  }

  def getInstanceInfo[T](map: util.Map[String, T]): ServiceInstance = {
    val name = map.get("name").toString
    val instance = map.get("instance").toString
    ServiceInstance(name, instance)
  }
}