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

import java.util.concurrent.TimeUnit

import com.webank.wedatasphere.linkis.common.utils.Logging
import com.webank.wedatasphere.linkis.protocol.BroadcastProtocol
import com.webank.wedatasphere.linkis.rpc.conf.RPCConfiguration.{BDP_RPC_RECEIVER_ASYN_CONSUMER_THREAD_FREE_TIME_MAX, BDP_RPC_RECEIVER_ASYN_CONSUMER_THREAD_MAX, BDP_RPC_RECEIVER_ASYN_QUEUE_CAPACITY}
import com.webank.wedatasphere.linkis.rpc.exception.DWCURIException
import com.webank.wedatasphere.linkis.rpc.transform.{RPCConsumer, RPCProduct}
import com.webank.wedatasphere.linkis.server.{Message, catchIt}
import javax.annotation.PostConstruct
import javax.ws.rs.core.MediaType
import javax.ws.rs.{Consumes, POST, Path, Produces}
import org.apache.commons.lang.StringUtils
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.stereotype.Component

import scala.concurrent.duration.Duration
import scala.runtime.BoxedUnit

/**
  * Created by enjoyyin on 2018/8/28.
  */
@Component
@Path("/rpc")
@Produces(Array(MediaType.APPLICATION_JSON))
@Consumes(Array(MediaType.APPLICATION_JSON))
private[rpc] class RPCReceiveRestful extends RPCReceiveRemote with Logging {

  @Autowired(required = false)
  private var receiverChoosers: Array[ReceiverChooser] = Array.empty
  @Autowired(required = false)
  private var receiverSenderBuilders: Array[ReceiverSenderBuilder] = Array.empty
  @Autowired(required = false)
  private var broadcastListeners: Array[BroadcastListener] = Array.empty
  private var rpcReceiverListenerBus: AsynRPCMessageBus = _

  private def getFirst[K, T](buildArray: Array[K], buildObj: K => Option[T]): Option[T] = {
    var obj: Option[T] = None
    for(builder <- buildArray if obj.isEmpty) obj = buildObj(builder)
    obj
  }

  private implicit def getReceiver(event: RPCMessageEvent): Option[Receiver] = getFirst[ReceiverChooser, Receiver](receiverChoosers, _.chooseReceiver(event))

  private implicit def getSender(event: RPCMessageEvent): Sender = getFirst[ReceiverSenderBuilder, Sender](receiverSenderBuilders, _.build(event)).get

  def registerReceiverChooser(receiverChooser: ReceiverChooser): Unit = {
    info("register a new ReceiverChooser " + receiverChooser)
    receiverChoosers = receiverChooser +: receiverChoosers
  }
  def registerBroadcastListener(broadcastListener: BroadcastListener): Unit = {
    broadcastListeners = broadcastListener +: broadcastListeners
    addBroadcastListener(broadcastListener)
  }

  @PostConstruct
  def initListenerBus(): Unit =  {
    if(!receiverChoosers.exists(_.isInstanceOf[CommonReceiverChooser]))
      receiverChoosers = receiverChoosers :+ new CommonReceiverChooser//如果没有自己定义Chooser(默认无)，就用common的，common的选择策略是直接通过名字（服务名->Receiver的bean实现类）
    info("init all receiverChoosers in spring beans, list => " + receiverChoosers.toList)
    if(!receiverSenderBuilders.exists(_.isInstanceOf[CommonReceiverSenderBuilder]))
      //ReceiverSenderBuilder的作用就是创建一个Sender？？用来回复的时候用的吗
      receiverSenderBuilders = receiverSenderBuilders :+ new CommonReceiverSenderBuilder//如果没有自己定义的builders（默认无），就用common的
    receiverSenderBuilders = receiverSenderBuilders.sortBy(_.order)
    info("init all receiverSenderBuilders in spring beans, list => " + receiverSenderBuilders.toList)
    val queueSize = BDP_RPC_RECEIVER_ASYN_QUEUE_CAPACITY.acquireNew
    val threadSize = BDP_RPC_RECEIVER_ASYN_CONSUMER_THREAD_MAX.acquireNew
    //AsynRPCMessageBus-------
    rpcReceiverListenerBus = new AsynRPCMessageBus(queueSize,
      "RPC-Receiver-Asyn-Thread")(threadSize,
      BDP_RPC_RECEIVER_ASYN_CONSUMER_THREAD_FREE_TIME_MAX.getValue.toLong)
    info(s"init RPCReceiverListenerBus with queueSize $queueSize and consumeThreadSize $threadSize.")
    //RPCMessageEventListener-----------
    rpcReceiverListenerBus.addListener(new RPCMessageEventListener {
      override def onEvent(event: RPCMessageEvent): Unit = event.message match {
        case _: BroadcastProtocol =>
        case _ =>
          //这里能调用fold主要是用了隐式转换，event被转化为自身服务的receiver line：60
          //_.receive 说明这里是处理异步的，receive方法没有返回值，通过参数中sender返回
          //这里event放入_.receive的方法中也是使用了隐式转换 line：62
          //event中的instance信息是发送方的，这里会将这个信息封装为新的Sender作为参数传入
          event.fold(warn(s"cannot find a receiver to deal $event."))(_.receive(event.message, event))
      }
      override def onMessageEventError(event: RPCMessageEvent, t: Throwable): Unit =
        warn(s"deal RPC message failed! Message: " + event.message, t)
    })
    //broadcastListeners 其实是空的 因为registerBroadcastListener实际上并未被调用
    broadcastListeners.foreach(addBroadcastListener)
    //启动listenerbus
    rpcReceiverListenerBus.start()
  }
  //实现了BroadcastListener的类被RPCMessageEventListener封装一层，通过listener中循环调用的特性，实现了广播这种特性
  //所有的broadcastListeners 一共有3种实现，一种是上面的1对1（非BroadcastProtocol），一种是BroadcastProtocol
  //这种listener的实现都会处理protocol 继承了BroadcastProtocol的类，从而实现广播的特性
  private def addBroadcastListener(broadcastListener: BroadcastListener): Unit = if(rpcReceiverListenerBus != null) {
    info("add a new RPCBroadcastListener => " + broadcastListener.getClass)
    rpcReceiverListenerBus.addListener(new RPCMessageEventListener {
      val listenerName = broadcastListener.getClass.getSimpleName
      override def onEvent(event: RPCMessageEvent): Unit = event.message match {
          //这里用匿名类可以方便传入方法中的broadcastListener参数
        case broadcastProtocol: BroadcastProtocol => broadcastListener.onBroadcastEvent(broadcastProtocol, event)
        case _ =>
      }
      override def onMessageEventError(event: RPCMessageEvent, t: Throwable): Unit =
        warn(s"$listenerName consume broadcast message failed! Message: " + event.message, t)
    })
  }

  private implicit def toMessage(obj: Any): Message = obj match {
    case Unit | () =>
      RPCProduct.getRPCProduct.ok()
    case _: BoxedUnit => RPCProduct.getRPCProduct.ok()
    case _ =>
      RPCProduct.getRPCProduct.toMessage(obj)
  }

  @Path("receive")
  @POST
  override def receive(message: Message): Message = catchIt {
    //catchIt 也是之前restfulApi中的aop用的，主要作用是封装异常为message对象
    //封装反序列化message
    val obj = RPCConsumer.getRPCConsumer.toObject(message)
    //封装为RPCMessageEvent
    val event = RPCMessageEvent(obj, BaseRPCSender.getInstanceInfo(message.getData))
    //这里接受对方发送的异步请求，放入rpcReceiverListenerBus
    rpcReceiverListenerBus.post(event)
  }

  private def receiveAndReply(message: Message, opEvent: (Receiver, Any, Sender) => Message): Message = catchIt {
    val obj = RPCConsumer.getRPCConsumer.toObject(message)
    val serviceInstance = BaseRPCSender.getInstanceInfo(message.getData)
    //这里封装一层event的主要原因也还是利用隐式转换
    val event = RPCMessageEvent(obj, serviceInstance)
    event.map(opEvent(_, obj, event)).getOrElse(RPCProduct.getRPCProduct.notFound())
  }

  @Path("receiveAndReply")
  @POST
  //这里接受对方发送的同步请求，因为jersey这里请求是同步的，所以要等信息返回了才返回，实现了同步
  //函数_.receiveAndReply(_, _)最后会被调用 最终传递到自身服务的Receiver的方法中
  override def receiveAndReply(message: Message): Message = receiveAndReply(message, _.receiveAndReply(_, _))

  @Path("replyInMills")
  @POST
  //这里接受对方发送的超时同步请求，jersey这里请求是同步的，但是传入了Duration对象，可以自行操控何时返回
  //函数_.receiveAndReply(_, timeout, _)最后会被调用 最终传递到自身服务的Receiver的方法中
  override def receiveAndReplyInMills(message: Message): Message = catchIt {
    val duration = message.getData.get("duration")
    if(duration == null || StringUtils.isEmpty(duration.toString)) throw new DWCURIException(10002, "The timeout period is not set!(超时时间未设置！)")
    val timeout = Duration(duration.toString.toLong, TimeUnit.MILLISECONDS)
    receiveAndReply(message, _.receiveAndReply(_, timeout, _))
  }
}