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

/**
  * Created by enjoyyin on 2019/1/14.
  */
trait ReceiverSenderBuilder {

  val order: Int

  def build(event: RPCMessageEvent): Option[Sender]

}

class CommonReceiverSenderBuilder extends ReceiverSenderBuilder {
  override val order: Int = Int.MaxValue
  //将发送方过来的event中获取serviceInstance信息，重新封装为一个Sender，这样可以用这个Sender进行回复消息，至于是异步还是同步由自己决定
  override def build(event: RPCMessageEvent): Option[Sender] = Some(Sender.getSender(event.serviceInstance))
}