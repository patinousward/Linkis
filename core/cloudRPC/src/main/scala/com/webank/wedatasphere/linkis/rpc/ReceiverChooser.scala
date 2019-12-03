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

/**
  * Created by enjoyyin on 2018/11/3.
  */
trait ReceiverChooser {

  def getReceivers: util.Map[String, Receiver] = RPCSpringBeanCache.getReceivers //从bean 中取出类型是Recevier的Compoment注解标注的

  def chooseReceiver(event: RPCMessageEvent): Option[Receiver]

}

class CommonReceiverChooser extends ReceiverChooser {
  override def chooseReceiver(event: RPCMessageEvent): Option[Receiver] = if(getReceivers.size() == 1) Some(getReceivers.values.iterator.next)
  else {
    var receiver = getReceivers.get(event.serviceInstance.getApplicationName)
    if(receiver == null) receiver = getReceivers.get("receiver")
    Option(receiver)
  }
}