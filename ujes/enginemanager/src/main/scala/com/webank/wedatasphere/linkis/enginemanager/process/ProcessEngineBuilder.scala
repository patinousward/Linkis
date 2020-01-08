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

package com.webank.wedatasphere.linkis.enginemanager.process

import com.webank.wedatasphere.linkis.common.utils.Logging
import com.webank.wedatasphere.linkis.enginemanager.EngineResource
import com.webank.wedatasphere.linkis.protocol.engine.RequestEngine

/**
  * Created by johnnwang on 2018/9/6.
  */

//ProcessEngineBuilder 不是用来创建Engine的,最主要的build方法只是用来根据请求参数封装ProcessEngineBuilder对象本身
trait ProcessEngineBuilder extends Logging {
  def setPort(port: Int): Unit
  def build(engineRequest: EngineResource, request: RequestEngine): Unit
  def getEngineResource: EngineResource
  def getRequestEngine: RequestEngine
  def start(args: Array[String]): Process
}