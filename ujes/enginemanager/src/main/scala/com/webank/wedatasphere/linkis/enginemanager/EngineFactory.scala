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

package com.webank.wedatasphere.linkis.enginemanager

import com.webank.wedatasphere.linkis.resourcemanager.Resource

/**
  * Created by johnnwang on 2018/9/6.
  */
//小结:
//EngineFactory,EngineCreator,ProcessEngineBuilder  三者只有EngineCreator 是真正创建引擎的,其他2个名字起得都有问题
//EngineFactory也不是用来创建engien的,只是用来管理启动引擎的,包括和engine的心跳,保存启动引擎对象的缓存集合等
abstract class EngineFactory {

  /**
    * Start the following thread:
  * 1. Time release the current node idle engine
    * 启动以下线程：
    * 1. 定时释放当前节点空闲引擎
    */
  def init(): Unit

  def get(port: Int): Option[Engine]

  def getUsedResources: Option[Resource]

  def list(): Array[Engine]

  def deleteAll(creator: String): Unit

  def delete(engine: Engine): Unit

  def shutdown(deleteEngines: Boolean): Unit

  def addEngine(engine: Engine): Unit

}