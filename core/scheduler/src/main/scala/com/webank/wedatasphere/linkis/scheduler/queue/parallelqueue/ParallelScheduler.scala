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

package com.webank.wedatasphere.linkis.scheduler.queue.parallelqueue

import com.webank.wedatasphere.linkis.scheduler.queue.{ConsumerManager, GroupFactory}
import com.webank.wedatasphere.linkis.scheduler.{AbstractScheduler, SchedulerContext}

/**
  * Created by enjoyyin on 2018/9/13.
  */
//这个类的用处
class ParallelScheduler(val schedulerContext: SchedulerContext) extends AbstractScheduler{

  private var consumerManager: ConsumerManager = _
  private var groupFactory: GroupFactory = _

  override def init() = {
    consumerManager = schedulerContext.getOrCreateConsumerManager
    groupFactory = schedulerContext.getOrCreateGroupFactory
  }

  override def getName = "ParallelScheduler"

  override def getSchedulerContext = schedulerContext
}
