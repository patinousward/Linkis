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

package com.webank.wedatasphere.linkis.scheduler.executer

import com.webank.wedatasphere.linkis.scheduler.listener.ExecutorListener
import com.webank.wedatasphere.linkis.scheduler.queue.{Job, SchedulerEvent}

import scala.concurrent.duration.Duration

/**
  * Created by enjoyyin on 2018/9/1.
  */
abstract class ExecutorManager {

  def setExecutorListener(executorListener: ExecutorListener): Unit

  protected def createExecutor(event: SchedulerEvent): Executor

  def askExecutor(event: SchedulerEvent): Option[Executor]

  /**
    * scheduler中的主要方法   askExecutor获取到Executor 后放入JOb中,当job 的run方法执行的时候
    */
  def askExecutor(event: SchedulerEvent, wait: Duration): Option[Executor]

  def getById(id: Long): Option[Executor]

  def getByGroup(groupName: String): Array[Executor]

  protected def delete(executor: Executor): Unit

  def shutdown(): Unit

}