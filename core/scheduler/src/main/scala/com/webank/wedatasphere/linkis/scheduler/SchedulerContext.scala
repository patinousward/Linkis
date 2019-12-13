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

package com.webank.wedatasphere.linkis.scheduler

import com.webank.wedatasphere.linkis.common.listener.ListenerEventBus
import com.webank.wedatasphere.linkis.scheduler.event.{ScheduleEvent, SchedulerEventListener}
import com.webank.wedatasphere.linkis.scheduler.executer.ExecutorManager
import com.webank.wedatasphere.linkis.scheduler.queue.fifoqueue.FIFOSchedulerContextImpl
import com.webank.wedatasphere.linkis.scheduler.queue.{ConsumerManager, GroupFactory}

/**
  * Created by enjoyyin on 2018/9/1.
  */
trait SchedulerContext {
  //GroupFactory单例  用来创建group,无论fifo还是parall，一个groupName 对应一个group
  def getOrCreateGroupFactory: GroupFactory//一个GroupFactory对应多个Group，用groupName来区别

  //ConsumerManager单例
  def getOrCreateConsumerManager: ConsumerManager

  def getOrCreateExecutorManager: ExecutorManager

  def getOrCreateSchedulerListenerBus: ListenerEventBus[_<: SchedulerEventListener, _<: ScheduleEvent]

}

object SchedulerContext {
  //SchedulerContext的下级类就2种，ParallelSchedulerContextImpl和 FIFOSchedulerContextImpl
  //ParallelSchedulerContextImpl和FIFOSchedulerContextImpl 中
  //ExecutorManager 和ListenerEventBus  由子类各自去实现，GroupFactory和ExecutorManager只是个单例的方法，单纯的new对象
  //默认使用fifo的
  val schedulerContext:SchedulerContext = new FIFOSchedulerContextImpl( 100)
  def getSchedulerContext:SchedulerContext = schedulerContext
}