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

import com.webank.wedatasphere.linkis.common.utils.Utils
import com.webank.wedatasphere.linkis.scheduler.exception.SchedulerErrorException
import com.webank.wedatasphere.linkis.scheduler.queue.SchedulerEvent
import org.apache.commons.lang.StringUtils

/**
  * Created by enjoyyin on 2018/10/28.
  */
abstract class AbstractScheduler extends Scheduler {
  override def init(): Unit = {}

  override def start(): Unit = {}

  private def getEventId(index: Int, groupName: String): String = groupName + "_" + index
  private def getIndexAndGroupName(eventId: String): (Int, String) = {
    if(StringUtils.isBlank(eventId) || !eventId.contains("_"))
      throw new SchedulerErrorException(12011, s"Unrecognized execId $eventId.（不能识别的execId $eventId.)")
    val index = eventId.lastIndexOf("_")
    if(index < 1) throw new SchedulerErrorException(12011, s"Unrecognized execId $eventId.（不能识别的execId $eventId.)")
    (eventId.substring(index + 1).toInt, eventId.substring(0, index))
  }
  override def submit(event: SchedulerEvent): Unit = {
    //entrance的groupname是creator + user
    //ioEntrance的groupname只有file和hdfs 2个
    val groupName = getSchedulerContext.getOrCreateGroupFactory.getGroupNameByEvent(event)
    val consumer = getSchedulerContext.getOrCreateConsumerManager.getOrCreateConsumer(groupName)
    val index = consumer.getConsumeQueue.offer(event)
    //job id 就是 groupname + 提交阻塞队列的最大index
    index.map(getEventId(_, groupName)).foreach(event.setId)
    if(index.isEmpty) throw  new SchedulerErrorException(12001,"The submission job failed and the queue is full!(提交作业失败，队列已满！)")
  }

  override def get(event: SchedulerEvent): Option[SchedulerEvent] = get(event.getId)

  override def get(eventId: String): Option[SchedulerEvent] = {
    //比如exec_id是IDE_johnnwang_11   index就是11 groupName就是IDE_johnnwang
    val (index, groupName) = getIndexAndGroupName(eventId)
    //获取的是一个fifo的consumer,虽然ConsumerManager 是parallel的
    val consumer = getSchedulerContext.getOrCreateConsumerManager.getOrCreateConsumer(groupName)
    consumer.getRunningEvents.find(_.getId == eventId).orElse(consumer.getConsumeQueue.get(index))
  }

  override def shutdown(): Unit = if(getSchedulerContext != null) {
    if(getSchedulerContext.getOrCreateConsumerManager != null)
      Utils.tryQuietly(getSchedulerContext.getOrCreateConsumerManager.shutdown())
    if(getSchedulerContext.getOrCreateExecutorManager != null)
      Utils.tryQuietly(getSchedulerContext.getOrCreateExecutorManager.shutdown())
    if(getSchedulerContext.getOrCreateSchedulerListenerBus != null)
      Utils.tryQuietly(getSchedulerContext.getOrCreateSchedulerListenerBus.stop())
  }
}
