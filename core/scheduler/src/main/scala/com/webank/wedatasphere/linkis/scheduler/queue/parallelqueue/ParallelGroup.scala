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

import com.webank.wedatasphere.linkis.scheduler.queue.SchedulerEvent
import com.webank.wedatasphere.linkis.scheduler.queue.fifoqueue.FIFOGroup

/**
  * Created by enjoyyin on 2018/9/12.
  */
//这里继承FIFOGroup有点不合适，相同的代码应该抽取成abstract类，而不是为了省代码而继承
class ParallelGroup(groupName: String, initCapacity: Int, maxCapacity: Int) extends FIFOGroup(groupName, initCapacity, maxCapacity) {
  override def belongTo(event: SchedulerEvent): Boolean = {
     val eventId = event.id.split("_")
     if(eventId.nonEmpty){
       val name = eventId(0) //这里看出，ParallelGroup的job的id的格式应该是groupName_xxx
       if (name.equals(groupName)) true else false
     }else{
       false
     }
  }
}
