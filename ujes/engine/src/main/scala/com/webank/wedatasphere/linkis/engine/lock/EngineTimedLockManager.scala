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

package com.webank.wedatasphere.linkis.engine.lock

import java.util.concurrent.{ConcurrentHashMap, ScheduledThreadPoolExecutor, TimeUnit}

import com.webank.wedatasphere.linkis.common.utils.Logging
import com.webank.wedatasphere.linkis.engine.LockManager
import com.webank.wedatasphere.linkis.engine.execute.EngineExecutorManager
import com.webank.wedatasphere.linkis.scheduler.SchedulerContext
import com.webank.wedatasphere.linkis.scheduler.executer.ExecutorState

import scala.collection.JavaConversions._
//这个是普通engine进行使用的
class EngineTimedLockManager(schedulerContext: SchedulerContext) extends LockManager(schedulerContext) with  Logging{

  var executorLock: EngineTimedLock = null

  override def isLockExist(lock: String): Boolean = {
    executorLock != null && executorLock.isAcquired()
  }

  /**
    * Try to lock an Executor in the ExecutorManager. If the lock is successful, it will return the Executor ID as the ID.
    * 尝试去锁住ExecutorManager里的一个Executor，如果锁成功的话，将返回Executor ID作为标识
    *
    * @return
    */
    //对方法加锁
  override def tryLock(timeout: Long): Option[String] = synchronized {
    val executor = schedulerContext.getOrCreateExecutorManager.asInstanceOf[EngineExecutorManager].getEngineExecutor
    debug("try to lock for executor state is "+ executor.state.toString)
    if(executor.state != ExecutorState.Idle) return None //executor状态不为idle,直接返回None
    debug("try to lock for executor id is "+ executor.getId.toString)
    if(executorLock == null) {  //如果不为null,就创建EngineTimedLock并且赋值给executorLock
      executorLock = new EngineTimedLock(timeout)
      debug("try to lock for executor get new lock "+ executorLock.toString)
    }
    if(executorLock.tryAcquire()){//所有请求执行都是同一个lock,信号量为1,获取失败返回None,比synchronized的好处是不用等待阻塞
      debug("try to lock for tryAcquire is true ")
      Some(executor.getId.toString)
    } else None
  }

  /**
    * Unlock(解锁)
    *
    * @param lock
    */
  override def unlock(lock: String): Unit = synchronized {
    debug("try to unlock for lockEntity is "+ executorLock.toString+",and lock is "+lock+",acquired is "+executorLock.isAcquired().toString)
    if(executorLock != null && executorLock.isAcquired()){
      debug("try to unlock lockEntity")
      executorLock.release()
    }
  }

}
