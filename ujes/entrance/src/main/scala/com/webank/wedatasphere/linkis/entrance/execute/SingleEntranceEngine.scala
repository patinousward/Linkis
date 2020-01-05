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

package com.webank.wedatasphere.linkis.entrance.execute

import com.webank.wedatasphere.linkis.entrance.exception.EntranceErrorException
import com.webank.wedatasphere.linkis.protocol.engine._
import com.webank.wedatasphere.linkis.rpc.Sender
import com.webank.wedatasphere.linkis.scheduler.executer.ExecutorState.{Busy, ExecutorState, Idle}
import com.webank.wedatasphere.linkis.scheduler.executer.{ExecutorState, SingleTaskOperateSupport}
import com.webank.wedatasphere.linkis.scheduler.queue.SchedulerEventState.Failed


/**
  * Created by enjoyyin on 2018/10/30.
  */
//这个是entrance创建的EntranceEngine
class SingleEntranceEngine(id: Long) extends EntranceEngine(id) with SingleTaskOperateSupport {

  private var lock: Option[String] = None
  //尝试lock   idle或者lock为None的时候
  //lock有值，或者非idle的情况下直接返回false
  def tryLock(lockOp: Sender => Option[String]): Boolean = if(!isIdle || lock.isDefined) false else synchronized {
    if(!isIdle || lock.isDefined) return false
    lockOp(sender).exists { lock =>  //这里exists好像没啥用，最后都是返回true
      this.lock = Some(lock)
      //engineLockListener  就是SingleEngineSelector 中加入缓存key是engine，value是（lock，当前时间戳）
      engineLockListener.foreach(_.onEngineLocked(this, lock))
      true
    }
  }

  private[execute] def releaseLock(unlockOp: Sender => Boolean): Unit = if(isIdle && this.lock.isDefined) synchronized {
    if(isIdle && this.lock.isDefined && unlockOp(sender)) this.lock = None
  }

  private def doMethod[T](exec: String => T): T = if(engineReturns.isEmpty)
    throw new EntranceErrorException(20001, s"Engine${id} could not find a job in RUNNING state(Engine${id}找不到处于RUNNING状态的Job)")
  else exec(engineReturns(0).execId)

  override def kill(): Boolean = doMethod(killExecId)

  override def pause(): Boolean = {
    doMethod { execId => sender.send(RequestTaskPause(execId))}
    true
  }

  override def resume(): Boolean = {
    doMethod { execId => sender.send(RequestTaskResume(execId))}
    true
  }


  override protected def changeState(fromState: ExecutorState, toState: ExecutorState): Unit =
    if(engineReturns.isEmpty) super.changeState(fromState, toState)

  override protected def callExecute(request: RequestTask): EngineExecuteAsynReturn = if(lock.contains(request.getLock)) ensureIdle({
    //将engine状态转为busy，这里并不会去rpc请求engine也变成busy，但是因为之前有lock，所以也没关系
    transition(Busy)
    //提交执行
    val response = sender.ask(request)
    engineLockListener.foreach(_.onEngineLockUsed(this))
    response match {
        //将response封装为EngineExecuteAsynReturn对象进行返回
      case ResponseTaskExecute(execId) => new EngineExecuteAsynReturn(request, getModuleInstance.getInstance, execId, _ => {
        //释放lock
        lock = None
        info("remove execId-" + execId + " with instance " + getModuleInstance.getInstance)
        //清除掉上次执行的engineReturns
        engineReturns.clear()
        //将engine状态转为idle
        whenBusy(transition(Idle))
      })
    }
  }, false) else throw new EntranceErrorException(20059, "Inconsistent locks! Engine lock is(锁不一致！engine锁为) " + lock.orNull + ", Job lock is(Job锁为) " + request.getLock)

  override def close(): Unit = {
    if(engineReturns.nonEmpty && ExecutorState.isAvailable(state)) kill()
    else if(engineReturns.nonEmpty) engineReturns.foreach{ e =>
      e.notifyError(s"$toString has already been completed with state $state.")
      e.notifyStatus(ResponseTaskStatus(e.execId, Failed.id)(null))
    }
  }
}
