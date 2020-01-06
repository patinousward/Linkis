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

package com.webank.wedatasphere.linkis.entrance.execute.impl

import java.util
import java.util.concurrent.TimeUnit

import com.webank.wedatasphere.linkis.common.exception.WarnException
import com.webank.wedatasphere.linkis.common.utils.{Logging, Utils}
import com.webank.wedatasphere.linkis.entrance.conf.EntranceConfiguration.{ENGINE_LOCK_MAX_HOLDER_TIME, ENGINE_LOCK_SCAN_TIME}
import com.webank.wedatasphere.linkis.entrance.event.{EntranceEvent, EntranceEventListener, EntranceEventListenerBus, MissingEngineNotifyEvent}
import com.webank.wedatasphere.linkis.entrance.execute.{EngineSelector, EntranceEngine, SingleEntranceEngine}
import com.webank.wedatasphere.linkis.protocol.engine._
import com.webank.wedatasphere.linkis.rpc.utils.RPCUtils
import com.webank.wedatasphere.linkis.scheduler.exception.WaitForNextAskExecutorException
import com.webank.wedatasphere.linkis.scheduler.executer.ExecutorState

import scala.collection.JavaConversions.asScalaSet

/**
  * Created by enjoyyin on 2018/9/21.
  */
class SingleEngineSelector extends EngineSelector with Logging {

  private val engineToLockAndCreateTimes = new util.HashMap[SingleEntranceEngine, (String, Long)]
  private var entranceEventListenerBus: Option[EntranceEventListenerBus[EntranceEventListener, EntranceEvent]] = _

  def setEntranceEventListenerBus(entranceEventListenerBus: EntranceEventListenerBus[EntranceEventListener, EntranceEvent]): Unit =
    this.entranceEventListenerBus = Option(entranceEventListenerBus)
  def getEntranceEventListenerBus = entranceEventListenerBus

  Utils.defaultScheduler.scheduleAtFixedRate(new Runnable {
    override def run(): Unit = engineToLockAndCreateTimes.entrySet().foreach { entry =>
      val startTime = entry.getValue._2
      val engine = entry.getKey
      val lock = entry.getValue._1
      if(System.currentTimeMillis - startTime >= ENGINE_LOCK_MAX_HOLDER_TIME.getValue.toLong) {
        warn(s"try to unlock the lock of the engine $engine, since it is expired for a long time.")
        //超时的话，releaseLock，将lock变为None，再rpc自动请求释放锁
        //说明所谓的lock只是个string对象
        //lock的时候，rpc请求，然后将stirng赋值给lock对象
        //释放有2种，一种是这里超时释放，因为engine种lock也没释放，所以要发送rpc请求过去，再将lock对象清空
        //另外一种就算返回结果后直接在entrance将lock变为None，Engine那边应该是自己处理的
        Utils.tryCatch(engine.releaseLock{ sender =>
          //send异步方法,不关心到底有没unlock成功的,也不使用返回值
          sender.send(RequestEngineUnlock(engine.getModuleInstance.getInstance, lock))
          //engineToLockAndCreateTimes中移除对象
          onEngineLockUsed(engine)
          true
        }){ t =>
          //TODO add event to alert
          error(s"unlock the lock for engine $engine failed, reason: " + t.getMessage)
        }
      }
      info(s"engine lock scan start:$startTime end:${System.currentTimeMillis}")
    }
  }, 2, ENGINE_LOCK_SCAN_TIME.getValue.toLong, TimeUnit.MILLISECONDS)

  private def getOverload(overloadInfo: EngineOverloadInfo): Float = overloadInfo.usedMemory * 1f / overloadInfo.maxMemory

  /**
    *
    * @param engines
    * @return
    */
  override def chooseEngine(engines: Array[EntranceEngine]): Option[EntranceEngine] = {
    //  EntranceEngine状态为idle并且 engineToLockAndCreateTimes 中 不包含这个engine，就算说没有上锁
    val usefulEngines = engines.filter(e => e.state == ExecutorState.Idle && !engineToLockAndCreateTimes.containsKey(e))
    //按负载进行排序
    val sortedEngines = usefulEngines.filter(_.getOverloadInfo.isDefined).sortBy(_.getOverloadInfo.map(getOverload).get)
    //unknownEngines没有负载的引擎
    val unknownEngines = usefulEngines.filter(_.getOverloadInfo.isEmpty)
    if(unknownEngines.isEmpty && sortedEngines.isEmpty) None //sortedEngines 和unknownEngines 都为empty的话，直接返回None
    else if(unknownEngines.isEmpty) Option(sortedEngines(0))//unknownEngines为空的话，选择sortedEngines的第一个
      //sortedEngines为空，或sortedEngines的第一个负载大于70%，unknownEngines中随机选择一个
    else if(sortedEngines.isEmpty || sortedEngines(0).getOverloadInfo.map(getOverload).exists(_ > 0.7)) {
      Option(unknownEngines((math.random * unknownEngines.length).toInt))
    } else Option(sortedEngines(0))
  }

  /**
    *
    * @param engine
    * @return
    */
  override def lockEngine(engine: EntranceEngine): Option[String] = engine match {
      //lock engine 失败后,将其加入unhealthy的engine缓存
    case s: SingleEntranceEngine =>
      var lock: Option[String] = None
      info(s"try to ask a lock for $engine.")
      s.tryLock(sender => Utils.tryThrow {
        //rpc请求锁引擎
        sender.ask(RequestEngineLock(engine.getModuleInstance.getInstance, ENGINE_LOCK_MAX_HOLDER_TIME.getValue.toLong)) match {
            //这个l是engine返回的一个String类型的lock
          case ResponseEngineLock(l) =>
            info(s"locked a lock $l for $engine.")
            lock = Some(l)
            lock
            //如果时返回ResponseEngineStatus,说明lockEngien失败了,engine还在busy状态,这里翻转下EnranceEngine的状态,并且返回None
          case ResponseEngineStatus(instance, state, overload, concurrent, _) =>
            info(s"request lock failed, transition engine $instance states old state:$state.")
            engine.updateState(ExecutorState.Idle, ExecutorState.apply(state), overload, concurrent)
            None
          case warn: WarnException =>
            info(warn.getMessage)
            None
        }
      } { t =>
        //没锁住，而且异常是特定的，或者异常信息包含一些特定的字符，就会加入unhealthy的列表
          if(RPCUtils.isReceiverNotExists(t)) {
            warn(s"lock $engine failed, I lost its connection, now post it to entranceEventListenerBus and notify others.", t)
            entranceEventListenerBus.foreach(_.post(MissingEngineNotifyEvent(null, t, engine)))
            new WaitForNextAskExecutorException(s"submit to $engine failed! Reason: lost its connection. Now try to submit to another engine.")
          } else t
      })
      lock
    case _ => None
  }

  override def onEngineLocked(engine: EntranceEngine, lock: String): Unit = engine match {
    case s: SingleEntranceEngine => engineToLockAndCreateTimes.put(s, (lock, System.currentTimeMillis))
  }


  override def onEngineLockUsed(engine: EntranceEngine): Unit = if(engineToLockAndCreateTimes.containsKey(engine)) {
    engineToLockAndCreateTimes.remove(engine)
  }
}
