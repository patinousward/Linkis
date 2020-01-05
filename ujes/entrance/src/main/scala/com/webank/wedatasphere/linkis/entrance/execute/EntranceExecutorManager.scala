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

import com.webank.wedatasphere.linkis.common.exception.WarnException
import com.webank.wedatasphere.linkis.common.utils.{Logging, Utils}
import com.webank.wedatasphere.linkis.scheduler.executer.ExecutorState.ExecutorState
import com.webank.wedatasphere.linkis.scheduler.executer.{Executor, ExecutorManager}
import com.webank.wedatasphere.linkis.scheduler.listener.ExecutorListener
import com.webank.wedatasphere.linkis.scheduler.queue.{GroupFactory, Job, LockJob, SchedulerEvent}

import scala.concurrent.duration.Duration

/**
  * Created by enjoyyin on 2018/9/10.
  */
abstract class EntranceExecutorManager(groupFactory: GroupFactory) extends ExecutorManager with Logging {
  //目前这里没有注入任何值
  @volatile private var executorListener: Option[ExecutorListener] = None

  def getOrCreateEngineBuilder(): EngineBuilder //EngineBuilder单例，用来创建EntranceEngine（是个executor） 的

  def getOrCreateEngineManager(): EngineManager //EngineManger单例，是EngineManagerImple对象，只是个实现了ExecutorListener（） 和EntranceEventListener（监听unhealthy 引擎的方法）

  def getOrCreateEngineRequester(): EngineRequester //单例EngineRequesterImpl 对象

  def getOrCreateEngineSelector(): EngineSelector //单例SingleEngineSelector,实现了EngineLockListener  用来锁引擎的

  def getOrCreateEntranceExecutorRulers(): Array[EntranceExecutorRuler]//FixedInstanceEntranceExecutorRuler，和ExceptInstanceEntranceExecutorRuler的数组的单例

  def getOrCreateInterceptors(): Array[ExecuteRequestInterceptor] //ExecuteRequestInterceptor的实现类的数组，以object类实现的单例

  private def getExecutorListeners: Array[ExecutorListener] =
    //从getOrCreateEngineManager()中进行返回一个EngineMangerImpl的单例
    executorListener.map(l => Array(getOrCreateEngineManager(), l)).getOrElse(Array(getOrCreateEngineManager()))

  override def setExecutorListener(executorListener: ExecutorListener): Unit =
    this.executorListener = Option(executorListener)

  def initialEntranceEngine(engine: EntranceEngine): Unit = {
    //entrance 中，这个初始化是None
    executorListener.map(_ => new ExecutorListener {
      override def onExecutorCreated(executor: Executor): Unit = getExecutorListeners.foreach(_.onExecutorCreated(executor))
      override def onExecutorCompleted(executor: Executor, message: String): Unit = getExecutorListeners.foreach(_.onExecutorCompleted(executor, message))
      override def onExecutorStateChanged(executor: Executor, fromState: ExecutorState, toState: ExecutorState): Unit =
        getExecutorListeners.foreach(_.onExecutorStateChanged(executor, fromState, toState))
      //每个EntranceEngine 都会 有EngineMangerImpl这个单例的listener
    }).orElse(Some(getOrCreateEngineManager())).foreach(engine.setExecutorListener)
    //放入拦截器和engine选择器
    engine.setInterceptors(getOrCreateInterceptors())
    engine.setEngineLockListener(getOrCreateEngineSelector())
    //调用EngineMangerImpl中的onExecutorCreated的方法
    getExecutorListeners.foreach(_.onExecutorCreated(engine))
  }

  override protected def createExecutor(schedulerEvent: SchedulerEvent): EntranceEngine = schedulerEvent match {
    case job: Job =>
      val newEngine = getOrCreateEngineRequester().request(job)
      newEngine.foreach(initialEntranceEngine)
      //There may be a situation where the broadcast is faster than the return. Here, you need to get the EntranceEngine that is actually stored in the EngineManager.
      //可能存在广播比返回快的情况，这里需拿到实际存入EngineManager的EntranceEngine
      newEngine.flatMap(engine => getOrCreateEngineManager().get(engine.getModuleInstance.getInstance)).orNull
  }

  private def setLock(lock: Option[String], job: Job): Unit = lock.foreach(l => job match {
    case lj: LockJob => lj.setLock(l)
    case _ =>
  })

  protected def findExecutors(job: Job): Array[EntranceEngine] = {
    val groupName = groupFactory.getGroupNameByEvent(job)

    var engines = getOrCreateEngineManager().listEngines(_.getGroup.getGroupName == groupName)

    getOrCreateEntranceExecutorRulers().foreach(ruler => engines = ruler.rule(engines, job))

    engines
  }

  private def findUsefulExecutor(job: Job): Option[Executor] = {
    val engines = findExecutors(job).toBuffer
    if(engines.isEmpty) {
      //没找到engine
      return None
    }
    //找到了engine
    var engine: Option[EntranceEngine] = None
    var lock: Option[String] = None
    while(lock.isEmpty && engines.nonEmpty) {
      //使用engine选择器去进一步选择SingleEngineSelector
      engine = getOrCreateEngineSelector().chooseEngine(engines.toArray)
      var ruleEngines = engine.map(Array(_)).getOrElse(Array.empty)
      //？？这个findExecutors中不是已经过滤了吗
      getOrCreateEntranceExecutorRulers().foreach(ruler => ruleEngines = ruler.rule(ruleEngines, job))
      if(engine.isEmpty) {
        return None
      }
      //selecttor中去锁定这个engine
      ruleEngines.foreach(e => lock = getOrCreateEngineSelector().lockEngine(e))
      engine.foreach(engines -= _)  //这里-= 单纯是为了推出while循环
    }
    setLock(lock, job)
    lock.flatMap(_ => engine)
  }

  override def askExecutor(schedulerEvent: SchedulerEvent): Option[Executor] = schedulerEvent match {
    case job: Job =>
      findUsefulExecutor(job).orElse { //先找个能用的executor，没有再自己创建
        val executor = createExecutor(job)
        if(executor != null) {
          if(!job.isCompleted){
            val lock = getOrCreateEngineSelector().lockEngine(executor)
            setLock(lock, job)
            lock.map(_ => executor)
          }else Some(executor)
        } else None
      }
  }

  override def askExecutor(schedulerEvent: SchedulerEvent, wait: Duration): Option[Executor] = schedulerEvent match {
    case job: Job =>
      val startTime = System.currentTimeMillis()
      var warnException: WarnException = null
      var executor: Option[Executor] = None
      while(System.currentTimeMillis - startTime < wait.toMillis && executor.isEmpty)
        Utils.tryCatch(askExecutor(job)) {
          case warn: WarnException =>
            this.warn("request engine failed!", warn)
            warnException = warn
            None
          case t: Throwable => throw t
        } match {
          case Some(e) => executor = Option(e)
          case _ => //这里应该是抛出异常的情况
            if(System.currentTimeMillis - startTime < wait.toMillis) {
              val interval = math.min(3000, wait.toMillis - System.currentTimeMillis + startTime)//这里计算的是wait（等待时间）减上面askExecutor所花费的时间的剩余
              getOrCreateEngineManager().waitForIdle(interval) //当前线程等待一段时间，时间是interval
            }
        }
      if(warnException != null && executor.isEmpty) throw warnException
      executor
  }

  override def getById(id: Long): Option[Executor] = Option(getOrCreateEngineManager().get(id))

  override def getByGroup(groupName: String): Array[Executor] =
    getOrCreateEngineManager().listEngines(_.getGroup.getGroupName == groupName).map(_.asInstanceOf)

  override protected def delete(executor: Executor): Unit = {
    getOrCreateEngineManager().delete(executor.getId)
    getExecutorListeners.foreach(_.onExecutorCompleted(executor, "deleted by ExecutorManager."))
  }

  override def shutdown(): Unit = {}
}
