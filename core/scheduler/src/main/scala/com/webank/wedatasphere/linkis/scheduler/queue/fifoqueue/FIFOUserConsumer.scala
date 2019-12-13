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

package com.webank.wedatasphere.linkis.scheduler.queue.fifoqueue

/**
  * Created by enjoyyin on 2018/9/7.
  */

import java.util.concurrent.{ExecutorService, Future}

import com.webank.wedatasphere.linkis.common.exception.{ErrorException, WarnException}
import com.webank.wedatasphere.linkis.common.utils.Utils
import com.webank.wedatasphere.linkis.scheduler.SchedulerContext
import com.webank.wedatasphere.linkis.scheduler.exception.SchedulerErrorException
import com.webank.wedatasphere.linkis.scheduler.executer.Executor
import com.webank.wedatasphere.linkis.scheduler.future.{BDPFuture, BDPFutureTask}
import com.webank.wedatasphere.linkis.scheduler.queue._

import scala.collection.mutable.ArrayBuffer
import scala.concurrent.TimeoutException

class FIFOUserConsumer(schedulerContext: SchedulerContext,
                       executeService: ExecutorService, private var group: Group) extends Consumer(schedulerContext, executeService) {
  private var fifoGroup = group.asInstanceOf[FIFOGroup]
  private var queue: ConsumeQueue = _
  private val maxRunningJobsNum = fifoGroup.getMaxRunningJobs
  private val runningJobs = new Array[Job](maxRunningJobsNum)
  private var future: Future[_] = _

  def this(schedulerContext: SchedulerContext,executeService: ExecutorService) = {
    this(schedulerContext,executeService, null)
  }
  //将自身提交到executeService去执行，并且将返回值[Future] 赋值给future  这个对象会放在queue中的每个job的consumerFuture 的BDPFutureTask中
  //就是说fifoconsumer中，每个job的consumerFuture对象是不同的，但是这个对象拥有的futrue都是相同的
  def start(): Unit = future = executeService.submit(this)

  override def setConsumeQueue(consumeQueue: ConsumeQueue) = {
    queue = consumeQueue
  }

  override def getConsumeQueue = queue

  override def getGroup = fifoGroup

  override def setGroup(group: Group) = {
    this.fifoGroup = group.asInstanceOf[FIFOGroup]
  }

  override def getRunningEvents = getEvents(_.isRunning)

  private def getEvents(op: SchedulerEvent => Boolean): Array[SchedulerEvent] = {
    val result = ArrayBuffer[SchedulerEvent]()
    runningJobs.filter(_ != null).filter(x => op(x)).foreach(result += _)
    result.toArray
  }

  override def run() = {
    Thread.currentThread().setName(s"${toString}Thread")
    info(s"$toString thread started!")
    while (!terminate) {
      Utils.tryAndError(loop())
      Utils.tryAndError(Thread.sleep(10))
    }
    info(s"$toString thread stopped!")
  }

  protected def askExecutorGap(): Unit = {}

  protected def loop(): Unit = {
    //==null的判断应该是为了防止对象初始化时刚创建对象但是暂时没分配内存的请求吧
    val completedNums = runningJobs.filter(e => e == null || e.isCompleted)
    if (completedNums.length < 1) {
      //如果runningjob为0，说明没有在跑的job，就sleep 1s，减少cpu的压力
      Utils.tryQuietly(Thread.sleep(1000))  //TODO can also be optimized to optimize by implementing JobListener(TODO 还可以优化，通过实现JobListener进行优化)
      return
    }
    var isRetryJob = false
    var event: Option[SchedulerEvent] = None
    def getWaitForRetryEvent: Option[SchedulerEvent] = {
      val waitForRetryJobs = runningJobs.filter(job => job != null && job.isJobCanRetry)
      waitForRetryJobs.find{job =>
        isRetryJob = Utils.tryCatch(job.turnToRetry()){ t =>
          job.onFailure("Job state flipped to Scheduled failed in Retry(Retry时，job状态翻转为Scheduled失败)！", t)
          false
        }
        isRetryJob
      }
    }
    while(event.isEmpty) {
      //从queue中取出一个event（takeEvent）没有取到，或者转到scheduler抛异常的话，都是false，event为None
      val takeEvent = if(getRunningEvents.isEmpty) Option(queue.take()) else queue.take(3000)
      event = if(takeEvent.exists(e => Utils.tryCatch(e.turnToScheduled()) {t =>
          takeEvent.get.asInstanceOf[Job].onFailure("Job state flipped to Scheduled failed(Job状态翻转为Scheduled失败)！", t)
          false
      })) takeEvent else getWaitForRetryEvent //没取到从runningJobs（是个内存缓存，不在queue中）取出需要retry的job，有返回true，没有返回false
    }
    event.foreach { case job: Job =>
      Utils.tryCatch {
        //获取group的getMaxAskExecutorDuration 和getAskExecutorInterval
        val (totalDuration, askDuration) = (fifoGroup.getMaxAskExecutorDuration, fifoGroup.getAskExecutorInterval)
        var executor: Option[Executor] = None
        //futrue封装进入BDPFutureTask，并且赋值给每个job的consumerFuture
        //-----------------每个job的consumerFuture对象是不同的，但是这个对象拥有的futrue都是相同的----------------------
        job.consumerFuture = new BDPFutureTask(this.future)
        //waitUntil 一直调用第一个函数，如果这个函数返回false，则会循环调用，直至超过totalDuration【会抛出超时异常】
        Utils.waitUntil(() => {
          //getOrCreateExecutorManager 是由entrance那边实现的
          //askExecutor  返回一个Executor（自己定义的） 对象
          //askDuration 是一次askExecute的持续时间，超过后，executor.isDefined返回false，因为waitUntil机制，会循环调用
          //直到totalDuration到时间
          executor = Utils.tryCatch(schedulerContext.getOrCreateExecutorManager.askExecutor(job, askDuration)) {
            case warn: WarnException =>
              job.getLogListener.foreach(_.onLogUpdate(job, warn.getDesc))
              None
            case e:ErrorException =>
              job.getLogListener.foreach(_.onLogUpdate(job, e.getDesc))
              throw e
            case error: Throwable =>
              throw error
          }
          Utils.tryQuietly(askExecutorGap())
          //返回值，看
          executor.isDefined
          //totalDuration 是总的持续时间
        }, totalDuration)
        //清空job的consumerFuture
        job.consumerFuture = null  //这里是不能调用consumerFuture的cancel方法的，下一个job还要用consumerFuture中的futrue对象
        executor.foreach { executor =>
          //如果成功找到了executor（应该就是entrance找到了engine并且lock住）
          job.setExecutor(executor)
          //executeService（线程池）中提交这个job，也提交一个jobDaemon
          // 并且把futrue放入job中
          //【所以executor（consuemr中的线程池）一共会提交3类，1.consumer的running（就一个，一直处于循环中）2.queue中提交的job  3.job的守护线程】
          //【总结：
          // 1.job中BDPFutrueTask 拥有1 和 2
          // 2.consuemr中拥有1
          // 3.守护线程中自己拥有3
          // 】
          job.future = executeService.submit(job)
          job.getJobDaemon.foreach(jobDaemon => jobDaemon.future = executeService.submit(jobDaemon))
          if(!isRetryJob) putToRunningJobs(job)
        }
      }{
            //抛出超时异常后，将会写入job日志
        case _: TimeoutException =>
          warn(s"Ask executor for Job $job timeout!")
          job.onFailure("The request engine times out and the cluster cannot provide enough resources(请求引擎超时，集群不能提供足够的资源).",
            new SchedulerErrorException(11055, "Insufficient resources, requesting available engine timeout(资源不足，请求可用引擎超时)！"))
        case error: Throwable =>
          job.onFailure("Request engine failed, possibly due to insufficient resources or background process error(请求引擎失败，可能是由于资源不足或后台进程错误)!", error)
          if(job.isWaitForRetry) {
            warn(s"Ask executor for Job $job failed, wait for the next retry!", error)
            if(!isRetryJob)  putToRunningJobs(job)
          } else warn(s"Ask executor for Job $job failed!", error)
      }
    }
  }
  //推到缓存runningJobs中
  private def putToRunningJobs(job: Job): Unit = {
    val index = runningJobs.indexWhere(f => f == null || f.isCompleted)
    runningJobs(index) = job
  }

  override def shutdown() = {
    future.cancel(true)
    super.shutdown()
  }
}
