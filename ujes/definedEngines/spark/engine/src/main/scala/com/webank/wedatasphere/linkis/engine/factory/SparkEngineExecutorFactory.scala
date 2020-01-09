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

package com.webank.wedatasphere.linkis.engine.factory

import java.io.File
import java.lang.reflect.Constructor

import com.webank.wedatasphere.linkis.common.utils.{Logging, Utils}
import com.webank.wedatasphere.linkis.engine.configuration.SparkConfiguration._
import com.webank.wedatasphere.linkis.engine.exception.SparkSessionNullException
import com.webank.wedatasphere.linkis.engine.execute.EngineExecutorFactory
import com.webank.wedatasphere.linkis.engine.executors.{SparkEngineExecutor, SparkPythonExecutor, SparkScalaExecutor, SparkSqlExecutor}
import com.webank.wedatasphere.linkis.engine.spark.utils.EngineUtils
import com.webank.wedatasphere.linkis.server.JMap
import org.apache.commons.lang.StringUtils
import org.apache.spark.sql.{SQLContext, SparkSession}
import org.apache.spark.{SparkConf, SparkContext}
import org.springframework.stereotype.Component
import com.google.common.base.Joiner
import com.webank.wedatasphere.linkis.common.conf.{CommonVars, TimeType}
import com.webank.wedatasphere.linkis.engine.configuration.SparkConfiguration
import org.apache.spark.util.SparkUtils

import scala.collection.mutable.ArrayBuffer
import scala.collection.JavaConversions._
/**
  * Created by allenlliu on 2019/4/8.
  */

@Component
class SparkEngineExecutorFactory extends EngineExecutorFactory with Logging{

  override def createExecutor(options: JMap[String, String]): SparkEngineExecutor = {
    info(s"Ready to create a Spark EngineExecutor ")
    val useSparkSubmit = true
    //创建sparkConf
    val conf = new SparkConf(true).setAppName(options.getOrDefault("spark.app.name", "dwc-spark-apps"))
    val master = conf.getOption("spark.master").getOrElse(CommonVars("spark.master", "yarn").getValue)
    info(s"------ Create new SparkContext {$master} -------")
    //获取sparkhome,就是pyspark的基本路径
    val pysparkBasePath = SparkConfiguration.SPARK_HOME.getValue
    //拼接地址
    val pysparkPath = new File(pysparkBasePath, "python" + File.separator + "lib")
    //找到sparkhome/python/lib/下的所有zip包
    val pythonLibUris = pysparkPath.listFiles().map(_.toURI.toString).filter(_.endsWith(".zip"))
    if (pythonLibUris.length == 2) {
      //逗号分隔的文件列表，其指向的文件将被复制到每个执行器的工作目录下。
      val confValue1 = Utils.tryQuietly(CommonVars("spark.yarn.dist.files","").getValue)
      val confValue2 = Utils.tryQuietly(conf.get("spark.yarn.dist.files"))
      if(StringUtils.isEmpty(confValue1) && StringUtils.isEmpty(confValue2))
        conf.set("spark.yarn.dist.files", pythonLibUris.mkString(","))
      else if(StringUtils.isEmpty(confValue1))
        conf.set("spark.yarn.dist.files", confValue2 + "," + pythonLibUris.mkString(","))
      else if(StringUtils.isEmpty(confValue2))
        conf.set("spark.yarn.dist.files", confValue1 + "," + pythonLibUris.mkString(","))
      else
        conf.set("spark.yarn.dist.files", confValue1 + "," + confValue2 + "," + pythonLibUris.mkString(","))
      //conf中spark.yarn.dist.files的值为confValue1(engine配置中指定) + confValue2(conf中本身有的) + pythonLibUris
      //这里相当于在提交sparksubmit的时候 --files
      if (!useSparkSubmit) conf.set("spark.files", conf.get("spark.yarn.dist.files"))
      //设置spark.submit.pyFiles
      conf.set("spark.submit.pyFiles", pythonLibUris.mkString(","))
    }
    // Distributes needed libraries to workers
    // when spark version is greater than or equal to 1.5.0
    if (master.contains("yarn")) conf.set("spark.yarn.isPython", "true")

    //val outputDir = new File(SPARK_OUTPUTDIR.getValue(options))
    val outputDir = createOutputDir(conf)
    info("outputDir====> " + outputDir)
    outputDir.deleteOnExit()
    //创建scala的executor
    val scalaExecutor =  new SparkScalaExecutor(conf)
    //设置输出目录
    scalaExecutor.outputDir = outputDir
    scalaExecutor.start()//开启
    //等待sparkILoopInited变为true
    Utils.waitUntil(() => scalaExecutor.sparkILoopInited == true && scalaExecutor.sparkILoop.intp != null, new TimeType("120s").toDuration)
    //设置线程类加载器,没有设置的话只能使用父类的
    Thread.currentThread().setContextClassLoader(scalaExecutor.sparkILoop.intp.classLoader)
    info("print current thread name "+ Thread.currentThread().getContextClassLoader.toString)
    //创建sparkSession
    val sparkSession = createSparkSession(outputDir, conf)
    if (sparkSession == null) throw new SparkSessionNullException(40009, "sparkSession can not be null")
    //获取sparkContext
    val sc = sparkSession.sparkContext
    //创建sqlContext
    val sqlContext = createSQLContext(sc,options)
    sc.hadoopConfiguration.set("mapred.output.compress", MAPRED_OUTPUT_COMPRESS.getValue(options))//默认true
    sc.hadoopConfiguration.set("mapred.output.compression.codec",MAPRED_OUTPUT_COMPRESSION_CODEC.getValue(options))//输出结果压缩格式GzipCodec
    println("Application report for " + sc.applicationId)
    scalaExecutor.sparkContext= sc
    scalaExecutor._sqlContext = sqlContext
    scalaExecutor.sparkSession = sparkSession
    //三大执行器的创建SparkSqlExecutor,ScalaExecutor,SparkPythonExecutor
    val seq = Seq(new SparkSqlExecutor(sc, sqlContext),
                   scalaExecutor,
                  new SparkPythonExecutor(sc, sqlContext,sparkSession)
                 )
    new SparkEngineExecutor(sc,5002l,500,seq)
  }

  def createSparkSession(outputDir: File, conf: SparkConf, addPythonSupport: Boolean = false): SparkSession = {
    val execUri = System.getenv("SPARK_EXECUTOR_URI")
    val sparkJars = conf.getOption("spark.jars")
    def unionFileLists(leftList: Option[String], rightList: Option[String]): Set[String] = {
      var allFiles = Set[String]()
      leftList.foreach { value => allFiles ++= value.split(",") }
      rightList.foreach { value => allFiles ++= value.split(",") }
      allFiles.filter { _.nonEmpty }
    }

    val jars = if (conf.get("spark.master").contains("yarn")) {
      val yarnJars = conf.getOption("spark.yarn.dist.jars")
      unionFileLists(sparkJars, yarnJars).toSeq
    } else {
      sparkJars.map(_.split(",")).map(_.filter(_.nonEmpty)).toSeq.flatten
    }
    if(outputDir != null) {
      conf.set("spark.repl.class.outputDir", outputDir.getAbsolutePath)
    }

    val master = conf.getOption("spark.master").getOrElse(SPARK_MASTER.getValue)
    info(s"------ Create new SparkContext {$master} -------")
    if(StringUtils.isNotEmpty(master)) {
      conf.setMaster(master)
    }
    if (jars.nonEmpty) conf.setJars(jars)
    if (execUri != null) conf.set("spark.executor.uri", execUri)
    if (System.getenv("SPARK_HOME") != null) conf.setSparkHome(System.getenv("SPARK_HOME"))
    conf.set("spark.scheduler.mode", "FAIR")

    val builder = SparkSession.builder.config(conf)
    builder.enableHiveSupport().getOrCreate()
  }

  def createSQLContext(sc: SparkContext,options: JMap[String, String]) = {
    var sqlc : SQLContext = null
    if (DWC_SPARK_USEHIVECONTEXT.getValue(options)) {
      val name = "org.apache.spark.sql.hive.HiveContext"
      var hc: Constructor[_] = null
      try {
        hc = getClass.getClassLoader.loadClass(name).getConstructor(classOf[SparkContext])
        sqlc = hc.newInstance(sc).asInstanceOf[SQLContext]
      } catch {
        case e: Throwable => {
          logger.warn("Can't create HiveContext. Fallback to SQLContext", e)
          sqlc = new SQLContext(sc)
        }
      }
    }
    else sqlc = new SQLContext(sc)
    sqlc
  }

  def createOutputDir(conf: SparkConf): File = {
    val rootDir = getOption("spark.repl.classdir")
      .getOrElse(conf.getOption("spark.repl.classdir").getOrElse(SparkUtils.getLocalDir(conf)))
    //创建临时目录  java.io.tmpdir/repl
    SparkUtils.createTempDir(root = rootDir, namePrefix = "repl")
  }

  def getOption(key: String): Option[String] = {
    val value = SPARK_REPL_CLASSDIR.getValue
    return Some(value)
  }

}
