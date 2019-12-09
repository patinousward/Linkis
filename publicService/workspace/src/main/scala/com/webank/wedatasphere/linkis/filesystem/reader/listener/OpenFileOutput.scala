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
package com.webank.wedatasphere.linkis.filesystem.reader.listener

import java.util

import com.webank.wedatasphere.linkis.filesystem.reader.PagerConstant
import com.webank.wedatasphere.linkis.filesystem.util.WorkspaceUtil
import com.webank.wedatasphere.linkis.storage.domain.Column
import org.slf4j.{Logger, LoggerFactory}

/**
  * Created by patinousward
  */
class OpenFileOutput extends ReaderListener {

  private val LOGGER = LoggerFactory.getLogger(classOf[OpenFileOutput])

  private var head: Object = _

  private var bodyString: StringBuilder = _
  private var bodyTable: util.ArrayList[Array[String]] = _

  var headKey:String = "params"

  var `type`:String = "script/text"

  def getHead(): Object = this.head

  def getBody(): Object = {
    if (bodyString != null) return bodyString.toString()
    if (bodyTable != null) return bodyTable
    null
  }

  override def onReadHead(readerEvent: ReaderEvent): Unit = {
    LOGGER.info("--head--",readerEvent.content)
    readerEvent.content match {
      case null =>
      case h: Array[Column] => head = h.map(_.toString)
      case h:String => head = h
    }
    if(readerEvent.params != null){
      val resultsetType = readerEvent.params.get(PagerConstant.resultsetType)
      if(resultsetType != null){
        `type` = resultsetType
        headKey = "metadata"
      }
    }
  }

  override def onReadBody(readerEvent: ReaderEvent): Unit = {
    LOGGER.info("-body------",readerEvent.content)
    readerEvent.content match {
      case b: Array[Any] => addBody(b)
      case b: Array[Byte] => addBody(b)
      case b: String => addBody(b)
    }
  }

  private def addBody(b: Array[Any]): Unit = {
    if (bodyTable == null) bodyTable = new util.ArrayList[Array[String]]
    bodyTable.add(b.map(l=>if(l == null)"NULL" else l.toString))
  }

  private def addBody(b: Array[Byte]): Unit = {
    if (bodyString == null) bodyString = new StringBuilder
    bodyString.append(new String(b))
  }

  private def addBody(b: String): Unit = {
    if (bodyString == null) bodyString = new StringBuilder
    bodyString.append(b).append("\n")
  }

  override def close(): Unit = {

  }
}
