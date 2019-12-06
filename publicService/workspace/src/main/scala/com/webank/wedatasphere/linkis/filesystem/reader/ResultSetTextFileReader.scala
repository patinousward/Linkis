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
package com.webank.wedatasphere.linkis.filesystem.reader

import java.util

import com.webank.wedatasphere.linkis.common.io.resultset.{ResultSet, ResultSetReader}
import com.webank.wedatasphere.linkis.common.io.{MetaData, Record}
import com.webank.wedatasphere.linkis.filesystem.exception.WorkSpaceException
import com.webank.wedatasphere.linkis.storage.resultset.table.{TableMetaData, TableRecord}
import com.webank.wedatasphere.linkis.storage.resultset.{ResultSetFactory, ResultSetReader}
import com.webank.wedatasphere.linkis.storage.{LineMetaData, LineRecord}
import org.apache.commons.io.IOUtils

import scala.collection.JavaConversions._

/**
  * .dolphin
  */
/**
  * Created by patinousward
  */
class ResultSetTextFileReader extends TextFileReader {

  override def getHeader(): Object = {
    if (reader == null) {
      resultSet = ResultSetFactory.getInstance.getResultSetByPath(getFsPath())
      reader = ResultSetReader.getResultSetReader(resultSet, getIs())
    }
    reader.getMetaData match {
      case metadata: LineMetaData => getLineShuffle().shuffleHead(metadata.getMetaData)
      case metadata: TableMetaData => getLineShuffle().shuffleHead(metadata.columns.map(_.toString))
    }
  }

  def getTableResultSetBody(): Object = {
    val recordList = new util.ArrayList[Array[String]]()
    while (reader.hasNext && ifContinueRead) {
      val line = reader.getRecord.asInstanceOf[TableRecord].row
      if (ifStartRead) {
        recordList.add(getLineShuffle().shuffleBody(line))
        totalLine += 1
      }
      count += 1
    }
    recordList
  }

  def getLineResultSetBody(): Object = {
    val recordList = new util.ArrayList[String]()
    while (reader.hasNext && ifContinueRead) {
      val line = reader.getRecord.asInstanceOf[LineRecord].getLine
      if (ifStartRead) {
        recordList.add(getLineShuffle().shuffleBody(line))
        totalLine += 1
      }
      count += 1
    }
    recordList.foldLeft("")((a, b) => a + b + "\n")
  }

  override def getBody(): Object = {
    getReturnType() match {
      case ResultSetFactory.TABLE_TYPE => getTableResultSetBody()
      case _ => getLineResultSetBody()
    }
  }

  private var reader: ResultSetReader[_ <: MetaData, _ <: Record] = _
  private var resultSet: ResultSet[_ <: MetaData, _ <: Record] = _

  override def getReturnType(): String = resultSet.resultSetType()

  override def close(): Unit = IOUtils.closeQuietly(reader)

  override def setPagerModel(pagerModel: PagerModel.Value): TextFileReader = {
    throw new WorkSpaceException("scriptTextFileReader can not setting pageModel")
  }

  override def getHeaderKey(): String = "metadata"
}

object ResultSetTextFileReader extends TextFileReaderSelector {

  fileType = Array("dolphin")

  override def select(): TextFileReader = new ResultSetTextFileReader
}
