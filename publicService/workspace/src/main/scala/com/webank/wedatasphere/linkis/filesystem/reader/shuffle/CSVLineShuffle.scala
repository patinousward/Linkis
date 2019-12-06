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
package com.webank.wedatasphere.linkis.filesystem.reader.shuffle

import java.io.{ByteArrayInputStream, InputStream}

import com.webank.wedatasphere.linkis.filesystem.reader.LineShuffle
import com.webank.wedatasphere.linkis.storage.csv.CSVFsWriter
import com.webank.wedatasphere.linkis.storage.domain.Column
import com.webank.wedatasphere.linkis.storage.resultset.table.{TableMetaData, TableRecord}

/**
  * Created by patinousward
  */
class CSVLineShuffle(charset: String, separator: String) extends LineShuffle{

  override def shuffleHead(line: Object): AnyRef = {
    //
    line match {
      case tableMetaData: Array[String] =>csvfsWriter.addMetaData(new TableMetaData(tableMetaData.map(c =>{
        val columnName = c.split(",")(0).split(":")(1)
        val dataType = c.split(",")(1).split(":")(1)
        Column(columnName,dataType,null)
      })))
      case _=>
    }
    null
  }

  override def shuffleBody(line: Array[Any]): Array[String] = {
    val tableRecord = new TableRecord(line)
    csvfsWriter.addRecord(tableRecord)
    null
  }

  override def shuffleBody(line: String): String = {
    lineBuff.append(line).append("\n")
    null
  }

  override def shuffleHead(line: String): String = {
    if(lineBuff == null) lineBuff = new StringBuilder
    if("null".equals(line))lineBuff.append(line).append("\n")
    null
  }


  private val csvfsWriter = CSVFsWriter.getCSVFSWriter(charset, separator);

  private var lineBuff :StringBuilder = _

  def getDownloadInputStream(): InputStream ={
    if(lineBuff == null)
      csvfsWriter.getCSVStream
    else
      new ByteArrayInputStream(lineBuff.toString.getBytes())
  }

  override def close(): Unit = {
    csvfsWriter.close()
  }

}
