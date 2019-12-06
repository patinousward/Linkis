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

import java.io.{ByteArrayInputStream, ByteArrayOutputStream, InputStream}

import com.webank.wedatasphere.linkis.filesystem.reader.LineShuffle
import com.webank.wedatasphere.linkis.storage.domain.Column
import com.webank.wedatasphere.linkis.storage.excel.ExcelFsWriter
import com.webank.wedatasphere.linkis.storage.resultset.table.{TableMetaData, TableRecord}
import org.apache.commons.io.IOUtils
/**
  * Created by patinousward
  */
class ExcelLineShuffle(charset:String,sheetName:String,dateFormat:String) extends LineShuffle{

  override def shuffleHead(line: Object): AnyRef = {
    //重新封装一个tableMetadata
    line match {
      case tableMetaData: Array[String] =>excelFsWriter.addMetaData(new TableMetaData(tableMetaData.map(c =>{
        val columnName = c.split(",")(0).split(":")(1)
        val dataType = c.split(",")(1).split(":")(1)
        Column(columnName,dataType,null)
      })))
      case _=>
    }
    null
  }

  override def shuffleBody(line: Array[Any]): Array[String] = {
    //重新封装一个tableRecord
    val tableRecord = new TableRecord(line)
    excelFsWriter.addRecord(tableRecord)
    null
  }

  private val excelFsWriter = ExcelFsWriter.getExcelFsWriter(charset,sheetName,dateFormat)
  private var baos: ByteArrayOutputStream= _

  def getDownloadInputStream(): InputStream ={
    val workBook = excelFsWriter.getWorkBook
    baos = new ByteArrayOutputStream
    workBook.write(baos)
    val content = baos.toByteArray
    new ByteArrayInputStream(content)
  }

  override def close(): Unit = {
    IOUtils.closeQuietly(baos)
    IOUtils.closeQuietly(excelFsWriter)
  }

}
