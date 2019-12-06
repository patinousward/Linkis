package com.webank.wedatasphere.linkis.filesystem.reader

import java.io.{BufferedReader, InputStreamReader}
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
import org.apache.commons.io.IOUtils

/**
  * .text,.csv,.log
  */
/**
  * Created by patinousward
  */
class OtherTextFileReader extends TextFileReader {

  private var multiplication = 1024

  setPagerModel(PagerModel.Byte)

  override def getHeader(): Object = {
    getLineShuffle().shuffleHead(null)
  }

  def getLineBody(): Object = {
    val isr = new InputStreamReader(getIs(), params.getOrDefault("charset", "utf-8"))
    val br = new BufferedReader(isr)
    val recordList = new StringBuilder
    var line = br.readLine()
    while (line != null && ifContinueRead) {
      if (ifStartRead) {
        recordList.append(getLineShuffle().shuffleBody(line)).append("\n")
        totalLine += 1
      }
      line = br.readLine()
      count += 1
    }
    recordList.toString()
  }

  def getByteBody(): Object = {
    getIs().skip((start - 1) * multiplication)
    //如果是开启分页，byte读取大小就是pageSize，如果没开启分页，byte读取大小就是默认2048
    val pageSize = end - start + 1
    val bufferLength = if (getPagerTrigger() == PagerTrigger.ON) pageSize else 2048
    val buffer = new Array[Byte](bufferLength * multiplication)
    var readLength = 0
    val recordList = new StringBuilder
    while (readLength != -1 && f(count <= pageSize * multiplication)) {
      recordList.append(new String(buffer, 0, readLength))
      count += readLength
      totalLine += readLength
      readLength = getIs().read(buffer)
    }
    recordList.toString()
  }


  override def getBody(): Object = {
    if (getPagerModel() == PagerModel.Line) getLineBody() else getByteBody()
  }


  override def getReturnType(): String = "script/text"

  override def getHeaderKey(): String = "metadata"

  override def close(): Unit = IOUtils.closeQuietly(getIs())


}

object OtherTextFileReader extends TextFileReaderSelector {

  fileType = Array("txt", "csv", "log")

  override def select(): TextFileReader = new OtherTextFileReader
}
