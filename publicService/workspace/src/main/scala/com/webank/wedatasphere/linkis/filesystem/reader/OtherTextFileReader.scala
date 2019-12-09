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

import java.io.{BufferedReader, InputStreamReader}
import java.util

import com.webank.wedatasphere.linkis.filesystem.reader.listener.ReaderEvent
import org.apache.commons.io.IOUtils

import scala.collection.JavaConversions._

/**
  * Created by patinousward
  */
class OtherTextFileReader private extends TextFileReader {

  private val multiplication = 1024

  setPagerModel(PagerModel.Byte)

  override def readHead(): Unit = {
    readerListeners.foreach(_.onReadHead(ReaderEvent(null)))
  }

  def getLineBody(): Unit = {
    val isr = new InputStreamReader(getIs(), params.getOrDefault("charset", "utf-8"))
    val br = new BufferedReader(isr)
    var line = br.readLine()
    while (line != null && ifContinueRead) {
      if (ifStartRead) {
        readerListeners.foreach(_.onReadBody(ReaderEvent(line)))
        totalLine += 1
      }
      line = br.readLine()
      count += 1
    }
  }

  def getByteBody(): Unit = {
    getIs().skip((start - 1) * multiplication)
    //如果是开启分页，byte读取大小就是pageSize，如果没开启分页，byte读取大小就是默认2048
    val pageSize = end - start + 1
    val bufferLength = if (getPagerTrigger() == PagerTrigger.ON) pageSize else 2048
    val buffer = new Array[Byte](bufferLength * multiplication)
    var readLength = 0
    while (readLength != -1 && f(count <= pageSize * multiplication)) {
      readerListeners.foreach(_.onReadBody(ReaderEvent(util.Arrays.copyOfRange(buffer, 0, readLength))))
      count += readLength
      totalLine += readLength
      readLength = getIs().read(buffer)
    }
  }


  override def readBody(): Unit = {
    if (getPagerModel() == PagerModel.Line) getLineBody() else getByteBody()
  }

  override def close(): Unit = {
    IOUtils.closeQuietly(getIs())
    super.close()
  }


}

object OtherTextFileReader extends TextFileReaderSelector {

  fileType = Array("txt", "csv", "log")

  override def select(): TextFileReader = new OtherTextFileReader
}
