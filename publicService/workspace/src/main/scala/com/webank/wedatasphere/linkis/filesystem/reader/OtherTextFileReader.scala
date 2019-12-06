package com.webank.wedatasphere.linkis.filesystem.reader

import java.io.{BufferedReader, InputStreamReader}

import org.apache.commons.io.IOUtils

/**
  * .text,.csv
  */

class OtherTextFileReader extends TextFileReader {

  private var multiplication = 1024

  setPagerModel(PagerModel.Byte)

  override def getHeader(): Object = {
    null
  }

  def getLineBody(): Object = {
    val isr = new InputStreamReader(getIs(), params.getOrDefault("charset", "utf-8"))
    val br = new BufferedReader(isr)
    val recordList = new StringBuilder
    var line = br.readLine()
    while (line != null && ifContinueRead) {
      if (ifStartRead) recordList.append(line).append("\n")
      line = br.readLine()
      count += 1
      totalLine += 1
    }
    recordList.toString()
  }

  def getByteBody(): Object = {
    getIs().skip((start - 1) * multiplication)
    //如果是开启分页，byte读取大小就是pageSize，如果没开启分页，byte读取大小就是默认2048
    val bufferLength = if (getPagerTrigger() == PagerTrigger.ON) end - start + 1 else 2048
    val buffer = new Array[Byte](bufferLength * multiplication)
    var readLength = 0
    val recordList = new StringBuilder
    while (readLength != -1 && f(count <= end * multiplication)) {
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
