package com.webank.wedatasphere.linkis.filesystem.reader

import java.io.InputStream

import com.webank.wedatasphere.linkis.storage.script.{ScriptFsReader, ScriptRecord}
import org.apache.commons.io.IOUtils

/**
  * .text,.csv
  */

class OtherTextFileReader extends TextFileReader {

  setPagerModel(PagerModel.Byte)

  override def getHeader(): Object = {
    null
  }

  def getLineBody(): Object = {
    val recordList = new StringBuilder
    while (reader.hasNext && ifContinueRead) {
      val line = reader.getRecord.asInstanceOf[ScriptRecord].getLine
      if (ifStartRead) recordList.append(line).append("\n")
      count += 1
      totalLine += 1
    }
    recordList.toString()
  }

  def getByteBody(): Object = {
    is.skip(start-1)
    //如果是开启分页，byte读取大小就是pageSize，如果没开启分页，byte读取大小就是默认2048
    val bufferLength = if(getPagerTrigger() == PagerTrigger.ON) end - start + 1 else 2048
    val buffer = new  Array[Byte](bufferLength * 1024)
    var readLength = 0
    val recordList = new StringBuilder
    while(readLength!= -1){
      recordList.append(new String(buffer,0,readLength))
      readLength = is.read(buffer)
    }
    recordList.toString()
  }


  override def getBody(): Object = {
    if (reader == null) {
      is = getFs().read(getFsPath())
      reader = ScriptFsReader.getScriptFsReader(getFsPath(), params.getOrDefault("charset", "utf-8"), is)
    }
    if (getPagerModel() == PagerModel.Line) getLineBody() else getByteBody()
  }


  override def getReturnType: String = "script/text"

  override def getHeaderKey: String = "metadata"

  override def close(): Unit = IOUtils.closeQuietly(reader)


  private var is: InputStream = _
  private var reader: ScriptFsReader = _

}

object OtherTextFileReader extends TextFileReaderSelector {

  fileType = Array("txt", "csv", "log")

  override def select(): TextFileReader = new OtherTextFileReader
}
