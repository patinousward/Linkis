package com.webank.wedatasphere.linkis.filesystem.reader

/**
  * .log
  */

class LogTextFileReader extends TextFileReader {
  override def getHeader(): AnyRef = ???

  override def getBody(): AnyRef = ???

  override def getReturnType: String = ???

  override def getHeaderKey: String = ???

  override def close(): Unit = ???
}

object LogTextFileReader extends TextFileReaderSelector {

  fileType = Array("log")

  override def select(): TextFileReader = new LogTextFileReader
}
