package com.webank.wedatasphere.linkis.filesystem.reader

/**
  * .text,.csv
  */

class OtherTextFileReader extends TextFileReader {
  override def getHeader(): AnyRef = ???

  override def getBody(): AnyRef = ???

  override def getReturnType: String = "script/text"

  override def getHeaderKey: String = ???

  override def close(): Unit = ???
}

object OtherTextFileReader extends TextFileReaderSelector {

  fileType = Array("txt", "csv")

  override def select(): TextFileReader = new OtherTextFileReader
}
