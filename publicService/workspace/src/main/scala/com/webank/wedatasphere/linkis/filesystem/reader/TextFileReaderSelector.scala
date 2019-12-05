package com.webank.wedatasphere.linkis.filesystem.reader

trait TextFileReaderSelector {

  def canRead(path: String): Boolean = f(path, fileType)

  protected var fileType: Array[String] = Array.empty

  private val f = (x: String, y: Array[String]) => y.exists(ft => x.endsWith("." + ft))

  def select(): TextFileReader
}
