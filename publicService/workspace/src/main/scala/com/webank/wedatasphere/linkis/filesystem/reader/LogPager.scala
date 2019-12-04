package com.webank.wedatasphere.linkis.filesystem.reader
import java.util

import com.webank.wedatasphere.linkis.common.io.FsPath
import com.webank.wedatasphere.linkis.storage.fs.FileSystem

class LogPager extends Pager {

  override def getHeader(): Object = ???

  override def getBody(): Object = ???

  override def startPage(fsPath:FsPath, fs:FileSystem, params:java.util.Map[String,String]): Unit = {
    super.startPage(fsPath,fs,params)
  }

  override def getType: String = null

  override def getHeaderKey: String = ???

  override def close(): Unit = ???
}
