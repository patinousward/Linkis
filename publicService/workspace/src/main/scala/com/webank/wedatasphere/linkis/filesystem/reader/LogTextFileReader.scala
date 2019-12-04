package com.webank.wedatasphere.linkis.filesystem.reader

import org.springframework.stereotype.Component

/**
  * .log
  */
@Component
class LogTextFileReader extends TextFileReader{

  fileType = Array("log")

  override def getPager(): Pager = new LogPager

}
