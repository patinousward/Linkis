package com.webank.wedatasphere.linkis.filesystem.reader

import org.springframework.stereotype.Component

/**
  * .dolphin
  */
@Component
class ResultSetTextFileReader extends TextFileReader {

  fileType = Array("dolphin")

  override def getPager(): Pager = new ResultSetPager
}
