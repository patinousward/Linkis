package com.webank.wedatasphere.linkis.filesystem.reader

import org.springframework.stereotype.Component

/**
  * .text,.csv
  */
@Component
class OtherTextFileReader extends TextFileReader {

  fileType = Array("txt","csv")

  override def getPager(): Pager = new OtherPager

}
