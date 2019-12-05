package com.webank.wedatasphere.linkis.filesystem.reader

object PagerTrigger extends Enumeration {
  type SourceType = Value
  val ON,OFF = Value
}

object PagerConstant{
  var defaultPage = 1
  var defaultPageSize = 5000
}
