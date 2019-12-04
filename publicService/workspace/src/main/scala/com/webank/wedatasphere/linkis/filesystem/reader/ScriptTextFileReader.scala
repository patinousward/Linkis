package com.webank.wedatasphere.linkis.filesystem.reader

import org.springframework.stereotype.Component

/**
  * sql,hql,mlsql,py,scala,python,r,out
  */
@Component
class ScriptTextFileReader extends TextFileReader {

  fileType = Array("sql","hql","mlsql","py","python","scala","r","out")

  override def getPager(): Pager = new ScriptPager

}
