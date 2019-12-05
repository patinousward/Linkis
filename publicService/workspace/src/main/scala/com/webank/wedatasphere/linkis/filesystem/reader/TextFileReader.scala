package com.webank.wedatasphere.linkis.filesystem.reader

import java.io.Closeable
import java.util

import com.webank.wedatasphere.linkis.common.io.FsPath
import com.webank.wedatasphere.linkis.filesystem.exception.WorkSpaceException
import com.webank.wedatasphere.linkis.storage.fs.FileSystem

import scala.beans.BeanProperty

trait TextFileReader extends Closeable{

  protected var start: Int = 1

  protected var end: Int = -1

  var totalPage = 1

  var totalLine = 0

  var count = 1

  @BeanProperty var pagerTrigger: PagerTrigger.Value =PagerTrigger.ON

  protected def ifContinueRead:Boolean= f(count<=end)

  protected def ifStartRead:Boolean =f(count>=start)

  def startPage(page: Int, pageSize: Int): Unit = {
    if (pagerTrigger == PagerTrigger.OFF) return
    if (page <= 0 || pageSize <= 0)
      throw new WorkSpaceException("Illegal parameter:page and pageSize can not be empty or less than zero")
    start = (page - 1) * pageSize + 1
    end = pageSize * page
  }

  @BeanProperty var fsPath:FsPath = _

  @BeanProperty var fs:FileSystem = _

  var params = new util.HashMap[String,String]

  def getHeader(): Object

  def getBody(): Object

  def getReturnType: String // TODO: 后面可以和前台统一一下

  def getHeaderKey: String // TODO: 后面可以和前台统一一下

  private val  f = (x:Boolean) =>if(pagerTrigger == PagerTrigger.OFF) true else x

}
