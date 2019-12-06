package com.webank.wedatasphere.linkis.filesystem.reader

import java.io.{Closeable, InputStream}
import java.util

import com.webank.wedatasphere.linkis.common.io.FsPath
import com.webank.wedatasphere.linkis.filesystem.exception.WorkSpaceException

trait TextFileReader extends Closeable {

  protected var start: Int = 1

  protected var end: Int = -1

  var totalPage = 1

  var totalLine = 0

  protected var count = 1

  private var pagerTrigger: PagerTrigger.Value = PagerTrigger.ON

  def setPagerTrigger(pagerTrigger: PagerTrigger.Value): Unit = this.pagerTrigger = pagerTrigger

  def getPagerTrigger(): PagerTrigger.Value = this.pagerTrigger

  private var pagerModel: PagerModel.Value = PagerModel.Line

  def setPagerModel(pagerModel: PagerModel.Value): Unit = this.pagerModel = pagerModel

  def getPagerModel(): PagerModel.Value = this.pagerModel

  protected def ifContinueRead: Boolean = f(count <= end)

  protected def ifStartRead: Boolean = f(count >= start)

  /**
    * the method does not take effect when pagetrigger is off
    *
    * @param page
    * @param pageSize
    */
  def startPage(page: Int, pageSize: Int): Unit = {
    if (pagerTrigger == PagerTrigger.OFF) return
    if (page <= 0 || pageSize <= 0)
      throw new WorkSpaceException("Illegal parameter:page and pageSize can not be empty or less than zero")
    if (pageSize > PagerConstant.maxPageSize) throw new WorkSpaceException(s"pageSize is too large,limit is ${PagerConstant.maxPageSize}")
    start = (page - 1) * pageSize + 1
    end = pageSize * page
  }

  private var fsPath: FsPath = _

  private var is: InputStream = _

  def getIs(): InputStream = {
    if (this.is == null) throw new WorkSpaceException("inputstream cannot be empty")
    this.is
  }

  def setIs(is: InputStream): TextFileReader = {
    this.is = is
    this
  }

  def getFsPath(): FsPath = {
    if (this.fsPath == null) throw new WorkSpaceException("fsPath cannot be empty")
    this.fsPath
  }

  def setFsPath(fsPath: FsPath): TextFileReader = {
    this.fsPath = fsPath
    this
  }

  var params = new util.HashMap[String, String]

  def getHeader(): Object

  def getBody(): Object

  def getReturnType(): String // TODO: 后面可以和前台统一一下

  def getHeaderKey(): String // TODO: 后面可以和前台统一一下

  protected val f = (x: Boolean) => if (pagerTrigger == PagerTrigger.OFF) true else x

  private var lineShuffle: LineShuffle = new LineShuffle {}

  def setLineShuffle(lineShuffle: LineShuffle): TextFileReader = {
    this.lineShuffle = lineShuffle
    this
  }

  def getLineShuffle(): LineShuffle = {
    if (this.lineShuffle == null) throw new WorkSpaceException("line shuffle can not be empty")
    this.lineShuffle
  }

}
