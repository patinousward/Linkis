package com.webank.wedatasphere.linkis.filesystem.reader

import java.io.Closeable

import com.webank.wedatasphere.linkis.common.io.FsPath
import com.webank.wedatasphere.linkis.storage.fs.FileSystem

trait Pager extends Closeable{

  protected var start:Int = 1

  protected var end:Int = 1

  var totalPage = 1

  var totalLine = 0

  var count = 1

  def getTotalpage() = totalPage

  def getHeader():Object

  def getBody():Object

  def getType:String

  def getHeaderKey:String // TODO: 后面可以和前台统一一下

  def startPage(fsPath:FsPath, fs:FileSystem, params:java.util.Map[String,String]):Unit={
    //计算start 和end 行数
    // TODO:  参数判断
    var page = params.get("page").toInt
    if(page <=0) page = Pager.defaultPage
    var pageSize = params.get("pageSize").toInt
    if(pageSize <=0) pageSize = Pager.defaultPageSize
    start = (page -1) * pageSize + 1
    end = pageSize * page
  }
}

object Pager{
  var defaultPage = 1
  var defaultPageSize = 5000
}
