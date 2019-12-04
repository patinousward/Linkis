package com.webank.wedatasphere.linkis.filesystem.reader

trait TextFileReader {

  def getPager():Pager

  def canRead(path:String):Boolean = f(path,fileType)

  protected var fileType:Array[String] = Array.empty

  private val f =(x:String,y:Array[String]) =>y.exists(ft=>x.endsWith("."+ft))
}
