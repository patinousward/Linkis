package com.webank.wedatasphere.linkis.filesystem.reader
import java.util

import com.webank.wedatasphere.linkis.common.io.FsPath
import com.webank.wedatasphere.linkis.filesystem.exception.WorkSpaceException
import com.webank.wedatasphere.linkis.storage.fs.FileSystem
import com.webank.wedatasphere.linkis.storage.script.{ScriptFsReader, ScriptMetaData, ScriptRecord, VariableParser}
import org.apache.commons.io.IOUtils
import scala.collection.JavaConversions._

class ScriptPager extends Pager {

  override def getHeader(): Object = {
    if(reader == null) throw new WorkSpaceException("The startPage method needs to be called first")
    val metadata = reader.getMetaData.asInstanceOf[ScriptMetaData]
    VariableParser.getMap(metadata.getMetaData)
  }

  override def getBody(): Object = {
    val recordList = new StringBuilder
    while(reader.hasNext && count<=end){
      val line = reader.getRecord.asInstanceOf[ScriptRecord].getLine
      if(count>=start)recordList.append(line).append("\n")
      count += 1
      totalLine +=1
    }
    recordList.toString()  //速度慢
  }

  private var reader:ScriptFsReader = _

  override def startPage(fsPath:FsPath, fs:FileSystem, params:java.util.Map[String,String]): Unit = {
    super.startPage(fsPath,fs,params)
    //脚本全部读取
    end = Int.MaxValue
    reader = ScriptFsReader.getScriptFsReader(fsPath,params.getOrDefault("charset","utf-8"),fs.read(fsPath))
  }

  override def getType: String = "script/text"

  override def close(): Unit = {
    IOUtils.closeQuietly(reader)
  }

  override def getHeaderKey: String = "params"

}
