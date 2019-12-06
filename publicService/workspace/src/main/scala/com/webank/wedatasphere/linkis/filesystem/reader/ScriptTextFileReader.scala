package com.webank.wedatasphere.linkis.filesystem.reader

import com.webank.wedatasphere.linkis.filesystem.exception.WorkSpaceException
import com.webank.wedatasphere.linkis.storage.script.{ScriptFsReader, ScriptMetaData, ScriptRecord, VariableParser}
import org.apache.commons.io.IOUtils

/**
  * sql,hql,mlsql,py,scala,python,r,out
  */

class ScriptTextFileReader extends TextFileReader {

  setPagerTrigger(PagerTrigger.OFF)

  override def getHeader(): Object = {
    if (reader == null) {
      reader = ScriptFsReader.getScriptFsReader(getFsPath(), params.getOrDefault("charset", "utf-8"), getIs())
    }
    val metadata = reader.getMetaData.asInstanceOf[ScriptMetaData]
    VariableParser.getMap(metadata.getMetaData)
  }

  override def getBody(): Object = {
    val recordList = new StringBuilder
    while (reader.hasNext && ifContinueRead) {
      val line = reader.getRecord.asInstanceOf[ScriptRecord].getLine
      if (ifStartRead) {
        recordList.append(getLineShuffle().shuffle(line)).append("\n")
        totalLine += 1
      }
      count += 1
    }
    recordList.toString()
  }

  private var reader: ScriptFsReader = _

  override def getReturnType(): String = "script/text"

  override def close(): Unit = {
    IOUtils.closeQuietly(reader)
  }

  override def setPagerModel(pagerModel: PagerModel.Value): TextFileReader = {
    throw new WorkSpaceException("scriptTextFileReader can not setting pageModel")
  }

  override def getHeaderKey(): String = "params"
}

object ScriptTextFileReader extends TextFileReaderSelector {

  fileType = Array("sql", "hql", "mlsql", "py", "python", "scala", "r", "out")

  override def select(): TextFileReader = new ScriptTextFileReader
}
