/*
 * Copyright 2019 WeBank
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.webank.wedatasphere.linkis.filesystem.reader

import com.webank.wedatasphere.linkis.filesystem.exception.WorkSpaceException
import com.webank.wedatasphere.linkis.storage.script.{ScriptFsReader, ScriptMetaData, ScriptRecord, VariableParser}
import org.apache.commons.io.IOUtils

/**
  * sql,hql,mlsql,py,scala,python,r,out
  */
/**
  * Created by patinousward
  */
class ScriptTextFileReader extends TextFileReader {

  setPagerTrigger(PagerTrigger.OFF)

  override def getHeader(): Object = {
    if (reader == null) {
      reader = ScriptFsReader.getScriptFsReader(getFsPath(), params.getOrDefault("charset", "utf-8"), getIs())
    }
    val metadata = reader.getMetaData.asInstanceOf[ScriptMetaData]
    getLineShuffle().shuffleHead(VariableParser.getMap(metadata.getMetaData))
  }

  override def getBody(): Object = {
    val recordList = new StringBuilder
    while (reader.hasNext && ifContinueRead) {
      val line = reader.getRecord.asInstanceOf[ScriptRecord].getLine
      if (ifStartRead) {
        recordList.append(getLineShuffle().shuffleBody(line)).append("\n")
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
    super.close()
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
