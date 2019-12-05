package com.webank.wedatasphere.linkis.filesystem.reader

import com.webank.wedatasphere.linkis.filesystem.exception.WorkSpaceException

object TextFileReaderFactory {

  private val array = Array[TextFileReaderSelector](ScriptTextFileReader, ResultSetTextFileReader, LogTextFileReader, OtherTextFileReader)

  def get(path: String): TextFileReader = {
    array.find(_.canRead(path)).getOrElse(throw new WorkSpaceException("unsupported file type!can not open it")).select()
  }

}
