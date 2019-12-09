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

/**
  * Created by patinousward
  */
object TextFileReaderFactory {

  private val array = Array[TextFileReaderSelector](ScriptTextFileReader, ResultSetTextFileReader, OtherTextFileReader)

  def get(path: String): TextFileReader = {
    array.find(_.canRead(path)).getOrElse(throw new WorkSpaceException("unsupported file type!can not open it")).select()
  }

}
