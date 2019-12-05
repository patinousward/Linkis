package com.webank.wedatasphere.linkis.filesystem.reader

import java.util

import com.webank.wedatasphere.linkis.common.io.resultset.{ResultSet, ResultSetReader}
import com.webank.wedatasphere.linkis.common.io.{MetaData, Record}
import com.webank.wedatasphere.linkis.storage.resultset.table.{TableMetaData, TableRecord}
import com.webank.wedatasphere.linkis.storage.resultset.{ResultSetFactory, ResultSetReader}
import com.webank.wedatasphere.linkis.storage.{LineMetaData, LineRecord}
import org.apache.commons.io.IOUtils
import scala.collection.JavaConversions._

/**
  * .dolphin
  */

class ResultSetTextFileReader extends TextFileReader {

  override def getHeader(): Object = {
    if (reader == null) {
      resultSet = ResultSetFactory.getInstance.getResultSetByPath(fsPath)
      reader = ResultSetReader.getResultSetReader(resultSet, fs.read(fsPath))
    }
    reader.getMetaData match {
      case metadata: LineMetaData => metadata.getMetaData
      case metadata: TableMetaData => metadata.columns.map(_.toString)
    }
  }

  def getTableResultSetBody(): Object = {
    val f = (x: Any) => if (x == null) "NULL" else x.toString
    val recordList = new util.ArrayList[Array[String]]()
    while (reader.hasNext && count <= end) {
      val line = reader.getRecord.asInstanceOf[TableRecord].row.map(f)
      if (count >= start) recordList.add(line)
      count += 1
      totalLine += 1
    }
    recordList
  }

  def getLineResultSetBody(): Object = {
    val recordList = new util.ArrayList[String]()
    while (reader.hasNext && count <= end) {
      val line = reader.getRecord.asInstanceOf[LineRecord].getLine
      if (count >= start) recordList.add(line)
      count += 1
      totalLine += 1
    }
    recordList.foldLeft("")((a, b) => a + "\n" + b)
  }

  override def getBody(): Object = {
    getReturnType match {
      case "2" => getTableResultSetBody()
      case _ => getLineResultSetBody()
    }
  }

  private var reader: ResultSetReader[_ <: MetaData, _ <: Record] = _
  private var resultSet: ResultSet[_ <: MetaData, _ <: Record] = _

  override def getReturnType: String = resultSet.resultSetType()

  override def close(): Unit = IOUtils.closeQuietly(reader)

  override def getHeaderKey: String = "metadata"
}

object ResultSetTextFileReader extends TextFileReaderSelector {

  fileType = Array("dolphin")

  override def select(): TextFileReader = new ResultSetTextFileReader
}
