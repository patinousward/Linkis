package com.webank.wedatasphere.linkis.filesystem.reader
import java.util

import com.webank.wedatasphere.linkis.common.io.{FsPath, MetaData, Record}
import com.webank.wedatasphere.linkis.common.io.resultset.{ResultSet, ResultSetReader}
import com.webank.wedatasphere.linkis.filesystem.exception.WorkSpaceException
import com.webank.wedatasphere.linkis.storage.{LineMetaData, LineRecord}
import com.webank.wedatasphere.linkis.storage.fs.FileSystem
import com.webank.wedatasphere.linkis.storage.resultset.table.{TableMetaData, TableRecord}
import com.webank.wedatasphere.linkis.storage.resultset.{ResultSetFactory, ResultSetReader}
import org.apache.commons.io.IOUtils
import scala.collection.JavaConversions._

class ResultSetPager extends Pager {

  override def getHeader(): Object = {
    if(reader == null) throw new WorkSpaceException("The startPage method needs to be called first")
    reader.getMetaData match {
      case metadata:LineMetaData =>metadata.getMetaData
      case metadata:TableMetaData =>metadata.columns.map(_.toString)
    }
  }

  def getTableResultSetBody(): Object = {
    val f=(x:Any)=>if(x == null) "NULL" else x.toString
    val recordList = new util.ArrayList[Array[String]]()
    while (reader.hasNext && count<=end){
      val line = reader.getRecord.asInstanceOf[TableRecord].row.map(f)
      if(count >=start)recordList.add(line)
      count += 1
      totalLine +=1
    }
    recordList
  }

  def getLineResultSetBody(): Object = {
    val recordList = new util.ArrayList[String]()
    while (reader.hasNext && count<=end){
      val line = reader.getRecord.asInstanceOf[LineRecord].getLine
      if(count >=start)recordList.add(line)
      count += 1
      totalLine +=1
    }
    recordList.foldLeft("")((a,b)=>a + "\n" + b)
  }

  override def getBody(): Object = {
    getType match {
      case "2" => getTableResultSetBody()
      case _ =>getLineResultSetBody()
    }
  }

  private var reader:ResultSetReader[_<:MetaData, _<:Record] = _
  private var resultSet:ResultSet[_<:MetaData, _<:Record] = _

  override def startPage(fsPath:FsPath, fs:FileSystem, params:java.util.Map[String,String]): Unit = {
    super.startPage(fsPath,fs,params)
    resultSet = ResultSetFactory.getInstance.getResultSetByPath(fsPath)
    reader = ResultSetReader.getResultSetReader(resultSet, fs.read(fsPath))
  }

  override def getType: String = resultSet.resultSetType()

  override def close(): Unit = IOUtils.closeQuietly(reader)

  override def getHeaderKey: String = "metadata"
}
