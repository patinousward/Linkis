package com.webank.wedatasphere.linkis.filesystem.reader

trait LineShuffle {

  def shuffle(line: String): String = line

  def shuffle(line: Array[Any]): Array[String] = {
    val f = (x: Any) => if (x == null) "NULL" else x.toString
    line.map(f)
  }

}
