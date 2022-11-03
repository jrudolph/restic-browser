package net.virtualvoid.restic

import java.io.{ File, FileInputStream, FileOutputStream }

object Utils {
  def writeString(file: File, string: String): Unit = {
    val fos = new FileOutputStream(file)
    try fos.write(string.getBytes("utf8"))
    finally fos.close()
  }
  def readString(file: File): String = {
    val fis = new FileInputStream(file)
    require(file.length() < Int.MaxValue)
    val data = new Array[Byte](file.length().toInt)
    try {
      val read = fis.read(data)
      require(read == data.length)
    } finally fis.close()
    new String(data, "utf8")
  }
}
