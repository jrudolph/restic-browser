package net.virtualvoid.restic

import java.io.{ File, FileInputStream }
import javax.crypto.Cipher
import javax.crypto.spec.{ IvParameterSpec, SecretKeySpec }
import spray.json._

case class PackBlob(
    id:     String,
    `type`: String,
    offset: Long,
    length: Long
)
case class PackIndex(
    id:    String,
    blobs: Seq[PackBlob]
)
case class IndexFile(
    packs: Seq[PackIndex]
)
object IndexFile {
  import spray.json.DefaultJsonProtocol._
  implicit val packBlobFormat = jsonFormat4(PackBlob.apply _)
  implicit val packIndexFormat = jsonFormat2(PackIndex.apply _)
  implicit val indexFileFormat = jsonFormat1(IndexFile.apply _)
}

object ResticReaderMain extends App {
  val secret = {
    val fis = new FileInputStream("secret")
    val res = new Array[Byte](32)
    val read = fis.read(res)
    require(read == 32)
    res
  }
  val keySpec = new SecretKeySpec(secret, "AES")
  val cipher = Cipher.getInstance("AES/CTR/NoPadding")

  def decryptBlob(blob: Array[Byte]): Array[Byte] = {
    val iv = blob.take(16)
    val data = blob.drop(16).dropRight(16)
    val ivSpec = new IvParameterSpec(iv)
    cipher.init(Cipher.DECRYPT_MODE, keySpec, ivSpec)
    cipher.doFinal(data)
  }
  def readBlobFile(file: File, offset: Int = 0, length: Int = -1): Array[Byte] = {
    val fis = new FileInputStream(file)
    fis.skip(offset)
    val len =
      if (length == -1) file.length().toInt - offset
      else length
    //require(file.length() <= Int.MaxValue)
    val buffer = new Array[Byte](len)
    val read = fis.read(buffer)
    require(read == buffer.length)
    decryptBlob(buffer)
  }
  def readJson[T: JsonFormat](file: File, offset: Int = 0, length: Int = -1): T =
    new String(readBlobFile(file, offset, length), "utf8").parseJson.convertTo[T]

  val repoDir = new File("/home/johannes/.cache/restic/0227d36ed1e3dc0d975ca4a93653b453802da67f0b34767266a43d20c9f86275/")
  val indexDir = new File(repoDir, "index")
  val snapshotDir = new File(repoDir, "snapshots")
  val dataDir = new File(repoDir, "data")

  def allFiles(dir: File): Iterable[File] = {
    def walk(dir: File): Iterable[File] = {
      require(dir.isDirectory)
      dir.listFiles()
        .filterNot(p => p.getName == "." || p.getName == "..")
        .flatMap { f =>
          if (f.isFile) Iterable(f)
          else walk(f)
        }
    }

    walk(dir)
  }

  val indexFile = "/home/johannes/.cache/restic/0227d36ed1e3dc0d975ca4a93653b453802da67f0b34767266a43d20c9f86275/index/00/006091dfe0cd65b2240f7e05eb6d7df5122f077940619f3a1092da60134a3db0"
  val dataFile = "/home/johannes/.cache/restic/0227d36ed1e3dc0d975ca4a93653b453802da67f0b34767266a43d20c9f86275/data/5c/5c141f74d422dd3607f0009def9ffd369fc68bf3a7a6214eb8b4d5638085e929"
  //val res = readBlobFile(new File(dataFile), 820)
  val res = readJson[IndexFile](new File(indexFile))

  val indexFiles = allFiles(indexDir)
  println(indexFiles.size)

  println(res)
}

