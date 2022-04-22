package net.virtualvoid.restic

import akka.actor.ActorSystem
import akka.stream.scaladsl.{ Sink, Source }

import java.io.{ File, FileInputStream }
import javax.crypto.Cipher
import javax.crypto.spec.{ IvParameterSpec, SecretKeySpec }
import spray.json._

import scala.collection.immutable
import scala.concurrent.{ ExecutionContext, Future }
import scala.util.{ Failure, Success, Try }

sealed trait Hash
object Hash {
  type T = String with Hash

  import spray.json.DefaultJsonProtocol._
  private val simpleHashFormat: JsonFormat[Hash.T] = JsonExtra.deriveFormatFrom[String].apply[T](identity, x => x.asInstanceOf[T])
  implicit val hashFormat: JsonFormat[Hash.T] = DeduplicationCache.cachedFormat(simpleHashFormat)
}

sealed trait BlobType
object BlobType {
  case object Data extends BlobType
  case object Tree extends BlobType

  implicit val blobTypeFormat = new JsonFormat[BlobType] {
    override def read(json: JsValue): BlobType = json match {
      case JsString("tree") => Tree
      case JsString("data") => Data
    }
    override def write(obj: BlobType): JsValue = ???
  }
}

case class TreeNode(
    name:    String,
    content: Option[Seq[Hash.T]],
    subtree: Option[Hash.T]
)
case class TreeBlob(
    nodes: Seq[TreeNode]
)
object TreeBlob {
  import spray.json.DefaultJsonProtocol._
  implicit val nodeFormat = jsonFormat3(TreeNode.apply _)
  implicit val treeBlobFormat = jsonFormat1(TreeBlob.apply _)
}

case class PackBlob(
    id:     Hash.T,
    `type`: BlobType,
    offset: Long,
    length: Int
) {
  def isTree: Boolean = `type` == BlobType.Tree
  def isData: Boolean = `type` == BlobType.Data
}
case class PackIndex(
    id:    Hash.T,
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

class ResticReader(repoDir: File, cpuBoundExecutor: ExecutionContext, blockingExecutor: ExecutionContext) {
  val secret = {
    val fis = new FileInputStream("secret")
    val res = new Array[Byte](32)
    val read = fis.read(res)
    require(read == 32)
    res
  }
  val keySpec = new SecretKeySpec(secret, "AES")

  def decryptBlob(blob: Array[Byte]): Array[Byte] = {
    val cipher = Cipher.getInstance("AES/CTR/NoPadding")
    val iv = blob.take(16)
    val data = blob.drop(16).dropRight(16)
    val ivSpec = new IvParameterSpec(iv)
    cipher.init(Cipher.DECRYPT_MODE, keySpec, ivSpec)

    cipher.doFinal(data)
  }
  def readBlobFile(file: File, offset: Long = 0, length: Int = -1): Future[Array[Byte]] = Future {
    val fis = new FileInputStream(file)
    try {
      fis.skip(offset)
      val len =
        if (length == -1) (file.length() - offset).toInt // FIXME
        else length
      //require(file.length() <= Int.MaxValue)
      val buffer = new Array[Byte](len)
      val read = fis.read(buffer)
      require(read == buffer.length)
      buffer
    } finally fis.close()
  }(blockingExecutor).map(decryptBlob)(cpuBoundExecutor)

  def packFile(id: Hash.T): File =
    new File(dataDir, s"${id.take(2)}/$id")
  def loadTree(pack: Hash.T, blob: PackBlob): Future[TreeBlob] =
    readJson[TreeBlob](packFile(pack), blob.offset, blob.length)

  def readJson[T: JsonFormat](file: File, offset: Long = 0, length: Int = -1): Future[T] =
    readBlobFile(file, offset, length).map(data => new String(data, "utf8").parseJson.convertTo[T])(cpuBoundExecutor)

  val indexDir = new File(repoDir, "index")
  val snapshotDir = new File(repoDir, "snapshots")
  val dataDir = new File(repoDir, "data")

  def allFiles(dir: File): immutable.Iterable[File] = {
    def walk(dir: File): immutable.Iterable[File] = {
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
}

object ResticReaderMain extends App {
  implicit val system = ActorSystem()
  import system.dispatcher

  //val indexFile = "/home/johannes/.cache/restic/0227d36ed1e3dc0d975ca4a93653b453802da67f0b34767266a43d20c9f86275/index/00/006091dfe0cd65b2240f7e05eb6d7df5122f077940619f3a1092da60134a3db0"
  //val dataFile = "/home/johannes/.cache/restic/0227d36ed1e3dc0d975ca4a93653b453802da67f0b34767266a43d20c9f86275/data/5c/5c141f74d422dd3607f0009def9ffd369fc68bf3a7a6214eb8b4d5638085e929"
  //val res = readBlobFile(new File(dataFile), 820)
  //val res = readJson[IndexFile](new File(indexFile))
  val repoDir = new File("/home/johannes/.cache/restic/0227d36ed1e3dc0d975ca4a93653b453802da67f0b34767266a43d20c9f86275/")
  val reader = new ResticReader(repoDir, system.dispatcher, system.dispatchers.lookup("blocking-dispatcher"))

  //val indexFiles = reader.allFiles(indexDir)
  //println(indexFiles.size)

  Source(reader.allFiles(reader.indexDir))
    .mapAsync(1024)(reader.readJson[IndexFile](_))
    .mapConcat(_.packs.flatMap(p => p.blobs.filter(_.isTree).map(_ -> p.id)))
    .async
    .mapAsync[Try[TreeBlob]](1024)(f => reader.loadTree(f._2, f._1).map(Success(_)).recover { case ex => println(s"pack ${f._2} not found while trying to load tree ${f._1.id}"); Failure(ex) })
    .runWith(Sink.seq)
    .onComplete {
      case Success(res) =>
        val (suc, fail) = res.partition(_.isSuccess)
        println(s"Success, loaded ${suc.size} trees successfully, failed to load ${fail.size} trees")
        Thread.sleep(100000)
      case Failure(ex) =>
        ex.printStackTrace()
    }
}

