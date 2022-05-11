package net.virtualvoid.restic

import akka.actor.ActorSystem
import akka.stream.scaladsl.Source
import net.virtualvoid.restic.ResticReaderMain.reader
import spray.json._

import java.io.{ File, FileInputStream }
import java.nio.channels.FileChannel
import java.nio.channels.FileChannel.MapMode
import java.nio.file.Files
import java.nio.{ ByteBuffer, MappedByteBuffer }
import javax.crypto.Cipher
import javax.crypto.spec.{ IvParameterSpec, SecretKeySpec }
import scala.collection.immutable
import scala.concurrent.{ ExecutionContext, Future }

class ResticReader(
    repoDir:          File,
    backingDir:       File,
    cacheBaseDir:     File,
    cpuBoundExecutor: ExecutionContext,
    blockingExecutor: ExecutionContext)(implicit val system: ActorSystem) {
  import system.dispatcher

  val repoId = repoDir.getName
  val cacheDir = {
    val res = new File(cacheBaseDir, repoId)
    res.mkdirs()
    res
  }
  val packIndexFile = new File(cacheDir, "pack.idx")

  private implicit val indexEntrySerializer = PackBlobSerializer
  lazy val packIndex: Future[Index[PackEntry]] =
    if (packIndexFile.exists()) Future.successful(Index.load(packIndexFile))
    else Index.createIndex(packIndexFile, allIndexEntries)

  private def allIndexEntries: Source[(Hash, PackEntry), Any] =
    Source(reader.allFiles(reader.indexDir))
      .mapAsync(16)(f => reader.loadIndex(f))
      .mapConcat { packIndex =>
        packIndex.packs.flatMap(p => p.blobs.map(b => b.id -> PackEntry(p.id, b.id, b.`type`, b.offset, b.length)))
      }

  val secret = {
    val fis = new FileInputStream("../secret")
    val res = new Array[Byte](32)
    val read = fis.read(res)
    require(read == 32)
    res
  }
  val keySpec = new SecretKeySpec(secret, "AES")

  private var mappedFiles: Map[File, MappedByteBuffer] = Map.empty
  private def mappedFileFor(file: File): MappedByteBuffer = synchronized {
    mappedFiles.get(file) match {
      case Some(b) => b
      case None =>
        val ch = FileChannel.open(file.toPath)
        val buffer = ch.map(MapMode.READ_ONLY, 0, file.length())
        mappedFiles += (file -> buffer)
        buffer
    }
  }
  def readBlobFile(file: File, offset: Long = 0, length: Int = -1): Future[Array[Byte]] = Future {
    val mapped = mappedFileFor(file).duplicate()
    val len =
      if (length == -1) (file.length() - offset).toInt
      else length
    mapped.position(offset.toInt).limit(offset.toInt + len)
  } /*(blockingExecutor)*/ .map(decryptBlob) /*(cpuBoundExecutor)*/

  class Decryptor {
    val cipher = Cipher.getInstance("AES/CTR/NoPadding")
    val ivBuffer = new Array[Byte](16)

    def decrypt(in: ByteBuffer): Array[Byte] = {
      in.get(ivBuffer, 0, 16)
        .limit(in.limit() - 16)
      val ivSpec = new IvParameterSpec(ivBuffer)
      cipher.init(Cipher.DECRYPT_MODE, keySpec, ivSpec)
      val outputBuffer = ByteBuffer.allocate(in.remaining())
      cipher.doFinal(in, outputBuffer)
      outputBuffer.array()
    }
  }

  val decryptor = new ThreadLocal[Decryptor] {
    override def initialValue(): Decryptor = new Decryptor
  }
  def decryptBlob(blob: ByteBuffer): Array[Byte] = decryptor.get().decrypt(blob)

  def packFile(id: Hash): File = {
    import FileExtension._

    val path = s"${id.toString.take(2)}/$id"
    val res = new File(dataDir, path).resolved
    if (res.exists()) res
    else {
      val cached = new File(cacheDir, "data/" + path).resolved
      if (cached.exists()) cached
      else {
        val backing = new File(backingDir, "data/" + path).resolved
        if (backingDir.exists()) {
          cached.getParentFile.mkdirs()
          val tmpFile = File.createTempFile(cached.getName.take(20) + "-", ".tmp", cached.getParentFile)
          tmpFile.delete()
          Files.copy(backing.toPath, tmpFile.toPath)
          tmpFile.renameTo(cached)
          cached
        } else throw new RuntimeException(s"File missing in backing dir: $backing")
      }
    }
  }

  def loadTree(id: Hash): Future[TreeBlob] =
    packIndex.flatMap(i => loadTree(i.lookup(id)))
  def loadTree(packEntry: PackEntry): Future[TreeBlob] =
    readJson[TreeBlob](packFile(packEntry.packId), packEntry.offset, packEntry.length)
  def loadTree(pack: Hash, blob: PackBlob): Future[TreeBlob] =
    readJson[TreeBlob](packFile(pack), blob.offset, blob.length)

  def loadBlob(id: Hash): Future[Array[Byte]] =
    packIndex.flatMap(i => loadBlob(i.lookup(id)))
  def loadBlob(packEntry: PackEntry): Future[Array[Byte]] =
    readBlobFile(packFile(packEntry.packId), packEntry.offset, packEntry.length)
  def loadBlob(pack: Hash, blob: PackBlob): Future[Array[Byte]] =
    readBlobFile(packFile(pack), blob.offset, blob.length)

  def loadIndex(file: File): Future[IndexFile] =
    readJson[IndexFile](file)

  def loadSnapshot(file: File): Future[Snapshot] =
    readJson[Snapshot](file)

  def readJson[T: JsonFormat](file: File, offset: Long = 0, length: Int = -1): Future[T] =
    readBlobFile(file, offset, length)
      .map(data => new String(data, "utf8").parseJson.convertTo[T])(cpuBoundExecutor)

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