package net.virtualvoid.restic

import spray.json._

import java.io.{ File, FileInputStream }
import java.nio.{ ByteBuffer, MappedByteBuffer }
import java.nio.channels.FileChannel
import java.nio.channels.FileChannel.MapMode
import java.nio.file.Files
import javax.crypto.Cipher
import javax.crypto.spec.{ IvParameterSpec, SecretKeySpec }
import scala.collection.immutable
import scala.concurrent.{ ExecutionContext, Future }

class ResticReader(
    repoDir:          File,
    backingDir:       File,
    cacheDir:         File,
    cpuBoundExecutor: ExecutionContext,
    blockingExecutor: ExecutionContext) {
  val secret = {
    val fis = new FileInputStream("secret")
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
    mapped.position(offset.toInt).limit(offset.toInt + len).asInstanceOf[ByteBuffer]
  }(blockingExecutor).map(decryptBlob)(cpuBoundExecutor)

  def decryptBlob(blob: ByteBuffer): Array[Byte] = {
    val cipher = Cipher.getInstance("AES/CTR/NoPadding")
    val ivBuffer = new Array[Byte](16)
    blob.get(ivBuffer, 0, 16)
      .limit(blob.limit() - 16)

    val ivSpec = new IvParameterSpec(ivBuffer)
    cipher.init(Cipher.DECRYPT_MODE, keySpec, ivSpec)

    val outputBuffer = ByteBuffer.allocate(blob.remaining())
    cipher.doFinal(blob, outputBuffer)
    outputBuffer.array()
  }

  def packFile(id: Hash.T): File = {
    import FileExtension._

    val path = s"${id.take(2)}/$id"
    val res = new File(dataDir, path).resolved
    if (res.exists()) res
    else {
      val cached = new File(cacheDir, "data/" + path).resolved
      if (cached.exists()) cached
      else {
        val backing = new File(backingDir, "data/" + path).resolved
        if (backingDir.exists()) {
          cached.getParentFile.mkdirs()
          Files.copy(backing.toPath, cached.toPath)
          cached
        } else throw new RuntimeException(s"File missing in backing dir: $backing")
      }
    }
  }

  def loadTree(pack: Hash.T, blob: PackBlob): Future[TreeBlob] =
    readJson[TreeBlob](packFile(pack), blob.offset, blob.length)

  def loadIndex(file: File): Future[IndexFile] =
    readJson[IndexFile](file)

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