package net.virtualvoid.restic

import akka.actor.ActorSystem
import akka.stream.scaladsl.Source
import net.virtualvoid.restic.ResticReaderMain.reader
import spray.json._

import java.io.File
import java.nio.channels.FileChannel
import java.nio.channels.FileChannel.MapMode
import java.nio.file.Files
import java.nio.{ ByteBuffer, MappedByteBuffer }
import javax.crypto.spec.SecretKeySpec
import scala.collection.immutable
import scala.concurrent.duration._
import scala.concurrent.{ Await, Future }
import scala.util.Try

object ResticRepository {
  def open(repoDir: File, cacheBaseDir: File)(implicit system: ActorSystem): Option[ResticRepository] = {
    def prompt(): String = {
      Console.err.println(s"Enter password for repo at $repoDir:")
      Try(new String(System.console().readPassword()))
        .getOrElse(throw new IllegalArgumentException("Couldn't read password from shell or RESTIC_PASSWORD_FILE"))
    }
    def loadPasswordFile(fileName: String): String = scala.io.Source.fromFile(fileName).mkString.trim
    val pass =
      Try(system.settings.config.getString("restic.password-file"))
        .toOption
        .orElse(sys.env.get("RESTIC_PASSWORD_FILE"))
        .map(loadPasswordFile)
        .getOrElse(prompt)
    openRepository(repoDir, cacheBaseDir, pass)
  }
  def openRepository(repoDir: File, cacheBaseDir: File, password: String)(implicit system: ActorSystem): Option[ResticRepository] = {
    val keyDir = new File(repoDir, "keys")
    val allKeys = allFiles(keyDir)
    allKeys
      .flatMap(readJsonPlain[Key](_).tryDecrypt(password))
      .headOption
      .map(mk => new ResticRepository(repoDir, mk, cacheBaseDir))
  }

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

  def readJsonPlain[T: JsonFormat](file: File): T =
    scala.io.Source.fromFile(file).mkString.parseJson.convertTo[T]
}

class ResticRepository(
    repoDir:      File,
    masterKey:    MasterKey,
    cacheBaseDir: File)(implicit val system: ActorSystem) {
  import system.dispatcher
  val keySpec = new SecretKeySpec(masterKey.encrypt.bytes, "AES")
  val decryptor = ThreadLocal.withInitial(() => new Decryptor(keySpec))

  private var mappedFiles: Map[File, MappedByteBuffer] = Map.empty

  val configFile = new File(repoDir, "config")
  val repoConfig = Await.result(readJson[Config](configFile), 3.seconds)
  lazy val repoId = repoConfig.id
  Console.err.println(s"Successfully opened repo $repoId at $repoDir")

  val resticCacheDir = new File(s"${sys.env("HOME")}/.cache/restic/$repoId")
  val localCacheDir = {
    val res = new File(cacheBaseDir, repoId)
    res.mkdirs()
    res
  }
  val packIndexFile = new File(localCacheDir, "pack.idx")

  private implicit val indexEntrySerializer = PackBlobSerializer
  lazy val packIndex: Future[Index[PackEntry]] =
    if (packIndexFile.exists()) Future.successful(Index.load(packIndexFile))
    else Index.createIndex(packIndexFile, allIndexEntries)

  private def allIndexEntries: Source[(Hash, PackEntry), Any] =
    Source(ResticRepository.allFiles(reader.indexDir))
      .mapAsync(16)(f => reader.loadIndex(f))
      .mapConcat { packIndex =>
        packIndex.packs.flatMap(p => p.blobs.map(b => b.id -> PackEntry(p.id, b.id, b.`type`, b.offset, b.length)))
      }

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
  def readBlobFile(file: File, offset: Long = 0, length: Int = -1): Future[Array[Byte]] =
    readBlobFilePlain(file, offset, length).map(decryptBlob)

  def readBlobFilePlain(file: File, offset: Long = 0, length: Int = -1): Future[ByteBuffer] = Future {
    val mapped = mappedFileFor(file).duplicate()
    val len =
      if (length == -1) (file.length() - offset).toInt
      else length
    mapped.position(offset.toInt).limit(offset.toInt + len)
  }

  def decryptBlob(blob: ByteBuffer): Array[Byte] = decryptor.get().decrypt(blob)

  def packFile(id: Hash): File = {
    import FileExtension._

    val path = s"${id.toString.take(2)}/$id"
    val res = new File(dataDir, path).resolved
    if (res.exists()) res
    else {
      val cached = new File(localCacheDir, "data/" + path).resolved
      if (cached.exists()) cached
      else {
        val backing = new File(repoDir, "data/" + path).resolved
        if (repoDir.exists()) {
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
      .map(data => new String(data, "utf8").parseJson.convertTo[T])

  val indexDir = new File(resticCacheDir, "index")
  val snapshotDir = new File(resticCacheDir, "snapshots")
  val dataDir = new File(resticCacheDir, "data")
}