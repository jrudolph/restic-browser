package net.virtualvoid.restic

import akka.actor.ActorSystem
import akka.stream.scaladsl.Source
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
  def open(settings: ResticSettings)(implicit system: ActorSystem): Option[ResticRepository] = {
    def prompt(): String = {
      Console.err.println(s"Enter password for repo at ${settings.repositoryDir}:")
      Try(new String(System.console().readPassword()))
        .getOrElse(throw new IllegalArgumentException("Couldn't read password from shell or RESTIC_PASSWORD_FILE"))
    }
    def loadPasswordFile(fileName: File): String = scala.io.Source.fromFile(fileName).mkString.trim
    val pass =
      settings.repositoryPasswordFile
        .map(loadPasswordFile)
        .getOrElse(prompt)
    openRepository(settings, pass)
  }
  def openRepository(settings: ResticSettings, password: String)(implicit system: ActorSystem): Option[ResticRepository] = {
    val keyDir = new File(settings.repositoryDir, "keys")
    val allKeys = allFiles(keyDir)
    allKeys
      .flatMap(readJsonPlain[Key](_).tryDecrypt(password))
      .headOption
      .map(mk => new ResticRepository(settings, mk))
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
    settings:  ResticSettings,
    masterKey: MasterKey)(implicit val system: ActorSystem) {
  def repoDir: File = settings.repositoryDir

  import system.dispatcher
  val keySpec = new SecretKeySpec(masterKey.encrypt.bytes, "AES")
  val decryptor = ThreadLocal.withInitial(() => new Decryptor(keySpec))

  private var mappedFiles: Map[File, MappedByteBuffer] = Map.empty

  val configFile = new File(settings.repositoryDir, "config")
  val repoConfig = Await.result(readJson[Config](configFile), 3.seconds)
  lazy val repoId = repoConfig.id
  Console.err.println(s"Successfully opened repo $repoId at $repoDir")

  val resticCacheDir = new File(settings.userCache, repoId)
  val localCacheDir = {
    val res = new File(settings.localCache, repoId)
    res.mkdirs()
    res
  }
  val packIndexFile = new File(localCacheDir, "pack.idx")

  private implicit val indexEntrySerializer = PackBlobSerializer
  lazy val packIndex: Future[Index[PackEntry]] =
    if (packIndexFile.exists()) Future.successful(Index.load(packIndexFile))
    else Index.createIndex(packIndexFile, allIndexEntries)

  private def allIndexEntries: Source[(Hash, PackEntry), Any] =
    Source(ResticRepository.allFiles(indexDir))
      .mapAsync(16)(f => loadIndex(f))
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

  private var downloads: Map[File, Future[File]] = Map.empty
  def download(from: File, to: File): Future[File] =
    synchronized {
      downloads.get(from) match {
        case Some(f) => f
        case None =>
          val f = Future {
            val tmpFile = File.createTempFile(to.getName.take(20) + "-", ".tmp", to.getParentFile)
            tmpFile.delete()
            Files.copy(from.toPath, tmpFile.toPath)
            tmpFile.renameTo(to)
            to
          }
          downloads += from -> f
          f
      }
    }

  def packFile(id: Hash): Future[File] = {
    import FileExtension._

    val path = s"${id.toString.take(2)}/$id"
    val res = new File(dataDir, path).resolved
    if (res.exists()) Future.successful(res)
    else {
      val cached = new File(localCacheDir, "data/" + path).resolved
      if (cached.exists()) Future.successful(cached)
      else {
        val backing = new File(repoDir, "data/" + path).resolved
        if (repoDir.exists()) {
          cached.getParentFile.mkdirs()
          download(backing, cached)
        } else Future.failed(new RuntimeException(s"File missing in backing dir: $backing"))
      }
    }
  }

  def loadTree(id: Hash): Future[TreeBlob] =
    packIndex.flatMap(i => loadTree(i.lookup(id)))
  def loadTree(packEntry: PackEntry): Future[TreeBlob] =
    packFile(packEntry.packId).flatMap(readJson[TreeBlob](_, packEntry.offset, packEntry.length))
  def loadTree(pack: Hash, blob: PackBlob): Future[TreeBlob] =
    packFile(pack).flatMap(readJson[TreeBlob](_, blob.offset, blob.length))

  def loadBlob(id: Hash): Future[Array[Byte]] =
    packIndex.flatMap(i => loadBlob(i.lookup(id)))
  def loadBlob(packEntry: PackEntry): Future[Array[Byte]] =
    packFile(packEntry.packId).flatMap(readBlobFile(_, packEntry.offset, packEntry.length))
  def loadBlob(pack: Hash, blob: PackBlob): Future[Array[Byte]] =
    packFile(pack).flatMap(readBlobFile(_, blob.offset, blob.length))

  def loadIndex(file: File): Future[IndexFile] =
    readJson[IndexFile](file)

  def loadSnapshot(file: File): Future[Snapshot] =
    readJson[Snapshot](file)

  def readJson[T: JsonFormat](file: File, offset: Long = 0, length: Int = -1): Future[T] =
    readBlobFile(file, offset, length)
      .map(data => new String(data, "utf8").parseJson.convertTo[T])

  def allSnapshots(): Future[Seq[Snapshot]] =
    Future.traverse(ResticRepository.allFiles(snapshotDir).toVector)(loadSnapshot)

  val indexDir = new File(resticCacheDir, "index")
  val snapshotDir = new File(resticCacheDir, "snapshots")
  val dataDir = new File(resticCacheDir, "data")
}