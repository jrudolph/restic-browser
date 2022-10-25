package net.virtualvoid.restic

import akka.actor.ActorSystem
import akka.stream.{ ActorAttributes, Attributes }
import akka.stream.scaladsl.{ Sink, Source, StreamConverters }
import akka.util.ByteString
import io.airlift.compress.zstd.ZstdDecompressor
import net.virtualvoid.restic.ResticRepository.CompressionType
import spray.json._

import java.io.{ BufferedOutputStream, File }
import java.nio.channels.FileChannel
import java.nio.channels.FileChannel.MapMode
import java.nio.file.Files
import java.nio.{ ByteBuffer, MappedByteBuffer }
import java.util.zip.{ ZipEntry, ZipOutputStream }
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
    Console.err.println(s"Trying to open repository at ${settings.repositoryDir}")
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

  sealed trait CompressionType
  object CompressionType {
    case object Uncompressed extends CompressionType
    case class Compressed(uncompressedLength: Int) extends CompressionType
    case object Tagged extends CompressionType

    def apply(compressed: Option[Int]): CompressionType =
      compressed match {
        case Some(l) => Compressed(l)
        case None    => Uncompressed
      }
  }
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
    Source(ResticRepository.allFiles(new File(repoDir, "index")))
      .mapAsync(1024)(f => loadIndex(Hash(f.getName)))
      .mapConcat { packIndex =>
        packIndex.packs.flatMap(p => p.blobs.map(b => b.id -> PackEntry(p.id, b.id, b.`type`, b.offset, b.length, b.uncompressed_length)))
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
  def readBlobFile(file: File, offset: Long = 0, length: Int = -1, uncompressed: Option[Int]): Future[Array[Byte]] =
    readBlobFilePlain(file, offset, length)
      .map(decryptBlob)
      .map(decompress(uncompressed))

  def decompress(uncompressed: Option[Int])(compressedData: Array[Byte]): Array[Byte] =
    if (uncompressed.isDefined) {
      val decompressor = new ZstdDecompressor
      val us0 = uncompressed.get
      val uncompressedSize = if (us0 >= 0) us0 else ZstdDecompressor.getDecompressedSize(compressedData, 0, compressedData.length).toInt
      require(uncompressedSize >= 0, s"uncompressed: $uncompressed compressed: ${compressedData.size}")
      val buffer = new Array[Byte](uncompressedSize)
      val size = decompressor.decompress(compressedData, 0, compressedData.length, buffer, 0, buffer.length)
      require(size == buffer.length)
      buffer
    } else
      compressedData

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
            val start = System.nanoTime()
            Console.err.println(s"Downloading $from into cache")
            val tmpFile = File.createTempFile(to.getName.take(20) + "-", ".tmp", to.getParentFile)
            tmpFile.delete()
            Files.copy(from.toPath, tmpFile.toPath)
            tmpFile.renameTo(to)
            val lastedMillis = (System.nanoTime() - start) / 1000000
            Console.err.println(f"Downloading $from into cache finished in $lastedMillis%5d ms size: ${to.length() / 1000}%5d kB speed: ${to.length().toFloat / lastedMillis}%5.0f kB/s")
            to
          }
          downloads += from -> f
          f
      }
    }

  def packFile(id: Hash): Future[File] =
    repoFile("data", id)

  def repoFile(kind: String, id: Hash, simpleBackingFormat: Boolean = false): Future[File] = {
    import FileExtension._

    val res = new File(resticCacheDir, s"$kind/${id.toString.take(2)}/$id").resolved
    if (res.exists()) Future.successful(res)
    else {
      val cached = new File(localCacheDir, s"$kind/${id.toString.take(2)}/$id").resolved
      if (cached.exists()) Future.successful(cached)
      else {
        val p =
          if (simpleBackingFormat) s"$kind/$id"
          else s"$kind/${id.toString.take(2)}/$id"
        val backing = new File(repoDir, p).resolved
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
    packFile(packEntry.packId).flatMap(readJson[TreeBlob](_, packEntry.offset, packEntry.length, CompressionType(packEntry.uncompressed_length)))
  def loadTree(pack: Hash, blob: PackBlob): Future[TreeBlob] =
    packFile(pack).flatMap(readJson[TreeBlob](_, blob.offset, blob.length, CompressionType(blob.uncompressed_length)))

  def loadBlob(id: Hash): Future[Array[Byte]] =
    packIndex.flatMap(i => loadBlob(i.lookup(id)))
  def loadBlob(packEntry: PackEntry): Future[Array[Byte]] =
    packFile(packEntry.packId).flatMap(readBlobFile(_, packEntry.offset, packEntry.length, packEntry.uncompressed_length))
  def loadBlob(pack: Hash, blob: PackBlob): Future[Array[Byte]] =
    packFile(pack).flatMap(readBlobFile(_, blob.offset, blob.length, blob.uncompressed_length))

  def loadIndex(id: Hash): Future[IndexFile] =
    repoFile("index", id, simpleBackingFormat = true).flatMap(readJson[IndexFile](_))

  def loadSnapshot(id: Hash): Future[Snapshot] =
    repoFile("snapshots", id, simpleBackingFormat = true).flatMap(readJson[Snapshot](_))

  def readJson[T: JsonFormat](file: File, offset: Long = 0, length: Int = -1, compressionType: CompressionType = CompressionType.Tagged): Future[T] =
    readBlobFile(file, offset, length, uncompressed = None) // will be dealt with afterwards
      .map { data0 =>
        val (uncompressedSize, data1) = compressionType match {
          case CompressionType.Uncompressed  => (None, data0)
          case CompressionType.Compressed(i) => (Some(i), data0)
          case CompressionType.Tagged =>
            data0(0) match {
              case 1           => (None, data0.drop(1))
              case 2           => (Some(-1), data0.drop(1))
              case 0x5b | 0x7b => (None, data0) // backwards compatible detection of no compression (start of JSON document)
            }
        }
        val data =
          if (uncompressedSize.isDefined)
            decompress(uncompressedSize)(data1)
          else
            data1

        new String(data, "utf8").parseJson.convertTo[T]
      }

  def allSnapshots(): Source[(Hash, Snapshot), Any] =
    Source(ResticRepository.allFiles(new File(repoDir, "snapshots")).toVector)
      .map(f => repoDir.toPath.relativize(f.toPath).toFile)
      .mapAsync(16)(f => loadSnapshot(Hash(f.getName)).map(Hash(f.getName) -> _))

  def asZip(hash: Hash): Source[ByteString, Future[Any]] =
    StreamConverters.asOutputStream().addAttributes(Attributes(ActorAttributes.Dispatcher("akka.actor.default-dispatcher")))
      .mapMaterializedValue { os =>
        val zipStream = new ZipOutputStream(new BufferedOutputStream(os, 1000000))
        zipStream.setLevel(0)

        def walk(hash: Hash, pathPrefix: String): Source[(String, TreeLeaf), Any] =
          Source.futureSource(
            loadTree(hash)
              .map { b =>
                val leafs = b.nodes.collect { case l: TreeLeaf => (pathPrefix + l.name) -> l }
                val subtrees = b.nodes.collect { case b: TreeBranch => walk(b.subtree, pathPrefix + b.name + "/") }
                Source(leafs).concat(Source(subtrees).flatMapConcat(identity))
              }
          )

        walk(hash, "")
          .mapAsync(1) {
            case (path, leaf) =>
              // stream leaf into zipStream
              val entry = new ZipEntry(path)
              entry.setSize(leaf.size.getOrElse(0))
              zipStream.putNextEntry(entry)

              Source(leaf.content)
                .mapAsync(8)(loadBlob)
                .mapAsync(1)(b => Future(zipStream.write(b)))
                .runWith(Sink.ignore)
          }
          .runWith(Sink.ignore)
          .transform { x =>
            zipStream.close() // FIXME: fail on failure
            x
          }
      }
}