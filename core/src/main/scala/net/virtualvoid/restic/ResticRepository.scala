package net.virtualvoid.restic

import akka.actor.ActorSystem
import akka.stream.scaladsl.{Sink, Source, StreamConverters}
import akka.util.ByteString
import io.airlift.compress.zstd.ZstdDecompressor
import spray.json._

import java.io.{BufferedOutputStream, File, FileInputStream, FileOutputStream}
import java.nio.channels.FileChannel
import java.nio.channels.FileChannel.MapMode
import java.nio.file.Files
import java.nio.{ByteBuffer, MappedByteBuffer}
import java.util.concurrent.Executors
import java.util.zip.{ZipEntry, ZipOutputStream}
import javax.crypto.spec.SecretKeySpec
import scala.collection.immutable
import scala.concurrent.duration._
import scala.concurrent.{Await, ExecutionContext, Future}
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

  case class FileLocation(file: File, offset: Long, length: Int)
  object FileLocation {
    implicit def fullFile(file: File): FileLocation =
      FileLocation(file, 0, -1)
  }
}

class ResticRepository(
    settings:  ResticSettings,
    masterKey: MasterKey)(implicit val system: ActorSystem) {
  import ResticRepository._

  def repoDir: File = settings.repositoryDir

  import system.dispatcher
  val copyExecutor = ExecutionContext.fromExecutor(Executors.newFixedThreadPool(100))
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
  def readBlobFile(location: FileLocation, uncompressed: Option[Int]): Future[Array[Byte]] =
    readBlobFilePlain(location)
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

  def readBlobFilePlain(location: FileLocation): Future[ByteBuffer] = Future {
    import location._
    val mapped = mappedFileFor(file).duplicate()
    val len = // FIXME: move to FileLocation
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
  def downloadPath(from: File, offset: Long, length: Int, to: File): Future[File] =
    Future {
      println(s"Downloading from ${from} at $offset len: $length")
      val buffer = new Array[Byte](65536)
      val fis = new FileInputStream(from)
      fis.skip(offset)
      val tmpFile = File.createTempFile(to.getName.take(20) + "-", ".tmp", to.getParentFile)
      tmpFile.delete()
      val fos = new FileOutputStream(tmpFile)
      var remaining = length
      while (remaining > 0) {
        val read = fis.read(buffer, 0, math.min(buffer.length, remaining))
        fos.write(buffer, 0, read)
        remaining -= read
      }
      fos.close()
      fis.close()
      tmpFile.renameTo(to)
      require(to.length() == length, s"Copied file should have length ${length} but had ${to.length()}")
      to
    }(copyExecutor)

  def packFile(id: Hash): Future[File] =
    repoFile("data", id)

  def localRepoFile(kind: String, id: Hash): Option[File] =
    localRepoFile[String](kind, id, "", _ => "")
  def localRepoFile[T](kind: String, id: Hash, extra: T, extraPath: T => String): Option[File] = {
    import FileExtension._

    val res = new File(resticCacheDir, s"$kind/${id.toString.take(2)}/$id").resolved
    if (res.exists()) Some(res)
    else {
      val cached = new File(localCacheDir, s"$kind/${id.toString.take(2)}/$id${extraPath(extra)}").resolved
      if (cached.exists()) Some(cached)
      else None
    }
  }

  def repoFile(kind: String, id: Hash, simpleBackingFormat: Boolean = false): Future[File] =
    repoFile[String](kind, id, extra = "", _ => "", (kind, id, _, cached) => {
      import FileExtension._
      val p =
        if (simpleBackingFormat) s"$kind/$id"
        else s"$kind/${id.toString.take(2)}/$id"
      val backing = new File(repoDir, p).resolved
      if (repoDir.exists()) {
        cached.getParentFile.mkdirs()
        download(backing, cached)
      } else Future.failed(new RuntimeException(s"File missing in backing dir: $backing"))
    })
  def repoFile[T](kind: String, id: Hash, extra: T, extraPath: T => String, retrieve: (String, Hash, T, File) => Future[File]): Future[File] = {
    import FileExtension._

    localRepoFile(kind, id, extra, extraPath) match {
      case Some(v) => Future.successful(v)
      case None =>
        val cached = new File(localCacheDir, s"$kind/${id.toString.take(2)}/$id${extraPath(extra)}").resolved // DRY with localRepoFile
        cached.getParentFile.mkdirs()
        retrieve(kind, id, extra, cached)
    }
  }

  def loadTree(id: Hash): Future[TreeBlob] =
    packIndex.flatMap(i => loadTree(i.lookup(id)))

  def packData(packEntry: PackEntry): Future[FileLocation] =
    settings.cacheStrategy match {
      case CacheStrategy.Pack =>
        packFile(packEntry.packId).map(f => FileLocation(f, packEntry.offset, packEntry.length))
      case CacheStrategy.Part =>
        println(s"Looking for ${packEntry.id} at ${packEntry.packId} ${packEntry.offset}")
        localRepoFile("data", packEntry.packId) match {
          case Some(f) => Future.successful(FileLocation(f, packEntry.offset, packEntry.length))
          case None =>
            repoFile[PackEntry]("part", packEntry.packId, packEntry, "_" + _.offset + ".part",
              { (_, id, entry, cached) =>
                val p = s"data/${id.toString.take(2)}/$id"
                val backing = new File(repoDir, p)
                require(backing.exists(), s"Backing file missing in repo: ${backing}")
                println(s"Preparing to download for ${packEntry.id} at ${packEntry.packId} ${packEntry.offset}")
                downloadPath(backing, entry.offset, entry.length, cached)
              }).map(FileLocation.fullFile)
        }
    }

  def loadTree(packEntry: PackEntry): Future[TreeBlob] =
    packData(packEntry).flatMap(readJson[TreeBlob](_, CompressionType(packEntry.uncompressed_length)))
  def loadTree(pack: Hash, blob: PackBlob): Future[TreeBlob] =
    loadTree(blob.toEntryOf(pack))

  def loadBlob(id: Hash): Future[Array[Byte]] =
    packIndex.flatMap(i => loadBlob(i.lookup(id)))
  def loadBlob(packEntry: PackEntry): Future[Array[Byte]] =
    packData(packEntry).flatMap(readBlobFile(_, packEntry.uncompressed_length))
  def loadBlob(pack: Hash, blob: PackBlob): Future[Array[Byte]] =
    loadBlob(blob.toEntryOf(pack))

  def loadIndex(id: Hash): Future[IndexFile] =
    repoFile("index", id, simpleBackingFormat = true).flatMap(readJson[IndexFile](_))

  def loadSnapshot(id: Hash): Future[Snapshot] =
    repoFile("snapshots", id, simpleBackingFormat = true).flatMap(readJson[Snapshot](_))

  def readJson[T: JsonFormat](location: FileLocation, compressionType: CompressionType = CompressionType.Tagged): Future[T] =
    readBlobFile(location, uncompressed = None) // will be dealt with afterwards
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
    StreamConverters.asOutputStream(60.seconds)
      .mapMaterializedValue { os =>
        val zipStream = new ZipOutputStream(new BufferedOutputStream(os, 10000))
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
          .mapConcat {
            case (path, leaf) =>
              // stream leaf into zipStream
              val entry = new ZipEntry(path)
              entry.setSize(leaf.size.getOrElse(0))
              Seq(entry) ++ leaf.content
          }
          .mapAsync(128) {
            case ze: ZipEntry => Future.successful(ze)
            case h: Hash => loadBlob(h)
          }
          .map {
            case ze: ZipEntry =>
              zipStream.flush()
              zipStream.putNextEntry(ze)
            case b: Array[Byte] =>
              zipStream.write(b)
          }
          .runWith(Sink.ignore)
          .transform { x =>
            zipStream.close() // FIXME: fail on failure
            x
          }
      }
}