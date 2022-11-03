package net.virtualvoid.restic

import akka.actor.ActorSystem
import akka.stream.OverflowStrategy
import akka.stream.alpakka.file.ArchiveMetadata
import akka.stream.alpakka.file.scaladsl.Archive
import akka.stream.scaladsl.Source
import akka.util.ByteString
import io.airlift.compress.zstd.ZstdDecompressor
import spray.json._

import java.io.{ File, FileInputStream, FileOutputStream }
import java.nio.channels.FileChannel
import java.nio.channels.FileChannel.MapMode
import java.nio.file.Files
import java.nio.{ ByteBuffer, MappedByteBuffer }
import java.util.concurrent.Executors
import javax.crypto.spec.SecretKeySpec
import scala.collection.immutable
import scala.concurrent.duration._
import scala.concurrent.{ Await, ExecutionContext, Future }
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
  val copyExecutor = ExecutionContext.fromExecutor(Executors.newFixedThreadPool(8))
  val keySpec = new SecretKeySpec(masterKey.encrypt.bytes, "AES")
  val decryptor = ThreadLocal.withInitial(() => new Decryptor(keySpec))
  val decompressor = ThreadLocal.withInitial(() => new ZstdDecompressor)

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
  def index[T: Serializer](indexFile: File, baseData: String, indexData: => Source[(Hash, T), Any]): Future[Index[T]] = {
    val inFile = new File(indexFile.getAbsoluteFile + ".in")
    def recreateIndex(): Future[Index[T]] = {
      val idx = Index.createIndex(indexFile, indexData)
      Utils.writeString(inFile, baseData)
      idx
    }

    if (!indexFile.exists() || !inFile.exists()) {
      Console.err.println(s"[${indexFile.getName}] Index missing. Starting rebuild...")
      recreateIndex()
    } else {
      val expectedIn = Utils.readString(inFile)
      if (expectedIn == baseData) // index still valid
        Future.successful(Index.load(indexFile))
      else {
        Console.err.println(s"[${indexFile.getName}] Index invalidated. Starting rebuild...")
        recreateIndex()
      }
    }
  }

  // Full set of all index files guards our indices. Indices need to be rebuilt when set of index
  // files changes.
  lazy val stateString: String =
    ResticRepository.allFiles(new File(repoDir, "index"))
      .map(_.getName)
      .toSeq
      .sorted
      .mkString

  val blob2PackIndexFile = new File(localCacheDir, "blob2pack.idx")

  private implicit val packEntrySerializer = PackBlobSerializer
  lazy val blob2packIndex: Future[Index[PackEntry]] =
    index(blob2PackIndexFile, stateString, allIndexEntries)

  private def allIndexEntries: Source[(Hash, PackEntry), Any] =
    Source(ResticRepository.allFiles(new File(repoDir, "index")))
      .mapAsync(8)(f => loadIndex(Hash(f.getName)))
      .mapConcat { packIndex =>
        packIndex.packs.flatMap(p => p.blobs.map(b => b.id -> PackEntry(p.id, b.id, b.`type`, b.offset, b.length, b.uncompressed_length)))
      }

  def packEntryFor(blob: Hash): Future[PackEntry] =
    blob2packIndex.map(_.lookup(blob))

  val pack2indexIndexFile = new File(localCacheDir, "pack2index.idx")
  private implicit val hashSerializer = HashSerializer
  private lazy val pack2indexIndex: Future[Index[Hash]] =
    index(pack2indexIndexFile, stateString, allPack2IndexEntries)

  def allPack2IndexEntries: Source[(Hash, Hash), Any] =
    Source(ResticRepository.allFiles(new File(repoDir, "index")))
      .mapAsync(1) { f =>
        val h = Hash(f.getName)
        loadIndex(h).map(h -> _)
      }
      .mapConcat {
        case (packIndexHash, idx) => idx.packs.map(_.id -> packIndexHash)
      }

  def packIndexFor(packHash: Hash): Future[PackIndex] =
    for {
      map <- pack2indexIndex
      indexFile <- loadIndex(map.lookup(packHash))
    } yield indexFile.packs.find(_.id == packHash).get

  lazy val backreferences = BackReferences(this)

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
      .map(decompress(uncompressed)(_, 0))

  def decompress(uncompressed: Option[Int])(compressedData: Array[Byte], offset: Int): Array[Byte] =
    if (uncompressed.isDefined) {
      val us0 = uncompressed.get
      val uncompressedSize = if (us0 >= 0) us0 else ZstdDecompressor.getDecompressedSize(compressedData, offset, compressedData.length - offset).toInt
      require(uncompressedSize >= 0, s"uncompressed: $uncompressed compressed: ${compressedData.size}")
      val buffer = new Array[Byte](uncompressedSize)
      val d = decompressor.get()
      val size = d.decompress(compressedData, offset, compressedData.length - offset, buffer, 0, buffer.length)

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
          }(copyExecutor)
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
    packEntryFor(id).flatMap(loadTree)

  def packData(packEntry: PackEntry, alwaysCachePack: Boolean = false): Future[FileLocation] =
    (if (alwaysCachePack) CacheStrategy.Pack else settings.cacheStrategy) match {
      case CacheStrategy.Pack =>
        packFile(packEntry.packId).map(f => FileLocation(f, packEntry.offset, packEntry.length))
      case CacheStrategy.Part =>
        localRepoFile("data", packEntry.packId) match {
          case Some(f) => Future.successful(FileLocation(f, packEntry.offset, packEntry.length))
          case None =>
            repoFile[PackEntry]("part", packEntry.packId, packEntry, "_" + _.offset + ".part",
              { (_, id, entry, cached) =>
                val p = s"data/${id.toString.take(2)}/$id"
                val backing = new File(repoDir, p)
                require(backing.exists(), s"Backing file missing in repo: ${backing}")
                downloadPath(backing, entry.offset, entry.length, cached)
              }).map(FileLocation.fullFile)
        }
    }

  def loadTree(packEntry: PackEntry): Future[TreeBlob] =
    // trees will be packed together so cache full pack
    packData(packEntry, alwaysCachePack = true)
      .flatMap(readJson[TreeBlob](_, CompressionType(packEntry.uncompressed_length)))
  def loadTree(pack: Hash, blob: PackBlob): Future[TreeBlob] =
    loadTree(blob.toEntryOf(pack))

  def loadBlob(id: Hash): Future[Array[Byte]] =
    blob2packIndex.flatMap(i => loadBlob(i.lookup(id)))
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
      .map(decompressToString(_, compressionType))
      .map(_.parseJson)
      .map(_.convertTo[T])

  def decompressToString(data0: Array[Byte], compressionType: CompressionType = CompressionType.Tagged): String = {
    val (uncompressedSize, offset) = compressionType match {
      case CompressionType.Uncompressed  => (None, 0)
      case CompressionType.Compressed(i) => (Some(i), 0)
      case CompressionType.Tagged =>
        data0(0) match {
          case 1           => (None, 1)
          case 2           => (Some(-1), 1)
          case 0x5b | 0x7b => (None, 0) // backwards compatible detection of no compression (start of JSON document)
        }
    }
    if (uncompressedSize.isDefined)
      new String(decompress(uncompressedSize)(data0, offset), "utf8")
    else
      new String(data0, offset, data0.length - offset, "utf8")
  }

  def allSnapshots(): Source[(Hash, Snapshot), Any] =
    Source(ResticRepository.allFiles(new File(repoDir, "snapshots")).toVector)
      .map(f => repoDir.toPath.relativize(f.toPath).toFile)
      .mapAsync(16)(f => loadSnapshot(Hash(f.getName)).map(Hash(f.getName) -> _))

  def dataForLeaf(leaf: TreeLeaf): Source[ByteString, Any] =
    Source(leaf.content)
      .mapAsync(8)(loadBlob)
      .map(ByteString(_))

  def asZip(hash: Hash): Source[ByteString, Any] = {
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
      .map {
        case (path, leaf) =>
          (
            ArchiveMetadata(path),
            dataForLeaf(leaf)
            .preMaterialize()._2 // requires changing the subscription timeout depending on buffer below
          )
      }
      .buffer(10, OverflowStrategy.backpressure)
      .via(Archive.zip())
  }
}