package net.virtualvoid.restic

import org.apache.pekko.actor.ActorSystem
import org.apache.pekko.stream.OverflowStrategy
import org.apache.pekko.stream.connectors.file.ArchiveMetadata
import org.apache.pekko.stream.connectors.file.scaladsl.Archive
import org.apache.pekko.stream.scaladsl.{ Sink, Source }
import org.apache.pekko.util.ByteString
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
  import system.dispatcher
  def initializeIndices(): Future[Any] = {
    val i1 = blob2packIndex
    val i2 = backreferences.initializeIndex()
    val i3 = pack2indexIndex
    val i4 = packInfoIndex
    Future.sequence(Seq(i1, i2, i3, i4))
  }

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

  def cached[T](cacheFileName: String, baseData: String, createCacheFile: File => Future[T], loadCacheFile: File => Future[T]): Future[T] = {
    val cacheFile = new File(localCacheDir, cacheFileName)
    val inFile = new File(cacheFile.getAbsoluteFile + ".in")
    def recreateIndex(): Future[T] = {
      cacheFile.delete()
      inFile.delete()
      createCacheFile(cacheFile).map { res =>
        Utils.writeString(inFile, baseData)
        res
      }
    }

    if (!cacheFile.exists() || !inFile.exists()) {
      Console.err.println(s"[${cacheFile.getName}] Cache missing. Starting rebuild...")
      recreateIndex()
    } else {
      val expectedIn = Utils.readString(inFile)
      if (expectedIn == baseData) // index still valid
        loadCacheFile(cacheFile)
      else {
        Console.err.println(s"[${cacheFile.getName}] Cache invalidated. Starting rebuild...")
        recreateIndex()
      }
    }
  }
  def cachedIndex[T: Serializer](indexName: String, baseData: String, indexData: => Source[(Hash, T), Any]): Future[Index[T]] =
    cached(s"$indexName.idx", baseData, f => Index.createIndex(f, indexData), f => Future.successful(Index.load(f)))

  def cachedIndexFromBaseElements[T: Serializer](indexName: String, baseData: Seq[String], indexData: Seq[String] => Source[(Hash, T), Any]): Future[Index[T]] =
    cachedIndexFromBaseElements(indexName, Future.successful(baseData), indexData)
  def cachedIndexFromBaseElements[T: Serializer](indexName: String, baseDataFut: Future[Seq[String]], indexData: Seq[String] => Source[(Hash, T), Any]): Future[Index[T]] = {
    def inFileFor(cacheFile: File): File = new File(cacheFile.getAbsolutePath + ".in")
    val cacheFiles = localCacheDir.listFiles().filter(f => f.getName.startsWith(s"$indexName.idx") && !f.getName.contains(".in") && f.getName.last.isDigit).toSeq
    val inFiles = cacheFiles.map(inFileFor)
    def deleteCacheFiles(): Unit = {
      cacheFiles.foreach(_.delete())
      inFiles.foreach(_.delete())
    }
    def recreateCompleteIndex(): Future[Index[T]] = {
      deleteCacheFiles()
      cachedIndexFromBaseElements(indexName, baseDataFut, indexData)
    }
    def loadInData(inFile: File): Seq[String] =
      scala.io.Source.fromFile(inFile).getLines().toVector

    baseDataFut.flatMap { baseData =>
      val baseSet = baseData.toSet
      val inDatas = inFiles.flatMap(loadInData).toSet
      val allDataIndexed = inDatas.intersect(baseSet) == baseSet
      val tooMuchDataSet = inDatas.diff(baseSet)

      if (tooMuchDataSet.nonEmpty) {
        Console.err.println(s"[$indexName] Index has ${tooMuchDataSet.size} outdated elements, rebuilding completely.")
        recreateCompleteIndex()
      } else if (allDataIndexed) {
        if (cacheFiles.size < 5) {
          Console.err.println(s"[$indexName] Index from ${cacheFiles.size} parts fully available, loading.")
          Future.successful(Index.composite(cacheFiles.map(f => Index.load(f))))
        } else {
          Console.err.println(s"[$indexName] Index from ${cacheFiles.size} parts fully available, merging...")

          def merge(i1: File, i2: File): File = {
            val res = File.createTempFile(s"$indexName.idx", ".tmp", localCacheDir)
            Index.merge(i1, i2, res)
            if (i1.getName.endsWith(".tmp")) i1.delete()
            if (i2.getName.endsWith(".tmp")) i2.delete()
            res
          }

          val res = cacheFiles.sortBy(_.length()).reduceLeft(merge)
          val newFile = new File(localCacheDir, s"$indexName.idx.0")
          val newIn = inFileFor(newFile)
          deleteCacheFiles()

          Utils.writeString(newIn, baseData.mkString("\n"))
          res.renameTo(newFile)
          Future.successful(Index.load(newFile))
        }
      } else {
        // create new partial index (or merge if necessary)
        val newSet = baseSet.diff(inDatas)
        val newFile = new File(localCacheDir, s"$indexName.idx.${cacheFiles.size}")
        val newInFile = inFileFor(newFile)

        Console.err.println(s"[$indexName] Index from ${cacheFiles.size} parts is missing data from ${newSet.size} base elements. Creating new index part.")

        Index.createIndex(newFile, indexData(newSet.toSeq))
          .flatMap { res =>
            Utils.writeString(newInFile, newSet.toSeq.mkString("\n"))
            cachedIndexFromBaseElements(indexName, baseData, indexData)
          }
      }
    }
  }

  def cachedJson[T: JsonFormat](cacheName: String, baseData: String, createData: () => Future[T]): Future[T] =
    cached(
      s"$cacheName.json.gz",
      baseData,
      { f =>
        createData().map { data =>
          Utils.writeStringGzipped(f, data.toJson.compactPrint)
          data
        }
      },
      f => Future.successful(Utils.readStringGzipped(f).parseJson.convertTo[T])
    )

  lazy val allIndexFileNames = ResticRepository.allFiles(new File(repoDir, "index")).map(_.getName).toSeq

  // Full set of all index files guards our indices. Indices need to be rebuilt when set of index
  // files changes.
  lazy val indexStateString: String = {
    val allIndexHashes =
      allIndexFileNames
        .sorted
        .mkString("\n")
    Utils.sha256sum(allIndexHashes)
  }

  def allPacks: Future[Seq[String]] = pack2indexIndex.map(_.allKeys.map(_.toString))

  private implicit val packEntrySerializer = PackBlobSerializer
  lazy val blob2packIndex: Future[Index[PackEntry]] =
    cachedIndexFromBaseElements("blob2pack", allPacks, indexEntriesFromPacks)

  private def indexEntriesFromPacks(packs: Seq[String]): Source[(Hash, PackEntry), Any] = {
    val packSet = packs.map(Hash(_)).toSet
    val indexSet = indexSetForPacks(packSet)

    Source.futureSource(indexSet.map(Source(_)))
      .mapAsyncUnordered(4)(idx => loadIndex(idx))
      .mapConcat { packIndex =>
        packIndex.packs
          .filter(p => packSet(p.id))
          .flatMap(p => p.blobs.map(b => b.id -> PackEntry(p.id, b.id, b.`type`, b.offset, b.length, b.uncompressed_length)))
      }
  }

  def packEntryFor(blob: Hash): Future[PackEntry] =
    blob2packIndex.map(_.lookup(blob))

  private implicit val hashSerializer = HashSerializer
  private lazy val pack2indexIndex: Future[Index[Hash]] =
    cachedIndexFromBaseElements("pack2index", allIndexFileNames, allPack2IndexEntries)

  def allPack2IndexEntries(indices: Seq[String]): Source[(Hash, Hash), Any] =
    Source(indices)
      .mapAsyncUnordered(16) { idx =>
        val h = Hash(idx)
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

  def indexSetForPacks(packs: Iterable[Hash]): Future[Set[Hash]] =
    pack2indexIndex.map(i => packs.map(i.lookup).toSet)

  def packInfos: Future[Seq[PackInfo]] = packInfoIndex.map(_.allValues)
  private lazy val packInfoIndex: Future[Index[PackInfo]] = {
    def packInfos(packs: Seq[String]): Source[(Hash, PackInfo), Any] = {
      val packSet = packs.map(Hash(_)).toSet
      val indexSet = indexSetForPacks(packSet)

      Source.futureSource(indexSet.map(Source(_)))
        .mapAsyncUnordered(4) { idx =>
          loadIndex(idx).map(_.allInfos.filter(p => packSet(p.id)).map(i => i.id -> i))
        }
        .mapConcat(identity)
    }

    cachedIndexFromBaseElements("packinfos", allPacks, packInfos)
  }

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

      if (uncompressedSize >= 0) {
        require(uncompressedSize >= 0, s"Could not estimate uncompressed size. uncompressed: $uncompressed compressed: ${compressedData.size}")
        val buffer = new Array[Byte](uncompressedSize)
        val d = decompressor.get()
        val size = d.decompress(compressedData, offset, compressedData.length - offset, buffer, 0, buffer.length)

        require(size == buffer.length)
        buffer
      } else {
        println(s"[WARN] Could not estimate uncompressed size. uncompressed: $uncompressed compressed: ${compressedData.size}")
        val buffer = new Array[Byte](math.min(100000, compressedData.size * 10))
        val d = decompressor.get()
        val size = d.decompress(compressedData, offset, compressedData.length - offset, buffer, 0, buffer.length)

        require(size < buffer.length)
        buffer.take(size)
      }
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