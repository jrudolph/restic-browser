package net.virtualvoid.restic

import akka.NotUsed
import akka.actor.ActorSystem
import akka.stream.scaladsl.{ Sink, Source }
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
import scala.util.Success

object FileExtension {
  implicit class FileImplicits(val f: File) extends AnyVal {
    def resolved: File =
      if (f.getName.length == 64) f // shortcut when full hashes are used
      else if (f.exists()) f
      else {
        val cands = f.getParentFile.listFiles().filter(_.getName startsWith f.getName)
        cands.size match {
          case 1 => cands.head
          case 0 => f // no resolution possible return original
          case _ => throw new RuntimeException(s"Ambiguous ref: $f can be resolved to all of [${cands.mkString(", ")}]")
        }
      }
  }
}

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

object ResticReaderMain extends App {
  implicit val system = ActorSystem()
  import system.dispatcher

  //val indexFile = "/home/johannes/.cache/restic/0227d36ed1e3dc0d975ca4a93653b453802da67f0b34767266a43d20c9f86275/index/00/006091dfe0cd65b2240f7e05eb6d7df5122f077940619f3a1092da60134a3db0"
  val dataFile = "/home/johannes/.cache/restic/0227d36ed1e3dc0d975ca4a93653b453802da67f0b34767266a43d20c9f86275/data/5c/5c141f74d422dd3607f0009def9ffd369fc68bf3a7a6214eb8b4d5638085e929"
  //val res = readBlobFile(new File(dataFile), 820)
  //val res = readJson[IndexFile](new File(indexFile))
  val repoDir = new File("/home/johannes/.cache/restic/0227d36ed1e3dc0d975ca4a93653b453802da67f0b34767266a43d20c9f86275/")
  val backingDir = new File("/tmp/restic-repo")
  val cacheDir = {
    val res = new File("restic-cache")
    res.mkdirs()
    res
  }
  val reader = new ResticReader(repoDir, backingDir, cacheDir, system.dispatcher, system.dispatchers.lookup("blocking-dispatcher"))
  val indexFile = new File("index.out")

  //val indexFiles = reader.allFiles(indexDir)
  //println(indexFiles.size)
  //reader.readJson[TreeBlob](new File(dataFile), 0, 419).onComplete(println)

  def loadIndex(): Future[Map[String, (Hash.T, PackBlob)]] = {
    Source(reader.allFiles(reader.indexDir))
      .mapAsync(1024)(reader.loadIndex)
      .mapConcat(_.packs.flatMap(p => p.blobs.map(b => b.id -> (p.id, b))))
      .runWith(Sink.seq)
      .map(_.toMap)
  }
  lazy val index = bench("loadIndex")(loadIndex())
  /*index.onComplete {
    case Success(res) =>
      //System.gc()
      //System.gc()
      println(s"Loaded ${res.size} index entries")

      if (!indexFile.exists())
        benchSync("writeIndex")(writeIndexFile(res))
    //Thread.sleep(100000)
  }*/

  lazy val index2: Future[Index] = Future.successful(Index.load(indexFile))

  def loadTree(id: String): Future[TreeBlob] =
    for {
      i <- index2
      (p, b) = i.lookup(id.asInstanceOf[Hash.T])
      tree <- reader.loadTree(p, b)
    } yield tree

  lazy val allTrees: Future[Seq[Hash.T]] =
    index.map { i =>
      benchSync("allTrees") {
        i.values.filter(_._2.isTree).map(_._2.id).toVector
      }
    }

  def walkTreeNodes(blob: TreeBlob): Source[TreeNode, NotUsed] = {
    val subtrees = blob.nodes.collect { case b: TreeBranch => b }
    Source(blob.nodes) ++
      Source(subtrees)
      .mapAsync(1024)(x => loadTree(x.subtree))
      .flatMapConcat(walkTreeNodes)
  }

  sealed trait Reference
  case class SnapshotReference(id: Hash.T) extends Reference
  case class TreeReference(treeBlobId: Hash.T, node: TreeNode) extends Reference
  case class BlobReference(id: Hash.T, referenceChain: Seq[Reference]) {
    def chainString: String = {
      def refString(ref: Reference): String = ref match {
        case SnapshotReference(id)  => s"snap:${id.take(10)}"
        case TreeReference(_, node) => node.name
      }

      referenceChain.reverse.map(refString).mkString("/")
    }
  }
  def reverseReferences(treeId: Hash.T, chain: List[Reference]): Future[Vector[BlobReference]] =
    loadTree(treeId).flatMap { blob =>
      val subdirs = blob.nodes.collect { case b: TreeBranch => b }

      val leaves =
        blob.nodes
          .flatMap {
            case node @ TreeLeaf(_, content) =>
              content.map(c => BlobReference(c, TreeReference(treeId, node) :: chain))
            case _ => Vector.empty
          }

      Future.traverse(subdirs)(b => reverseReferences(b.subtree, TreeReference(treeId, b) :: chain)).map(_.flatten ++ leaves)
    }

  def bench[T](desc: String)(f: => Future[T]): Future[T] = {
    val start = System.nanoTime()
    val res = f
    res.onComplete { res =>
      val end = System.nanoTime()
      val lastedMillis = (end - start) / 1000000
      println(f"[$desc] $lastedMillis%4d ms")
    }
    res
  }
  def benchSync[T](desc: String)(f: => T): T = bench[T](desc)(Future.successful(f)).value.get.get

  //  {
  //    //println(s"walking ${node.name}")
  //    node match {
  //      case b @ TreeBranch(_, subtree) =>
  //        Source.futureSource(
  //          loadTree(subtree).map { t =>
  //            val (subtrees, others) = t.nodes.partition(_.isBranch)
  //
  //
  //          }
  //
  //        )
  //      case x => Source.empty
  //    }
  //  }

  /*loadTree("5ea8baa28b12c186a50d13a88c902b98063339cb9fb1227a59e9376d72f98a8a")
    .map { t =>
      walkTreeNodes(t)
        .runFold(Map.empty[String, Int].withDefaultValue(0))((s, n) => s.updatedWith(n.productPrefix)(x => Some(x.getOrElse(0) + 1)))
        .onComplete(println)
    }*/

  reverseReferences("a5b1b14e1b87f8e4804604065179d8edfd0815752b386b18de8660d099856d70".asInstanceOf[Hash.T], Nil)
    .onComplete {
      case Success(v) =>
        println(s"Got ${v.size} entries")
        val grouped = v.groupBy(_.id).toVector.sortBy(-_._2.size)

        grouped.take(10).foreach {
          case (id, refs) =>
            val size = index2.value.get.get.lookup(id)._2.length

            println()
            println(f"${refs.size}%3d $id%64s size: $size%6d")
            refs.take(20).foreach(r => println(r.chainString))
        }

        system.terminate()
    }

  /*loadTree("5ea8baa28b12c186a50d13a88c902b98063339cb9fb1227a59e9376d72f98a8a")
    .map { t =>
      walkTreeNodes(t)
        .mapConcat { case l: TreeLeaf => l.content; case _ => Vector.empty }
        .runWith(Sink.seq)
        .map(_.toSet.size)
        .onComplete(println)
    }*/

  /*Source.futureSource(allTrees.map(Source(_)))
    .mapAsync[Try[TreeBlob]](1024)(treeId => loadTree(treeId).map(Success(_)).recover {
      case ex =>
        println(s"Failure while trying to load tree ${treeId}: ${ex.getMessage}");
        Failure(ex)
    }
    )
    .runWith(Sink.seq)
    .onComplete {
      case Success(res) =>
        val (suc, fail) = res.partition(_.isSuccess)
        println(s"Success, loaded ${suc.size} trees successfully, failed to load ${fail.size} trees")
        Thread.sleep(100000)
      case Failure(ex) =>
        ex.printStackTrace()
    }*/
}

