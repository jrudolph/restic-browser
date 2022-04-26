package net.virtualvoid.restic

import akka.NotUsed
import akka.actor.ActorSystem
import akka.stream.scaladsl.{ Sink, Source }
import net.virtualvoid.restic.Hash.T

import java.io.{ BufferedOutputStream, File, FileInputStream, FileOutputStream }
import javax.crypto.Cipher
import javax.crypto.spec.{ IvParameterSpec, SecretKeySpec }
import spray.json._

import java.nio.{ ByteBuffer, ByteOrder, MappedByteBuffer }
import java.nio.channels.FileChannel
import java.nio.channels.FileChannel.MapMode
import java.nio.file.Files
import java.util
import java.util.{ Collections, Random }
import scala.annotation.tailrec
import scala.collection.immutable
import scala.concurrent.{ ExecutionContext, Future }
import scala.util.{ Failure, Success, Try }

sealed trait Hash
object Hash {
  type T = String with Hash

  import spray.json.DefaultJsonProtocol._
  private val simpleHashFormat: JsonFormat[Hash.T] =
    // use truncated hashes for lesser memory usage
    JsonExtra.deriveFormatFrom[String].apply[T](identity, x => x /*.take(12)*/ .asInstanceOf[T])
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

sealed trait CachedName
object CachedName {
  type T = String with CachedName

  import spray.json.DefaultJsonProtocol._
  private val simpleCachedNameFormat: JsonFormat[CachedName.T] = JsonExtra.deriveFormatFrom[String].apply[T](identity, x => x.asInstanceOf[T])
  implicit val hashFormat: JsonFormat[CachedName.T] = DeduplicationCache.cachedFormat(simpleCachedNameFormat)
}

sealed trait TreeNode extends Product {
  def name: String
  def isBranch: Boolean
}
case class TreeLeaf(
    name:    CachedName.T,
    content: Vector[Hash.T]
) extends TreeNode {
  override def isBranch: Boolean = false
}
case class TreeBranch(
    name:    CachedName.T,
    subtree: Hash.T
) extends TreeNode {
  override def isBranch: Boolean = true
}
case class TreeLink(
    name:       CachedName.T,
    linktarget: String
) extends TreeNode {
  override def isBranch: Boolean = false
}
case class TreeBlob(
    nodes: Vector[TreeNode]
)
object TreeBlob {
  import spray.json.DefaultJsonProtocol._
  implicit val leafFormat = jsonFormat2(TreeLeaf.apply _)
  implicit val branchFormat = jsonFormat2(TreeBranch.apply _)
  implicit val linkFormat = jsonFormat2(TreeLink.apply _)
  implicit val nodeFormat = new JsonFormat[TreeNode] {
    override def read(json: JsValue): TreeNode = json.asJsObject.fields("type") match {
      case JsString("dir")     => json.convertTo[TreeBranch]
      case JsString("file")    => json.convertTo[TreeLeaf]
      case JsString("symlink") => json.convertTo[TreeLink]
    }

    override def write(obj: TreeNode): JsValue = ???
  }
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
    blobs: Vector[PackBlob]
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

  trait Index {
    def lookup(blobId: Hash.T): (Hash.T, PackBlob)

    def find(blobId: Hash.T): (Int, Int)
    def allKeys: IndexedSeq[Hash.T]
  }

  def writeIndexFile(index: Map[String, (Hash.T, PackBlob)]): Unit = {
    val keys = index.keys.toVector.sorted
    val fos = new BufferedOutputStream(new FileOutputStream(indexFile), 1000000)

    def uint32le(value: Int): Unit = {
      fos.write(value)
      fos.write(value >> 8)
      fos.write(value >> 16)
      fos.write(value >> 24)
    }
    def hash(hash: String): Unit = {
      require(hash.length == 64)
      var i = 0
      while (i < 32) {
        fos.write(Integer.parseInt(hash, i * 2, i * 2 + 2, 16))
        i += 1
      }
    }

    keys.foreach { blobId =>
      val (packId, packBlob) = index(blobId)
      hash(blobId)
      val isTree = if (packBlob.isTree) 1 else 0
      require(packBlob.offset <= Int.MaxValue)
      hash(packId)
      uint32le(packBlob.offset.toInt | (isTree << 31))
      uint32le(packBlob.length)
    }
    fos.close()
  }

  lazy val index2: Future[Index] = Future {
    val HeaderSize = 0
    val EntrySize = 72 /* 2 * 32 + 4 + 4 */

    val file = FileChannel.open(indexFile.toPath)
    val indexBuffer = file.map(MapMode.READ_ONLY, 0, file.size()).order(ByteOrder.LITTLE_ENDIAN)
    val intBuffer = indexBuffer.duplicate().order(ByteOrder.LITTLE_ENDIAN).asIntBuffer()
    val numEntries = ((indexFile.length() - HeaderSize) / EntrySize).toInt
    println(s"Found $numEntries")

    val longBEBuffer = indexBuffer.duplicate().order(ByteOrder.BIG_ENDIAN).asLongBuffer()
    def keyAt(idx: Int): Long = longBEBuffer.get((idx * EntrySize) >> 3) >>> 4 // only use 60 bits

    new Index {
      override def allKeys: IndexedSeq[Hash.T] = new IndexedSeq[Hash.T] {
        override def length: Int = numEntries
        override def apply(i: Int): T = {
          val targetPackHashBytes = {
            val dst = new Array[Byte](32)
            indexBuffer.asReadOnlyBuffer.position(i * EntrySize).get(dst)
            dst
          }
          targetPackHashBytes.map("%02x".format(_)).mkString.asInstanceOf[Hash.T]
        }
      }

      override def lookup(blobId: Hash.T): (Hash.T, PackBlob) = {
        def entryAt(idx: Int, step: Int): (Hash.T, PackBlob) = {
          val targetBaseOffset = idx * EntrySize
          val targetPackHashBytes = {
            val dst = new Array[Byte](32)
            indexBuffer.asReadOnlyBuffer.position(targetBaseOffset + 32).get(dst)
            dst
          }
          val targetPackHash = targetPackHashBytes.map("%02x".format(_)).mkString.asInstanceOf[Hash.T]
          val offsetAndType = intBuffer.get((targetBaseOffset + 64) >> 2)
          val length = intBuffer.get((targetBaseOffset + 68) >> 2)
          val offset = offsetAndType & 0x7fffffff
          val tpe = if ((offsetAndType & 0x80000000) != 0) BlobType.Tree else BlobType.Data

          val res = (targetPackHash, PackBlob(blobId, tpe, offset, length))
          //println(f"[${blobId.take(15)}] step: $step%2d   at: $idx%8d found: $res")
          res
        }

        val (idx, step) = find(blobId)
        entryAt(idx, step)
      }

      def find(blobId: Hash.T): (Int, Int) = {
        val targetKey = java.lang.Long.parseLong(blobId.take(15), 16)

        def interpolate(left: Int, right: Int): Int = {
          val leftKey = keyAt(left)
          val rightKey = keyAt(right)
          left + ((targetKey - leftKey).toFloat * (right - left) / (rightKey - leftKey)).toInt
        }
        // https://www.sciencedirect.com/science/article/pii/S221509862100046X
        // hashes should be uniformly distributed, so interpolation search is fastest
        @tailrec def rec(leftIndex: Int, rightIndex: Int, guess: Int, step: Int): (Int, Int) = {
          val guessKey = keyAt(guess)
          //println(f"[$targetKey%015x] step: $step%2d left: $leftIndex%8d right: $rightIndex%8d range: ${rightIndex - leftIndex}%8d guess: $guess%8d ($guessKey%015x)")
          if (guessKey == targetKey) (guess, step)
          else if (leftIndex == rightIndex) throw new IllegalStateException
          else { // interpolation step
            val newLeft = if (targetKey < guessKey) leftIndex else guess + 1
            val newRight = if (targetKey < guessKey) guess - 1 else rightIndex

            rec(newLeft, newRight, interpolate(newLeft, newRight), step + 1)
          }
        }

        //val firstGuess = numEntries / 2
        val firstGuess = interpolate(0, numEntries - 1)
        rec(0, numEntries - 1, firstGuess, 1)
      }
    }
  }

  //index.foreach { _ =>
  /*index2.foreach { i2 =>
    /*import scala.collection.JavaConverters._
    val ordered = new util.ArrayList[Hash.T]
    ordered.addAll(i2.allKeys.asJava)
    Collections.shuffle(ordered)
    val hashes = ordered.asScala.take(1000)*/

    val steps = i2.allKeys.map { h =>
      i2.find(h.asInstanceOf[Hash.T])._2
    }
    val num = steps.size
    val maxSteps = steps.max
    val totalSteps = steps.sum
    println(f"Total steps: $totalSteps%7d avg: ${totalSteps.toFloat / num}%5.2f maxSteps: $maxSteps%2d")
  }*/
  //}

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

