package net.virtualvoid.restic

import akka.NotUsed
import akka.actor.ActorSystem
import akka.http.caching.LfuCache
import akka.http.caching.scaladsl.{ CachingSettings, LfuCacheSettings }
import akka.stream.scaladsl.{ Sink, Source }
import akka.util.ByteString
import com.github.benmanes.caffeine.cache.Caffeine
import net.virtualvoid.restic.ResticReaderMain.reader

import java.io.{ File, FileOutputStream }
import java.time.format.DateTimeFormatter
import java.time.{ LocalDateTime, LocalTime, ZoneOffset }
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.atomic.AtomicLong
import scala.annotation.tailrec
import scala.concurrent.{ Await, Future }
import scala.concurrent.duration._
import scala.util.{ Failure, Success }

object ResticReaderMain extends App {
  implicit val system = ActorSystem()
  import system.dispatcher

  val backingDir = new File("/tmp/restic-repo")
  val cacheBaseDir = new File("../restic-cache")

  val reader = ResticRepository.open(ResticSettings()).get

  import reader.loadTree

  //val indexFiles = reader.allFiles(indexDir)
  //println(indexFiles.size)
  //reader.readJson[TreeBlob](new File(dataFile), 0, 419).onComplete(println)

  /*def loadIndex(): Future[Map[Hash, PackEntry]] =
    Future.traverse(reader.allFiles(reader.indexDir))(f => ).map { indices =>
      val allSuperseded = indices.flatMap(_._2.realSupersedes).toSet
      val withoutSuperseded = indices.toMap -- allSuperseded
      println(s"${allSuperseded.size} indices were superseded, ignored ${indices.size - withoutSuperseded.size} superseded index files")
      val allEntries = withoutSuperseded.flatMap(_._2.packs.flatMap(p => p.blobs.map(b => b.id -> PackEntry(p.id, b.id, b.`type`, b.offset, b.length))))
      allEntries
    }*/

  //lazy val index = bench("loadIndex")(loadIndex())
  /*index.onComplete {
    case Success(res) =>
      //System.gc()
      //System.gc()
      println(s"Loaded ${res.size} index entries")

      if (!indexFile.exists())
        benchSync("writeIndex")(writeIndexFile(res))
    //Thread.sleep(100000)
  }*/

  /*lazy val allTrees: Future[Seq[Hash]] =
    index.map { i =>
      benchSync("allTrees") {
        i.values.filter(_._2.isTree).map(_._2.id).toVector
      }
    }*/

  def walkTreeNodes(blob: TreeBlob): Source[TreeNode, NotUsed] = {
    val subtrees = blob.nodes.collect { case b: TreeBranch => b }
    Source(blob.nodes) ++
      Source(subtrees)
      .mapAsync(1024)(x => loadTree(x.subtree))
      .flatMapConcat(walkTreeNodes)
  }

  sealed trait Reference
  sealed trait BackReference extends Reference
  case class SnapshotReference(id: Hash) extends BackReference
  case class TreeReference(treeBlobId: Hash, idx: Int, node: TreeNode) extends BackReference
  case class BlobReference(id: Hash, referenceChain: Seq[Reference]) {
    def chainString: String = {
      def refString(ref: Reference): String = ref match {
        case SnapshotReference(id)     => s"snap:${id.toString.take(10)}"
        case TreeReference(_, _, node) => node.name
      }

      referenceChain.reverse.map(refString).mkString("/")
    }
  }
  def reverseReferences(treeId: Hash, chain: List[Reference]): Future[Vector[BlobReference]] =
    loadTree(treeId).flatMap { blob =>
      val subdirs = blob.nodes.collect { case b: TreeBranch => b }

      val leaves =
        blob.nodes.zipWithIndex
          .flatMap {
            case (node: TreeLeaf, idx) =>
              node.content.map(c => BlobReference(c, TreeReference(treeId, idx, node) :: chain))
            case _ => Vector.empty
          }

      Future.traverse(subdirs)(b => reverseReferences(b.subtree, TreeReference(treeId, 0, b) :: chain)).map(_.flatten ++ leaves)
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

  def snapshotStats: Unit =
    reader.allSnapshots()
      .runWith(Sink.seq)
      .foreach { snaps =>
        snaps.toVector.sortBy(_._2.time).foreach { s =>
          println(s._1, s._2.time, s._2.hostname, s._2.paths, s._2.tags)
        }
        println(s"Found ${snaps.size} snapshots")
      }

  def treeBackReferences(tree: Hash): Future[Vector[(Hash, TreeReference)]] =
    loadTree(tree).map { treeBlob =>
      treeBlob.nodes.zipWithIndex.flatMap {
        case (b: TreeBranch, idx) => Vector(b.subtree -> TreeReference(tree, idx, b))
        case (l: TreeLeaf, idx)   => l.content.map(c => c -> TreeReference(tree, idx, l))
        case _                    => Nil
      }
    }
  def snapshotBackRefs(): Future[Seq[(Hash, SnapshotReference)]] =
    reader.allSnapshots()
      .map(s => s._2.tree -> SnapshotReference(s._1))
      .runWith(Sink.seq[(Hash, SnapshotReference)])

  val backrefIndexFile = new File(reader.localCacheDir, "backrefs.idx")
  implicit object BackReferenceSerializer extends Serializer[BackReference] {
    override def entrySize: Int = 40

    override def write(id: Hash, t: BackReference, writer: Writer): Unit = t match {
      case t: TreeReference =>
        writer.uint32le(0)
        writer.hash(t.treeBlobId)
        writer.uint32le(t.idx)
      case SnapshotReference(hash) =>
        writer.uint32le(1)
        writer.hash(hash)
        writer.uint32le(0)
    }
    override def read(id: Hash, reader: Reader): BackReference =
      reader.uint32le() match {
        case 0 =>
          val treeBlob = reader.hash()
          val idx = reader.uint32le()
          TreeReference(treeBlob, idx, null)
        case 1 =>
          SnapshotReference(reader.hash())
      }
  }

  reader.packIndex.flatMap { i =>
    if (!backrefIndexFile.exists()) {
      /*println("Loading all trees")
      val allTrees =
        i.allKeys
          .map(i.lookup)
          .filter(_.isTree).toVector
      println(s"Loaded ${allTrees.size} trees")*/
      val treeSource: Source[(Hash, BackReference), Any] =
        Source(i.allKeys)
          .filter(i.lookup(_).isTree)
          .mapAsync(1024)(treeBackReferences)
          .async
          .mapConcat(identity)
          .concat(Source.futureSource(snapshotBackRefs().map(Source(_))))

      Index.createIndex(backrefIndexFile, treeSource)
    } else Future.successful(Index.load[BackReference](backrefIndexFile))

    /*Future.traverse(allTrees) {
      case (h, _) => treeBackReferences(h)
    }.map(_.flatten)*/
  }.onComplete {
    case Success(idx) =>
      /*idx.lookupAll(Hash("f5c40b8948370710e31763d4176aa3ec272390ed7140347081b5010fd27311d2"))
        .foreach(println)*/
      sealed trait ChainNode
      case class TreeChainNode(tree: TreeNode) extends ChainNode
      case class SnapshotNode(id: Hash, node: Snapshot) extends ChainNode
      case class Chain(id: Hash, chain: List[ChainNode])

      val snaps: Map[Hash, Snapshot] =
        Await.result(
          reader.allSnapshots().runWith(Sink.seq),
          10.seconds).toMap

      def lookupRef(t: TreeReference): Future[TreeNode] = loadTree(t.treeBlobId).map(_.nodes(t.idx))
      def lookupBackRef(r: BackReference): Future[ChainNode] =
        r match {
          case t: TreeReference     => lookupRef(t).map(TreeChainNode)
          case s: SnapshotReference => Future.successful(SnapshotNode(s.id, snaps(s.id)))
        }

      def memoizedStrict[T, U](f: T => U): T => U = {
        val cache =
          Caffeine.newBuilder()
            .maximumSize(50000)
            .build[T, U]()

        t => cache.get(t, t => f(t))
      }

      def memoized[T, U](f: T => Future[U]): T => Future[U] = {
        val cache = LfuCache[T, U](CachingSettings(system).withLfuCacheSettings(LfuCacheSettings(system).withMaxCapacity(50000).withInitialCapacity(10000)))

        val hits = new AtomicLong()
        val misses = new AtomicLong()

        t => {
          def report(): Unit = {
            val hs = hits.get()
            val ms = misses.get()
            println(f"Hits: $hs%10d Misses: $ms%10d hit rate: ${hs.toFloat / (hs + ms)}%5.2f")
          }

          if (cache.get(t).isDefined) {
            if (hits.incrementAndGet() % 10000 == 0) report()
          } else if (misses.incrementAndGet() % 10000 == 0) report()

          cache(t, () => f(t))
        }
      }

      def findBackChainsInternal(id: Hash): Future[Seq[Chain]] = {
        val parents = idx.lookupAll(id)
        if (parents.isEmpty) Future.successful(Vector(Chain(id, Nil)))
        else
          Source(parents)
            .mapAsync(16)(ref => lookupBackRef(ref).flatMap {
              case n: TreeChainNode =>
                findBackChains(ref.asInstanceOf[TreeReference].treeBlobId).map(_.map(x => x.copy(chain = n :: x.chain)))
              case s: SnapshotNode =>
                Future.successful(Vector(Chain(id, s :: Nil)))
            })
            .mapConcat(identity)
            .runWith(Sink.seq)
      }
      lazy val findBackChains = memoized(findBackChainsInternal)

      def chainString(s: Seq[ChainNode]): String =
        "/" + s.map {
          case TreeChainNode(t: TreeBranch) => t.name
          case TreeChainNode(l: TreeLeaf)   => s"${l.name}(${l.size})"
          case SnapshotNode(id, snap) =>
            s"$id(${snap.time})"
        }.mkString("/")

      def findChainsForHash(target: Hash): Unit = {
        val chains = findBackChains(target)
        chains
          .onComplete {
            case Success(chs) =>
              chs.map(s => chainString(s.chain.reverse)).groupBy(identity).view.mapValues(_.size).toVector.sortBy(-_._2).foreach(println)
          }
      }
      //val target = "c72413bcff23b206"
      //val target = "0004da4650044bcd"
      //findChainsForHash(Hash(target))

      reader.packIndex.foreach { packIdx =>
        lazy val singleChain = memoized(singleChainInternal)
        def singleChainInternal(id: Hash): Future[Option[Chain]] = {
          val parents = idx.lookupAll(id)
          parents.size match {
            case 0 => Future.successful(Some(Chain(id, Nil)))
            case 1 =>
              val ref = parents.head
              lookupBackRef(ref).flatMap {
                case n: TreeChainNode =>
                  singleChain(ref.asInstanceOf[TreeReference].treeBlobId).map(_.map(x => x.copy(chain = n :: x.chain)))
                case s: SnapshotNode =>
                  Future.successful(Some(Chain(id, s :: Nil)))
              }
            case _ => Future.successful(None)
          }
        }

        val fos = new FileOutputStream("report.log")
        def originalChains: Source[(Hash, Chain), Any] =
          Source(packIdx.allKeys)
            .filter(id => !packIdx.lookup(id).isTree)
            //.take(100000)
            .via(new Stats("blobs", "element", _ => 1))
            .mapAsync(1024)(x => singleChain(x).map(_.map(x -> _)))
            .mapConcat(identity)
            .filter(_._2.chain.nonEmpty)

        val onlyReferencedOnce: Hash => Boolean =
          memoizedStrict[Hash, Boolean](h => idx.lookupAll(h).size == 1)

        def collectChains(id: Hash, chain: List[ChainNode]): Future[Seq[(Hash, Chain)]] =
          if (onlyReferencedOnce(id))
            reader.loadTree(id).flatMap { tree =>
              val blobs: Seq[(Hash, Chain)] =
                tree.nodes.flatMap {
                  case l: TreeLeaf =>
                    l.content
                      .filter(onlyReferencedOnce)
                      .map(b => b -> Chain(b, TreeChainNode(l) :: chain))
                  case _ => Vector.empty
                }

              Future.sequence {
                val trees =
                  tree.nodes.collect {
                    case b: TreeBranch =>
                      collectChains(b.subtree, TreeChainNode(b) :: chain)
                  }
                trees
              }.map(_.flatten ++ blobs)
            }
          else Future.successful(Vector.empty)

        def fastChains: Source[(Hash, Chain), Any] =
          reader.allSnapshots()
            .map { case (h, s) => SnapshotNode(h, s) }
            .mapAsync(1024)(snap => collectChains(snap.node.tree, snap :: Nil))
            .mapConcat(identity)

        fastChains
          .runForeach {
            case (hash, chain) =>
              def size(id: Hash): Long = packIdx.lookup(id).length
              def stack(chain: Chain): String =
                chain.chain.reverse
                  .map {
                    case t: TreeChainNode => t.tree.name
                    case s: SnapshotNode =>
                      val dt = s.node.time
                      s"${dt.getYear};${dt.getMonth};${s.id.toString.take(16)}(${s.node.time})"
                  }
                  .mkString(";") + s";${hash.toString.take(16)}"
              fos.write(f"${stack(chain)} ${size(hash)}%10d\n".getBytes)
          }
          .onComplete { res =>
            println(s"Res: $res")
            fos.close()
          }
        /*.runWith(Sink.seq)
          .onComplete {
            case Success(chains) =>
              /*def size(chain: Seq[TreeNode]): Long = chain.headOption match {
                case Some(l: TreeLeaf) => l.content
                case _                 => 0
              }*/
              def size(id: Hash): Long = packIdx.lookup(id).length
              chains
                .map(x => x -> size(x._1))
                .sortBy(-_._2)
                .take(100)
                .foreach {
                  case (chain, size) => println(f"$size%12d ${chain._1} ${chainString(chain._2.chain.reverse)}")
                }

          }*/
      }

    /*
      println(s"Found ${refs.size} backrefs")
      println("sorting...")
      val sorted = benchSync("sorting")(refs.sortBy(_._1))
      println("... done!")
      println("grouping")
      val grouped = benchSync("grouping")(refs.groupBy(_._1))*/
    //println("done")
    case Failure(ex) =>
      ex.printStackTrace()
  }

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

  /*reverseReferences(Hash("a5b1b14e1b87f8e4804604065179d8edfd0815752b386b18de8660d099856d70"), Nil)
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
      case Failure(ex) =>
        ex.printStackTrace()
    }*/

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

