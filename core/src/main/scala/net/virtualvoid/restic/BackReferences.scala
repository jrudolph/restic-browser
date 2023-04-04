package net.virtualvoid.restic

import org.apache.pekko.http.caching.LfuCache
import org.apache.pekko.http.caching.scaladsl.{ CachingSettings, LfuCacheSettings }
import org.apache.pekko.stream.scaladsl.{ Sink, Source }

import java.io.File
import java.util.concurrent.atomic.AtomicLong
import scala.concurrent.Future

sealed trait BackReference
case class SnapshotReference(id: Hash) extends BackReference
case class TreeReference(treeBlobId: Hash, idx: Int) extends BackReference

sealed trait ChainNode
case class TreeChainNode(tree: TreeNode) extends ChainNode
case class SnapshotNode(id: Hash, node: Snapshot) extends ChainNode
case class Chain(id: Hash, chain: List[ChainNode])

trait BackReferences {
  def backReferencesFor(hash: Hash): Future[Seq[BackReference]]
  def chainsFor(id: Hash): Future[Seq[Chain]]
}
object BackReferences {
  private implicit object BackReferenceSerializer extends Serializer[BackReference] {
    override def entrySize: Int = 40

    override def write(id: Hash, t: BackReference, writer: Writer): Unit = t match {
      case t: TreeReference =>
        writer.uint32le(0)
        writer.uint32le(t.idx)
        writer.hash(t.treeBlobId)
      case SnapshotReference(hash) =>
        writer.uint32le(1)
        writer.uint32le(0)
        writer.hash(hash)
    }
    override def read(id: Hash, reader: Reader): BackReference =
      reader.uint32le() match {
        case 0 =>
          val idx = reader.uint32le()
          val treeBlob = reader.hash()
          TreeReference(treeBlob, idx)
        case 1 =>
          reader.uint32le() // unused
          SnapshotReference(reader.hash())
      }
  }

  def apply(reader: ResticRepository): BackReferences =
    new BackReferences {
      import reader.system
      import reader.system.dispatcher

      def treeBackReferences(packEntry: PackEntry): Future[Vector[(Hash, TreeReference)]] =
        reader.loadTree(packEntry).map { treeBlob =>
          treeBlob.nodes.zipWithIndex.flatMap {
            case (b: TreeBranch, idx) => Vector(b.subtree -> TreeReference(packEntry.id, idx))
            case (l: TreeLeaf, idx)   => l.content.map(c => c -> TreeReference(packEntry.id, idx))
            case _                    => Nil
          }
        }
      lazy val snapshotBackRefs: Future[Map[Hash, Seq[SnapshotReference]]] =
        reader.allSnapshots()
          .map(s => s._2.tree -> SnapshotReference(s._1))
          .runWith(Sink.seq[(Hash, SnapshotReference)])
          .map(_.groupBy(_._1).view.mapValues(_.map(_._2)).toMap)

      lazy val backrefIndex: Future[Index[BackReference]] = {
        def allBackReferences(indices: Seq[String]): Source[(Hash, BackReference), Any] =
          Source(indices)
            .mapAsync(1)(idx => reader.loadIndex(Hash(idx)))
            .mapConcat { i =>
              i.packs.flatMap { p =>
                p.blobs.filter(_.isTree).map { pb =>
                  PackEntry(p.id, pb.id, BlobType.Tree, pb.offset, pb.length, pb.uncompressed_length)
                }
              }
            }
            .mapAsync(1024)(treeBackReferences)
            .async
            .mapConcat(identity)
        reader.cachedIndexFromBaseElements("backrefs", reader.allIndexFileNames, allBackReferences)
      }

      def backReferencesFor(hash: Hash): Future[Seq[BackReference]] =
        for {
          idx <- backrefIndex
          snaps <- snapshotBackRefs
        } yield idx.lookupAll(hash) ++ snaps.getOrElse(hash, Seq.empty)

      lazy val snapshots: Future[Map[Hash, Snapshot]] =
        reader.allSnapshots().runWith(Sink.seq).map(_.toMap)

      def lookupRef(t: TreeReference): Future[TreeNode] = reader.loadTree(t.treeBlobId).map(_.nodes(t.idx))
      def lookupBackRef(r: BackReference): Future[ChainNode] =
        snapshots.flatMap { snaps =>
          r match {
            case t: TreeReference     => lookupRef(t).map(TreeChainNode)
            case s: SnapshotReference => Future.successful(SnapshotNode(s.id, snaps(s.id)))
          }
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

      def findBackChainsInternal(id: Hash): Future[Seq[Chain]] =
        backReferencesFor(id).flatMap { parents =>
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
      def chainsFor(id: Hash): Future[Seq[Chain]] = findBackChains(id)
    }
}
