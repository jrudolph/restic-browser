package net.virtualvoid.restic

import org.apache.pekko.stream.scaladsl.{ Sink, Source }

import scala.concurrent.Future

sealed trait BackReference
case class SnapshotReference(id: Hash) extends BackReference
case class TreeReference(treeBlobId: Hash, idx: Int) extends BackReference

sealed trait ChainNode
case class TreeChainNode(tree: TreeNode) extends ChainNode
case class SnapshotNode(id: Hash, node: Snapshot) extends ChainNode
case class Chain(chain: List[ChainNode])

trait BackReferences {
  def backReferencesFor(hash: Hash): Future[Seq[BackReference]]
  def chainsFor(id: Hash): Future[Seq[Chain]]

  def initializeIndex(): Future[Any]
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
        def allBackReferences(blobIndex: Index[PackEntry])(packs: Seq[String]): Source[(Hash, BackReference), Any] = {
          val packSet = packs.toSet
          Source.fromIterator(blobIndex.iterateAllValuesUnordered)
            .filter(e => e.isTree && packSet(e.packId.toString))
            .mapAsyncUnordered(16)(treeBackReferences)
            .async
            .mapConcat(identity)
        }
        reader.blob2packIndex.flatMap { pi =>
          reader.cachedIndexFromBaseElements("backrefs", reader.allPacks, allBackReferences(pi))
        }
      }

      def backReferencesFor(hash: Hash): Future[Seq[BackReference]] =
        for {
          idx <- backrefIndex
          snaps <- snapshotBackRefs
        } yield idx.lookupAll(hash) ++ snaps.getOrElse(hash, Seq.empty)

      override def initializeIndex(): Future[Any] = backrefIndex

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

      def findBackChainsInternal(id: Hash): Future[Seq[Chain]] =
        backReferencesFor(id).flatMap { parents =>
          if (parents.isEmpty) Future.successful(Vector(Chain(Nil)))
          else
            Source(parents)
              .mapAsync(16)(ref => lookupBackRef(ref).flatMap {
                case n: TreeChainNode =>
                  findBackChains(ref.asInstanceOf[TreeReference].treeBlobId).map(_.map(x => x.copy(chain = n :: x.chain)))
                case s: SnapshotNode =>
                  Future.successful(Vector(Chain(s :: Nil)))
              })
              .mapConcat(identity)
              .runWith(Sink.seq)
        }
      lazy val findBackChains = Utils.memoized(findBackChainsInternal)
      def chainsFor(id: Hash): Future[Seq[Chain]] = findBackChains(id)
    }
}
