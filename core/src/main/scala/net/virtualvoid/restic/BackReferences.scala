package net.virtualvoid.restic

import akka.stream.scaladsl.{ Sink, Source }

import java.io.File
import scala.concurrent.Future

sealed trait BackReference
case class SnapshotReference(id: Hash) extends BackReference
case class TreeReference(treeBlobId: Hash, idx: Int) extends BackReference

trait BackReferences {
  def backReferencesFor(hash: Hash): Future[Seq[BackReference]]
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

      def treeBackReferences(tree: Hash): Future[Vector[(Hash, TreeReference)]] =
        reader.loadTree(tree).map { treeBlob =>
          treeBlob.nodes.zipWithIndex.flatMap {
            case (b: TreeBranch, idx) => Vector(b.subtree -> TreeReference(tree, idx))
            case (l: TreeLeaf, idx)   => l.content.map(c => c -> TreeReference(tree, idx))
            case _                    => Nil
          }
        }
      def snapshotBackRefs(): Future[Seq[(Hash, SnapshotReference)]] =
        reader.allSnapshots()
          .map(s => s._2.tree -> SnapshotReference(s._1))
          .runWith(Sink.seq[(Hash, SnapshotReference)])

      val backrefIndexFile = new File(reader.localCacheDir, "backrefs.idx")

      lazy val backrefIndex: Future[Index[BackReference]] =
        reader.blob2packIndex.flatMap { i =>
          if (!backrefIndexFile.exists()) {
            val treeSource: Source[(Hash, BackReference), Any] =
              Source(i.allKeys)
                .filter(i.lookup(_).isTree)
                .mapAsync(1024)(treeBackReferences)
                .async
                .mapConcat(identity)
                .concat(Source.futureSource(snapshotBackRefs().map(Source(_))))

            Index.createIndex(backrefIndexFile, treeSource)
          } else Future.successful(Index.load[BackReference](backrefIndexFile))
        }

      def backReferencesFor(hash: Hash): Future[Seq[BackReference]] =
        backrefIndex.map(_.lookupAll(hash))
    }
}
