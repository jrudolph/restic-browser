package net.virtualvoid.restic

import java.time.{ Duration, Instant, LocalDate, Period, ZonedDateTime }
import scala.concurrent.Future
import MergedTreeNode.convertToInterval

sealed trait Child
sealed trait DirectoryChild {
  def treeId: Hash
}
case class TreeDirectoryChild(treeBlobId: Hash, node: TreeBranch) extends DirectoryChild {
  override def treeId: Hash = node.subtree
}
case class FileChild(treeBlobId: Hash, node: TreeLeaf) extends Child
case class SnapshotRoot(treeId: Hash) extends DirectoryChild

case class PathRevision(snapshots: Seq[Snapshot], treeBlobId: Hash, node: TreeNode) {
  def firstSeen: ZonedDateTime = snapshots.map(_.time).min
  def firstSeenPeriod: String = convertToInterval(firstSeen)
  def lastSeen: ZonedDateTime = snapshots.map(_.time).max
  def lastSeenPeriod: String = convertToInterval(lastSeen)
}

case class MergedTreeNode(
    name:            String,
    nestedRevisions: Seq[PathRevision]
) {
  def revisions: Seq[(TreeNode, Snapshot)] = nestedRevisions.flatMap { case p: PathRevision => p.snapshots.map(p.node -> _) }
  def firstSeen: ZonedDateTime = revisions.map(_._2.time).min
  def firstSeenPeriod: String = convertToInterval(firstSeen)
  def lastSeen: ZonedDateTime = revisions.map(_._2.time).max
  def lastSeenPeriod: String = convertToInterval(lastSeen)

  def lastNewVersionSeen: ZonedDateTime =
    revisions
      .groupBy { case (b: TreeBranch, _) => b.subtree; case (l: TreeLeaf, _) => l.content; case (l: TreeLink, _) => l.linktarget }
      .values
      .map(_.map(_._2.time).min).max
  def lastNewVersionSeenPeriod: String = convertToInterval(lastNewVersionSeen)

  def isOlderThanDays(days: Int): Boolean = Duration.between(lastSeen.toInstant, Instant.now()).toDays > days
}
object MergedTreeNode {
  def lookupBranch(path: Seq[String], repo: ResticRepository, snapshots: Seq[Snapshot]): Future[Seq[MergedTreeNode]] = {
    import repo.system.dispatcher
    def rec(path: List[String], roots: Seq[(Hash, Seq[Snapshot])]): Future[Seq[MergedTreeNode]] = path match {
      case Nil =>
        Future.traverse(roots) { case (tree, snap) => repo.loadTree(tree).map(b => (tree, b, snap)) }
          .map { blobs =>
            blobs.flatMap {
              case (blobId, blob, snaps) =>
                blob.nodes.map(n => PathRevision(snaps, blobId, n))
            }
              .groupBy(_.node.name)
              .toVector
              .map {
                case (n, els) => MergedTreeNode(n, els)
              }
          }
      case next :: rem =>
        Future.traverse(roots) { case (tree, snap) => repo.loadTree(tree).map(_ -> snap) }
          .map { blobs =>
            blobs.flatMap {
              case (blob, snap) =>
                blob.nodes.collect { case b: TreeBranch if b.name == next => b.subtree -> snap }
            }.groupBy(_._1).view.mapValues(_.flatMap(_._2)).toVector
          }
          .flatMap(newRoots => rec(rem, newRoots))
    }

    rec(path.toList, snapshots.map(s => s.tree -> Seq(s)))
  }

  def lookupBranch2(path: Seq[String], repo: ResticRepository, snapshots: Seq[Snapshot]): Future[(MergedTreeNode, Seq[MergedTreeNode])] = {
    import repo.system.dispatcher
    def rec(at: MergedTreeNode, path: List[String]): Future[(MergedTreeNode, Seq[MergedTreeNode])] = path match {
      case last :: Nil =>
        lookupChildren(at, repo).map { children =>
          children.filter(_.name == last).head
        }.flatMap { merged =>
          lookupChildren(merged, repo).map(chs => merged -> chs)
        }
      case head :: tail =>
        lookupChildren(at, repo).map { children =>
          children.filter(_.name == head).head
        }.flatMap(rec(_, tail))
    }
    rec(
      MergedTreeNode("", snapshots.groupBy(_.tree).map { case (tree, snaps) => PathRevision(snaps, Hash("00") /* dummy */ , TreeBranch("".asInstanceOf[CachedName.T], tree)) }.toVector),
      path.toList
    )
  }
  def lookupChildren(node: MergedTreeNode, repo: ResticRepository): Future[Seq[MergedTreeNode]] = {
    import repo.system.dispatcher
    Future.traverse(node.nestedRevisions.collect { case PathRevision(snaps, _, b: TreeBranch) => b.subtree -> snaps }) {
      case (treeBlobId, snaps) =>
        repo.loadTree(treeBlobId).map(b => (treeBlobId, b, snaps))
    }.map { blobSnaps =>
      blobSnaps.flatMap {
        case (treeBlobId, blob, snaps) =>
          blob.nodes
            .map { node =>
              MergedTreeNode(node.name, Seq(PathRevision(snaps, treeBlobId, node)))
            }
      }
        .groupBy(_.name)
        .map {
          case (n, merged) => MergedTreeNode(
            n,
            merged.flatMap(_.nestedRevisions)
              .groupBy(_.node match { case b: TreeBranch => b.subtree; case l: TreeLeaf => l.content; case l: TreeLink => l.linktarget })
              .map {
                case (_, revs) =>
                  PathRevision(revs.flatMap(_.snapshots), revs.head.treeBlobId, revs.head.node)
              }
              .toVector
          )
        }
        .toVector
    }
  }

  def convertToInterval(dt: ZonedDateTime): String = {
    val p = Period.between(dt.toLocalDate, LocalDate.now())
    def str(short: String, what: Int): String =
      if (what > 0) f"$what%2d$short" else ""
    if (dt.toLocalDate == LocalDate.now()) "today"
    else if (dt.toLocalDate == LocalDate.now().minusDays(1)) "yesterday"
    else
      Seq(str("y", p.getYears), str("m", p.getMonths), str("d", p.getDays)).filter(_.nonEmpty).mkString(" ") + " ago"
  }
}