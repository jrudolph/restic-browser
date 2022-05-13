package net.virtualvoid.restic

import java.time.{ Duration, Instant, LocalDate, Period, ZonedDateTime }
import scala.concurrent.Future
import MergedTreeNode.convertToInterval

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
  def lookupNode(path: Seq[String], repo: ResticRepository, snapshots: Seq[Snapshot]): Future[(MergedTreeNode, Seq[MergedTreeNode])] = {
    import repo.system.dispatcher
    def rec(at: MergedTreeNode, path: List[String]): Future[(MergedTreeNode, Seq[MergedTreeNode])] =
      lookupChildren(at, repo).flatMap { children =>
        path match {
          case Nil          => Future.successful(at -> children)
          case head :: tail => rec(children.find(_.name == head).get, tail)
        }
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