package net.virtualvoid.restic

import java.time.{ Duration, Instant, LocalDate, Period, ZonedDateTime }
import scala.concurrent.Future

case class MergedTreeNode(
    name:      String,
    revisions: Seq[(TreeNode, Snapshot)]
) {
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

  private def convertToInterval(dt: ZonedDateTime): String = {
    val p = Period.between(dt.toLocalDate, LocalDate.now())
    def str(short: String, what: Int): String =
      if (what > 0) f"$what%2d$short" else ""
    if (dt.toLocalDate == LocalDate.now()) "today"
    else if (dt.toLocalDate == LocalDate.now().minusDays(1)) "yesterday"
    else
      Seq(str("y", p.getYears), str("m", p.getMonths), str("d", p.getDays)).filter(_.nonEmpty).mkString(" ") + " ago"
  }
}
object MergedTreeNode {
  def lookupBranch(path: Seq[String], repo: ResticRepository, snapshots: Seq[Snapshot]): Future[Seq[MergedTreeNode]] = {
    import repo.system.dispatcher
    def rec(path: List[String], roots: Seq[(Hash, Snapshot)]): Future[Seq[MergedTreeNode]] = path match {
      case Nil =>
        Future.traverse(roots) { case (tree, snap) => repo.loadTree(tree).map(_ -> snap) }
          .map { blobs =>
            blobs.flatMap {
              case (blob, snap) =>
                blob.nodes.map(_ -> snap)
            }
              .groupBy(_._1.name)
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
            }
          }
          .flatMap(newRoots => rec(rem, newRoots))
    }

    rec(path.toList, snapshots.map(s => s.tree -> s))
  }
}