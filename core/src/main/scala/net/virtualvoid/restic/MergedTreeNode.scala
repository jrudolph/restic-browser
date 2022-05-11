package net.virtualvoid.restic

import scala.concurrent.Future

case class MergedTreeNode(
    name:      String,
    revisions: Seq[(TreeNode, Snapshot)]
)
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