package net.virtualvoid.restic

import scala.concurrent.Future

sealed trait MergedTreeNode {
  def name: String
  def isBranch: Boolean
  def isLeaf: Boolean = !isBranch
  def revisions: Seq[(TreeNode, Snapshot)]
}
sealed trait MergedLeaf extends MergedTreeNode {
  override def isBranch: Boolean = false
}
sealed trait MergedBranch extends MergedTreeNode {
  override def isBranch: Boolean = true
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
                case (n, els) if els.head._1.isBranch => new MergedBranch {
                  override def name: String = n
                  override def revisions: Seq[(TreeNode, Snapshot)] = els
                }
                case (n, els) => new MergedLeaf {
                  override def name: String = n
                  override def revisions: Seq[(TreeNode, Snapshot)] = els
                }
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