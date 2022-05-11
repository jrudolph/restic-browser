package net.virtualvoid.restic

import scala.concurrent.Future

sealed trait MergedTreeNode {
  def name: String
  def isBranch: Boolean
  def isLeaf: Boolean = !isBranch
}
sealed trait MergedLeaf extends MergedTreeNode {
  override def isBranch: Boolean = false
}
sealed trait MergedBranch extends MergedTreeNode {
  override def isBranch: Boolean = true
}
object MergedTreeNode {
  def lookupBranch(path: Seq[String], repo: ResticRepository, roots: Seq[Hash]): Future[Seq[MergedTreeNode]] = {
    import repo.system.dispatcher
    def rec(path: List[String], roots: Seq[Hash]): Future[Seq[MergedTreeNode]] = path match {
      case Nil =>
        Future.traverse(roots)(repo.loadTree(_))
          .map { blobs =>
            blobs.flatMap(_.nodes).groupBy(_.name).mapValues(_.head).values
              .map {
                case b: TreeBranch => new MergedBranch {
                  override def name: String = b.name
                }
                case n => new MergedLeaf {
                  override def name: String = n.name
                }
              }.toVector
          }
      case next :: rem =>
        Future.traverse(roots)(repo.loadTree(_))
          .map { blobs =>
            blobs.flatMap(_.nodes.collect { case b: TreeBranch if b.name == next => b.subtree })
          }.flatMap(newRoots => rec(rem, newRoots))
    }

    rec(path.toList, roots)
  }
}