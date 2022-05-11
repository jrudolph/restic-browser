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
  //def children: Future[Seq[MergedTreeNode]]
}
object MergedTreeNode {
  /*def forRoots(repo: ResticRepository, roots: Seq[Hash]): MergedBranch = {
    import repo.system.dispatcher
    def at(path: Seq[String], roots: Seq[Hash]): MergedBranch = new MergedBranch {
      override def children: Future[Seq[MergedTreeNode]] =
        Future.traverse(roots)(repo.loadTree(_))
          .map()

      override def name: String = path.lastOption.getOrElse("/")
    }

    at(Vector.empty, roots)
  }*/
  //private case class

  def lookupBranch(path: Seq[String], repo: ResticRepository, roots: Seq[Hash]): Future[Seq[MergedTreeNode]] = {
    import repo.system.dispatcher
    def rec(path: List[String], roots: Seq[Hash]): Future[Seq[MergedTreeNode]] = path match {
      case Nil =>
        roots.foreach(println)
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
        roots.foreach(println)
        Future.traverse(roots)(repo.loadTree(_))
          .map { blobs =>
            blobs.flatMap(_.nodes.collect { case b: TreeBranch if b.name == next => b.subtree })
          }.flatMap(newRoots => rec(rem, newRoots))
    }

    println(s"at $path")
    rec(path.toList, roots)
  }
}