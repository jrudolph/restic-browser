package net.virtualvoid.restic

import spray.json._

sealed trait Hash
object Hash {
  type T = String with Hash

  import spray.json.DefaultJsonProtocol._
  private val simpleHashFormat: JsonFormat[Hash.T] =
    // use truncated hashes for lesser memory usage
    JsonExtra.deriveFormatFrom[String].apply[T](identity, x => x /*.take(12)*/ .asInstanceOf[T])
  implicit val hashFormat: JsonFormat[Hash.T] = DeduplicationCache.cachedFormat(simpleHashFormat)
}

sealed trait BlobType
object BlobType {
  case object Data extends BlobType
  case object Tree extends BlobType

  implicit val blobTypeFormat = new JsonFormat[BlobType] {
    override def read(json: JsValue): BlobType = json match {
      case JsString("tree") => Tree
      case JsString("data") => Data
    }
    override def write(obj: BlobType): JsValue = ???
  }
}

sealed trait CachedName
object CachedName {
  type T = String with CachedName

  import spray.json.DefaultJsonProtocol._
  private val simpleCachedNameFormat: JsonFormat[CachedName.T] = JsonExtra.deriveFormatFrom[String].apply[T](identity, x => x.asInstanceOf[T])
  implicit val hashFormat: JsonFormat[CachedName.T] = DeduplicationCache.cachedFormat(simpleCachedNameFormat)
}

sealed trait TreeNode extends Product {
  def name: String
  def isBranch: Boolean
}
case class TreeLeaf(
    name:    CachedName.T,
    content: Vector[Hash.T]
) extends TreeNode {
  override def isBranch: Boolean = false
}
case class TreeBranch(
    name:    CachedName.T,
    subtree: Hash.T
) extends TreeNode {
  override def isBranch: Boolean = true
}
case class TreeLink(
    name:       CachedName.T,
    linktarget: String
) extends TreeNode {
  override def isBranch: Boolean = false
}
case class TreeBlob(
    nodes: Vector[TreeNode]
)
object TreeBlob {
  import spray.json.DefaultJsonProtocol._
  implicit val leafFormat = jsonFormat2(TreeLeaf.apply _)
  implicit val branchFormat = jsonFormat2(TreeBranch.apply _)
  implicit val linkFormat = jsonFormat2(TreeLink.apply _)
  implicit val nodeFormat = new JsonFormat[TreeNode] {
    override def read(json: JsValue): TreeNode = json.asJsObject.fields("type") match {
      case JsString("dir")     => json.convertTo[TreeBranch]
      case JsString("file")    => json.convertTo[TreeLeaf]
      case JsString("symlink") => json.convertTo[TreeLink]
    }

    override def write(obj: TreeNode): JsValue = ???
  }
  implicit val treeBlobFormat = jsonFormat1(TreeBlob.apply _)
}

case class PackBlob(
    id:     Hash.T,
    `type`: BlobType,
    offset: Long,
    length: Int
) {
  def isTree: Boolean = `type` == BlobType.Tree
  def isData: Boolean = `type` == BlobType.Data
}
case class PackIndex(
    id:    Hash.T,
    blobs: Vector[PackBlob]
)
case class IndexFile(
    packs: Seq[PackIndex]
)
object IndexFile {
  import spray.json.DefaultJsonProtocol._
  implicit val packBlobFormat = jsonFormat4(PackBlob.apply _)
  implicit val packIndexFormat = jsonFormat2(PackIndex.apply _)
  implicit val indexFileFormat = jsonFormat1(IndexFile.apply _)
}