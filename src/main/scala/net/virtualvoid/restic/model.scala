package net.virtualvoid.restic

import spray.json._

import java.nio.ByteBuffer
import java.util
import scala.annotation.tailrec

final class Hash private (val bytes: Array[Byte]) {
  override lazy val hashCode: Int =
    ((bytes(0) & 0xff) << 24) |
      ((bytes(1) & 0xff) << 16) |
      ((bytes(2) & 0xff) << 8) |
      (bytes(3) & 0xff)

  override def equals(obj: Any): Boolean = obj match {
    case h2: Hash => java.util.Arrays.equals(bytes, h2.bytes)
    case _        => false
  }

  def prefix60AsLong: Long = ByteBuffer.wrap(bytes).getLong >>> 4

  override def toString: String = {
    val chArray = new Array[Char](bytes.length * 2)
    def intToHex(i: Int): Char =
      if (i >= 0 && i < 10) ('0' + i).toChar
      else if (i < 16) ('a' + i - 10).toChar
      else throw new IllegalArgumentException(s"Cannot convert to hex: $i")

    @tailrec def rec(ix: Int): String =
      if (ix < bytes.length) {
        chArray(ix * 2) = intToHex((bytes(ix) & 0xf0) >> 4)
        chArray(ix * 2 + 1) = intToHex((bytes(ix) & 0x0f))
        rec(ix + 1)
      } else new String(chArray)
    rec(0)
  }
}
object Hash {
  def unsafe(bytes: Array[Byte]): Hash = new Hash(bytes)
  def apply(string: String): Hash = {
    def hexToInt(ch: Char): Int =
      if (ch >= '0' && ch <= '9') ch - '0'
      else if (ch >= 'a' && ch <= 'f') (ch - 'a') + 10
      else throw new IllegalArgumentException(s"not a hex char: '$ch' ${ch.toInt}")
    @tailrec def rec(ix: Int, buffer: Array[Byte]): Hash =
      if (ix < string.length) {
        buffer(ix / 2) = ((hexToInt(string.charAt(ix)) << 4) | hexToInt(string.charAt(ix + 1))).toByte
        rec(ix + 2, buffer)
      } else new Hash(buffer)

    rec(0, new Array[Byte](string.length / 2))
  }

  implicit val hashOrder: Ordering[Hash] = new Ordering[Hash] {
    override def compare(x: Hash, y: Hash): Int = util.Arrays.compare(x.bytes, y.bytes)
  }

  import spray.json.DefaultJsonProtocol._
  private val simpleHashFormat: JsonFormat[Hash] =
    // use truncated hashes for lesser memory usage
    JsonExtra.deriveFormatFrom[String].apply[Hash](_.toString, apply(_))
  implicit val hashFormat: JsonFormat[Hash] = DeduplicationCache.cachedFormat(simpleHashFormat)
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
    content: Vector[Hash]
) extends TreeNode {
  override def isBranch: Boolean = false
}
case class TreeBranch(
    name:    CachedName.T,
    subtree: Hash
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
    id:     Hash,
    `type`: BlobType,
    offset: Long,
    length: Int
) {
  def isTree: Boolean = `type` == BlobType.Tree
  def isData: Boolean = `type` == BlobType.Data
}
case class PackIndex(
    id:    Hash,
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