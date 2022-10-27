package net.virtualvoid.restic

import com.lambdaworks.crypto.SCrypt
import spray.json._

import java.nio.ByteBuffer
import java.time.ZonedDateTime
import java.time.format.DateTimeFormatter
import java.util.Base64
import javax.crypto.spec.SecretKeySpec
import scala.annotation.tailrec
import scala.util.Try

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

  def short: String = toString.take(16)
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
    override def compare(x: Hash, y: Hash): Int = {
      val xb = x.bytes
      val yb = y.bytes
      require(xb.length == yb.length)
      @tailrec def rec(ix: Int): Int =
        if (ix < xb.size) {
          val r = Integer.compare(xb(ix) & 0xff, yb(ix) & 0xff)
          if (r == 0) rec(ix + 1)
          else r
        } else 0
      rec(0)
    }
  }

  import spray.json.DefaultJsonProtocol._
  private val simpleHashFormat: JsonFormat[Hash] =
    // use truncated hashes for lesser memory usage
    JsonExtra.deriveFormatFrom[String].apply[Hash](_.toString, apply(_))
  implicit val hashFormat: JsonFormat[Hash] = simpleHashFormat
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
  implicit val hashFormat: JsonFormat[CachedName.T] = simpleCachedNameFormat
}

sealed trait TreeNode extends Product {
  def name: String
  def isBranch: Boolean
  def isLeaf: Boolean
}
case class TreeLeaf(
    name:    CachedName.T,
    size:    Option[Long],
    content: Vector[Hash]
) extends TreeNode {
  override def isBranch: Boolean = false
  override def isLeaf: Boolean = true
}
case class TreeBranch(
    name:    CachedName.T,
    subtree: Hash
) extends TreeNode {
  override def isBranch: Boolean = true
  override def isLeaf: Boolean = false
}
case class TreeLink(
    name:       CachedName.T,
    linktarget: String
) extends TreeNode {
  override def isBranch: Boolean = false
  override def isLeaf: Boolean = false
}
case class TreeBlob(
    nodes: Vector[TreeNode]
)
object TreeBlob {
  import spray.json.DefaultJsonProtocol._
  implicit val leafFormat = jsonFormat3(TreeLeaf.apply _)
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
    id:                  Hash,
    `type`:              BlobType,
    offset:              Long,
    length:              Int,
    uncompressed_length: Option[Int]
) {
  def isTree: Boolean = `type` == BlobType.Tree
  def isData: Boolean = `type` == BlobType.Data
  def isCompressed: Boolean = uncompressed_length.isDefined
  def toEntryOf(pack: Hash): PackEntry =
    PackEntry(pack, id, `type`, offset, length, uncompressed_length)
}
case class PackIndex(
    id:    Hash,
    blobs: Vector[PackBlob]
)
case class IndexFile(
    supersedes: Option[Seq[Hash]],
    packs:      Seq[PackIndex]
) {
  def realSupersedes = supersedes.getOrElse(Nil)
}
object IndexFile {
  import spray.json.DefaultJsonProtocol._
  implicit val packBlobFormat = jsonFormat5(PackBlob.apply _)
  implicit val packIndexFormat = jsonFormat2(PackIndex.apply _)
  implicit val indexFileFormat = jsonFormat2(IndexFile.apply _)
}

case class Snapshot(
    time:     ZonedDateTime,
    parent:   Option[Hash],
    tree:     Hash,
    paths:    Seq[String],
    hostname: String,
    tags:     Option[Seq[String]],
    username: String,
    uid:      Option[Int],
    gid:      Option[Int],
    excludes: Option[Seq[String]]
) {
  def flatTags: Seq[String] = tags.toSeq.flatten
}
object Snapshot {
  import spray.json.DefaultJsonProtocol._
  implicit val dateTimeFormat = new JsonFormat[ZonedDateTime] {
    override def read(json: JsValue): ZonedDateTime = json match {
      case JsString(str) => ZonedDateTime.parse(str, DateTimeFormatter.ISO_OFFSET_DATE_TIME)
    }

    override def write(obj: ZonedDateTime): JsValue = ???
  }

  implicit val snapshotFormat = jsonFormat10(Snapshot.apply _)
}

case class Config(
    version:            Int,
    id:                 String,
    chunker_polynomial: String
)
object Config {
  import spray.json.DefaultJsonProtocol._
  implicit val configFormat = jsonFormat3(Config.apply _)
}

class Base64Bytes(val bytes: Array[Byte]) extends AnyVal
object Base64Bytes {
  implicit val bytesAsBase64String: JsonFormat[Base64Bytes] = new JsonFormat[Base64Bytes] {
    override def read(json: JsValue): Base64Bytes = json match {
      case JsString(str) => new Base64Bytes(Base64.getDecoder.decode(str))
    }
    override def write(obj: Base64Bytes): JsValue = ???
  }
}

case class Mac(k: Base64Bytes, r: Base64Bytes)
case class MasterKey(mac: Mac, encrypt: Base64Bytes)
object MasterKey {
  import spray.json.DefaultJsonProtocol._

  implicit val macFormat = jsonFormat2(Mac.apply _)
  implicit val keyFormat = jsonFormat2(MasterKey.apply _)
}

case class Key(
    username: String,
    hostname: String,
    kdf:      String,
    N:        Int,
    r:        Int,
    p:        Int,
    salt:     Base64Bytes,
    data:     Base64Bytes,
    created:  String
) {
  def tryDecrypt(password: String): Option[MasterKey] = {
    val key = SCrypt.scrypt(password.getBytes("utf8"), salt.bytes, N, r, p, 64)
    val decr = new Decryptor(new SecretKeySpec(key.take(32), "AES"))
    Try(new String(decr.decrypt(ByteBuffer.wrap(data.bytes))).parseJson.convertTo[MasterKey]).toOption
  }
}
object Key {
  import spray.json.DefaultJsonProtocol._
  implicit val keyFormat = jsonFormat9(Key.apply _)
}

// for use in index
case class PackEntry(packId: Hash, id: Hash, `type`: BlobType, offset: Long, length: Int, uncompressed_length: Option[Int]) {
  def isTree: Boolean = `type` == BlobType.Tree
  def isData: Boolean = `type` == BlobType.Data
  def isCompressed: Boolean = uncompressed_length.isDefined
}