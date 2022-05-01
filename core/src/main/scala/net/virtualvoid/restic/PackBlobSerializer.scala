package net.virtualvoid.restic

object PackBlobSerializer extends Serializer[(Hash, PackBlob)] {
  override def entrySize: Int = 40

  override def write(id: Hash, t: (Hash, PackBlob), writer: Writer): Unit = {
    val (packId, packBlob) = t
    val isTree = if (packBlob.isTree) 1 else 0
    require(packBlob.offset <= Int.MaxValue)
    writer.hash(packId)
    writer.uint32le(packBlob.offset.toInt | (isTree << 31))
    writer.uint32le(packBlob.length)
  }
  override def read(id: Hash, reader: Reader): (Hash, PackBlob) = {
    val targetPackHash = reader.hash()
    val offsetAndType = reader.uint32le()
    val length = reader.uint32le()
    val offset = offsetAndType & 0x7fffffff
    val tpe = if ((offsetAndType & 0x80000000) != 0) BlobType.Tree else BlobType.Data

    (targetPackHash, PackBlob(id, tpe, offset, length))
  }
}