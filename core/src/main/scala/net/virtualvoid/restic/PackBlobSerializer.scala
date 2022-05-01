package net.virtualvoid.restic

object PackBlobSerializer extends Serializer[PackEntry] {
  override def entrySize: Int = 40

  override def write(id: Hash, e: PackEntry, writer: Writer): Unit = {
    val isTree = if (e.isTree) 1 else 0
    require(e.offset <= Int.MaxValue)
    writer.hash(e.packId)
    writer.uint32le(e.offset.toInt | (isTree << 31))
    writer.uint32le(e.length)
  }
  override def read(id: Hash, reader: Reader): PackEntry = {
    val packId = reader.hash()
    val offsetAndType = reader.uint32le()
    val length = reader.uint32le()
    val offset = offsetAndType & 0x7fffffff
    val tpe = if ((offsetAndType & 0x80000000) != 0) BlobType.Tree else BlobType.Data

    PackEntry(packId, id, tpe, offset, length)
  }
}