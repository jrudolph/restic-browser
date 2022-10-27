package net.virtualvoid.restic

object HashSerializer extends Serializer[Hash] {
  override def entrySize: Int = 32

  override def write(id: Hash, e: Hash, writer: Writer): Unit = writer.hash(e)
  override def read(id: Hash, reader: Reader): Hash = reader.hash()
}