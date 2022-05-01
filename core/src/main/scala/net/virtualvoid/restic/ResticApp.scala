package net.virtualvoid.restic

import java.io.File
import scala.concurrent.Future

trait ResticApp {
  def reader: ResticReader
  def index: Index[PackEntry]
  def loadTree(hash: Hash): Future[TreeBlob]
  def loadBlob(hash: Hash): Future[Array[Byte]]
}

object ResticApp {
  def apply(indexFile: File, _reader: ResticReader): ResticApp =
    new ResticApp {
      val reader: ResticReader = _reader
      val index: Index[PackEntry] = Index.load(indexFile)(PackBlobSerializer)

      override def loadTree(hash: Hash): Future[TreeBlob] =
        reader.loadTree(index.lookup(hash))

      override def loadBlob(hash: Hash): Future[Array[Byte]] =
        reader.loadBlob(index.lookup(hash))
    }
}