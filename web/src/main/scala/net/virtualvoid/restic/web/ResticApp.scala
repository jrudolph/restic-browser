package net.virtualvoid.restic
package web

import scala.concurrent.Future

trait ResticApp {
  def reader: ResticReader
  def index: Index[PackEntry]
  def loadTree(hash: Hash): Future[TreeBlob]
  def loadBlob(hash: Hash): Future[Array[Byte]]
}
