package net.virtualvoid.restic
package web

import scala.concurrent.Future

trait ResticApp {
  def reader: ResticReader
  def index: Index
  def loadTree(hash: Hash): Future[TreeBlob]
}
