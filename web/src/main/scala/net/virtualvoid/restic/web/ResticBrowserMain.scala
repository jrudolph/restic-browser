package net.virtualvoid.restic.web

import akka.actor.ActorSystem
import akka.http.scaladsl.Http
import net.virtualvoid.restic.{ Hash, Index, ResticReader, TreeBlob }

import java.io.File
import scala.concurrent.Future

object ResticBrowserMain extends App {
  implicit val system = ActorSystem()
  import system.dispatcher

  val dataFile = "/home/johannes/.cache/restic/0227d36ed1e3dc0d975ca4a93653b453802da67f0b34767266a43d20c9f86275/data/5c/5c141f74d422dd3607f0009def9ffd369fc68bf3a7a6214eb8b4d5638085e929"
  val repoDir = new File("/home/johannes/.cache/restic/0227d36ed1e3dc0d975ca4a93653b453802da67f0b34767266a43d20c9f86275/")
  val backingDir = new File("/tmp/restic-repo")
  val cacheDir = {
    val res = new File("../restic-cache")
    res.mkdirs()
    res
  }
  val _reader = new ResticReader(repoDir, backingDir, cacheDir, system.dispatcher, system.dispatchers.lookup("blocking-dispatcher"))
  val indexFile = new File("../index.out")

  val app: ResticApp = new ResticApp {
    val reader: ResticReader = _reader
    val index: Index = Index.load(indexFile)

    override def loadTree(hash: Hash): Future[TreeBlob] = {
      val (p, b) = index.lookup(hash)
      reader.loadTree(p, b)
    }

    override def loadBlob(hash: Hash): Future[Array[Byte]] = {
      val (p, b) = index.lookup(hash)
      reader.loadBlob(p, b)
    }
  }

  val binding =
    Http().newServerAt("localhost", 8080)
      .bind(new ResticRoutes(app).main)
  binding.onComplete { res =>
    println(s"Binding now $res")
  }
}
