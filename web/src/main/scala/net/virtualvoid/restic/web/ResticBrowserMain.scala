package net.virtualvoid.restic
package web

import akka.actor.ActorSystem
import akka.http.scaladsl.Http

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

  val app = ResticApp(indexFile, _reader)

  val binding =
    Http().newServerAt("localhost", 8080)
      .bind(new ResticRoutes(app).main)
  binding.onComplete { res =>
    println(s"Binding now $res")
  }
}
