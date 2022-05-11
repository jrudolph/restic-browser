package net.virtualvoid.restic
package web

import akka.actor.ActorSystem
import akka.http.scaladsl.Http

import java.io.File

object ResticBrowserMain extends App {
  implicit val system = ActorSystem()
  import system.dispatcher

  val repoDir = new File("/tmp/restic-repo")
  val cacheDir = {
    val res = new File("../restic-cache")
    res.mkdirs()
    res
  }
  val reader = ResticReader.openRepository(repoDir, cacheDir).getOrElse(throw new RuntimeException(s"Couldn't open repository at $repoDir"))

  val binding =
    Http().newServerAt("localhost", 8080)
      .bind(new ResticRoutes(reader).main)
  binding.onComplete { res =>
    println(s"Binding now $res")
  }
}
