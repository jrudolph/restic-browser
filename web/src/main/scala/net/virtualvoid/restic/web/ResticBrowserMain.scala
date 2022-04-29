package net.virtualvoid.restic.web

import akka.actor.ActorSystem
import akka.http.scaladsl.Http

object ResticBrowserMain extends App {
  implicit val system = ActorSystem()
  import system.dispatcher

  val binding =
    Http().newServerAt("localhost", 8080)
      .bind(ResticRoutes.main)
  binding.onComplete { res =>
    println(s"Binding now $res")
  }
}
