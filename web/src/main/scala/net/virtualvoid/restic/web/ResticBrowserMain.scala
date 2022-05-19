package net.virtualvoid.restic
package web

import akka.actor.ActorSystem
import akka.http.scaladsl.Http

object ResticBrowserMain extends App {
  implicit val system = ActorSystem()
  import system.dispatcher

  val settings = ResticSettings()
  val reader = ResticRepository.open(settings).getOrElse(throw new RuntimeException(s"Couldn't open repository at ${settings.repositoryDir}"))

  val binding =
    Http().newServerAt("localhost", 8080)
      .bind(new ResticRoutes(reader).main)
  binding.onComplete { res =>
    println(s"Binding now $res")
  }
}
