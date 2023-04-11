package net.virtualvoid.restic
package web

import org.apache.pekko.actor.ActorSystem
import org.apache.pekko.http.scaladsl.Http

import scala.concurrent.Await
import scala.concurrent.duration._

object ResticBrowserMain extends App {
  implicit val system = ActorSystem()
  import system.dispatcher

  val settings = ResticSettings()
  val reader = ResticRepository.open(settings).getOrElse(throw new RuntimeException(s"Couldn't open repository at ${settings.repositoryDir}"))

  println("Loading / updating indices...")
  Await.result(reader.initializeIndices(), 5.minutes)
  println("...finished.")

  val binding =
    Http().newServerAt("localhost", 8080)
      .bind(new ResticRoutes(reader).main)
  binding.onComplete { res =>
    println(s"Binding now $res")
  }
}
