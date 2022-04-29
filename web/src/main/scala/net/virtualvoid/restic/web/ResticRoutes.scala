package net.virtualvoid.restic.web

import akka.http.scaladsl.server.Directives._
import akka.http.scaladsl.server.Route

object ResticRoutes {
  lazy val main = concat(
    auxiliary
  )

  lazy val routes =
    get {
      complete("nothing here")
    }

  lazy val auxiliary: Route =
    getFromResourceDirectory("web")
}
