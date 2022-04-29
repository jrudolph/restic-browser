package net.virtualvoid.restic
package web

import akka.http.scaladsl.server.Directives._
import akka.http.scaladsl.server.Route

class ResticRoutes(app: ResticApp) {
  import TwirlSupport._

  lazy val main = concat(
    routes,
    auxiliary
  )

  lazy val routes =
    get {
      pathPrefix("blob" / Segment) { h =>
        val hash = Hash(h)
        val tF = app.loadTree(hash)
        onSuccess(tF) { t =>
          complete(html.tree(hash, t))
        }
      }
    }

  lazy val auxiliary: Route =
    getFromResourceDirectory("web")
}
