package net.virtualvoid.restic
package web

import akka.http.scaladsl.model.headers.ContentDispositionTypes
import akka.http.scaladsl.model.{ ContentTypes, HttpEntity, headers }
import akka.http.scaladsl.server.Directives._
import akka.http.scaladsl.server.Route
import akka.stream.scaladsl.Sink

import scala.concurrent.Future

case class SnapshotInfo(host: String, paths: Set[String], tags: Set[String], snapshots: Seq[Snapshot])

class ResticRoutes(reader: ResticRepository) {
  import TwirlSupport._
  import reader.system
  import reader.system.dispatcher

  lazy val snapshots = reader.allSnapshots()

  lazy val main = concat(
    routes,
    auxiliary
  )

  lazy val routes =
    get {
      concat(
        pathEndOrSingleSlash {
          onSuccess(snapshotInfos) { infos =>
            complete(html.home(infos, reader))
          }
        },
        pathPrefix("pack") {
          concat(
            //pathEndOrSingleSlash
            path(Segment) { h =>
              val hash = Hash(h)
              onSuccess(reader.packIndexFor(hash)) { idx =>
                complete(html.pack(idx))
              }
            }
          )

        },
        pathPrefix("blob" / Segment) { h =>
          val hash = Hash(h)
          concat(
            pathEnd {
              parameter("zip".?) {
                case None =>
                  onSuccess(reader packEntryFor (hash)) { pE =>
                    onSuccess(reader.loadTree(pE)) { t =>
                      complete(html.tree(pE.packId, hash, t))
                    }
                  }
                case Some(_) =>
                  respondWithHeader(headers.`Content-Disposition`(ContentDispositionTypes.attachment, Map("filename" -> (h.take(16) + ".zip")))) {
                    complete(HttpEntity(ContentTypes.`application/octet-stream`, reader.asZip(hash)))
                  }
              }
            },
            path(Segment) { fileName =>
              val hash = Hash(h)
              val tF = reader.loadTree(hash)
              onSuccess(tF) { t =>
                val node = t.nodes.find(_.name == fileName).get.asInstanceOf[TreeLeaf]
                val dataF = node.content.headOption.map(reader.loadBlob).getOrElse(Future.successful(Array.empty[Byte]))
                import reader.system.dispatcher
                val entriesF = Future.traverse(node.content)(c => reader.packIndex.map(_.lookup(c)))

                (onSuccess(dataF) & onSuccess(entriesF)) { (data, entries) =>
                  complete(html.file(hash, fileName, node, printBytes(data), entries))
                }
              }
            }
          )
        },
        (pathPrefix("host" / Segment / Segments) & pathEndOrSingleSlash) { (host, segments) =>
          val snapsF = snapshots.runWith(Sink.seq)
          onSuccess(snapsF) { snaps0 =>
            val snaps = snaps0.map(_._2)
            val thoseSnaps = snaps.filter(_.hostname == host)

            val branchesF = MergedTreeNode.lookupNode(segments, reader, thoseSnaps)
            onSuccess(branchesF) { (thisNode, children) =>
              complete(html.MergedTree(host, segments, thisNode, children))
            }
          }
        }
      )
    }

  lazy val auxiliary: Route =
    getFromResourceDirectory("web")

  def printBytes(bytes: Array[Byte], maxBytes: Int = 10000, addPrefix: Boolean = false, indent: String = " "): String = {
    def formatBytes(bs: Array[Byte]): Iterator[String] = {
      def asHex(b: Byte): String = "%02X" format b

      def formatLine(bs: Array[Byte]): String = {
        val hex = bs.map(asHex).mkString(" ")
        val ascii = bs.map(asASCII).mkString
        f"$indent%s$hex%-48s | $ascii"
      }
      def formatBytes(bs: Array[Byte]): String =
        bs.grouped(16).map(formatLine).mkString("\n")

      val prefix = s"${indent}ByteString(${bs.size} bytes)"

      if (bs.size <= maxBytes * 2) Iterator(if (addPrefix) prefix + "\n" else "", formatBytes(bs))
      else
        Iterator(
          if (addPrefix) s"$prefix first + last $maxBytes:\n" else "",
          formatBytes(bs.take(maxBytes)),
          s"\n$indent                    ... [${bs.size - (maxBytes * 2)} bytes omitted] ...\n",
          formatBytes(bs.takeRight(maxBytes)))
    }

    formatBytes(bytes).mkString("")
  }
  def asASCII(b: Byte): Char =
    if (b >= 0x20 && b < 0x7f) b.toChar
    else '.'

  def snapshotInfos: Future[Seq[SnapshotInfo]] =
    reader.allSnapshots()
      .runWith(Sink.seq)
      .map { snaps =>
        snaps.map(_._2).groupBy(s => (s.hostname, s.paths.toSet, s.flatTags.toSet))
          .map {
            case ((host, paths, tags), snaps) => SnapshotInfo(host, paths, tags, snaps)
          }
          .toVector
      }
}
