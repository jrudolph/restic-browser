package net.virtualvoid.restic
package web

import org.apache.pekko.http.scaladsl.model.headers.ContentDispositionTypes
import org.apache.pekko.http.scaladsl.model._
import org.apache.pekko.http.scaladsl.server.Directives._
import org.apache.pekko.http.scaladsl.server.{ Directive1, Route }
import org.apache.pekko.stream.scaladsl.Sink
import org.apache.pekko.util.ByteString

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
            pathEndOrSingleSlash {
              onSuccess(reader.packInfos) { infos =>
                complete(html.packs(infos.sortBy(_.id)))
              }
            },
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
                  parameter("json".?) {
                    case None =>
                      onSuccess(reader packEntryFor (hash)) { pE =>
                        def chainSet(shouldBeBlanked: Boolean = false): Directive1[ChainSet] =
                          if (shouldBeBlanked) provide(ChainSet(Nil))
                          else onSuccess(reader.backreferences.chainsFor(hash)).map(chainSetForChains)
                        pE.`type` match {
                          case BlobType.Tree =>
                            onSuccess(reader.loadTree(pE)) { t =>
                              chainSet(t.nodes.isEmpty) { cs => // do not calculate ChainSet for empty trees (because it is huge)
                                complete(html.tree(pE.packId, hash, t, cs))
                              }
                            }
                          case BlobType.Data =>
                            chainSet() { cs =>
                              complete(html.chunk(pE, cs))
                            }
                        }
                      }
                    case Some(_) =>
                      onSuccess(reader.loadBlob(hash)) { data =>
                        complete(HttpEntity(ContentTypes.`application/json`, ByteString(data)))
                      }
                  }
                case Some(_) =>
                  respondWithHeader(headers.`Content-Disposition`(ContentDispositionTypes.attachment, Map("filename" -> (hash.short + ".zip")))) {
                    complete(HttpEntity(ContentTypes.`application/octet-stream`, reader.asZip(hash)))
                  }
              }
            },
            pathPrefix(Segment) { fileName =>
              val hash = Hash(h)
              val tF = reader.loadTree(hash)
              onSuccess(tF) { t =>
                val leaf = t.nodes.find(_.name == fileName).get.asInstanceOf[TreeLeaf]
                import reader.system.dispatcher
                val entriesF = Future.traverse(leaf.content)(c => reader.blob2packIndex.map(_.lookup(c)))
                def dataEntity = {
                  val extIdx = fileName.lastIndexOf(".") + 1
                  val ext = fileName.drop(extIdx).toLowerCase
                  val mediaType = MediaTypes.forExtensionOption(ext).getOrElse(MediaTypes.`application/octet-stream`)
                  HttpEntity(ContentType(mediaType, () => HttpCharsets.`UTF-8`), reader.dataForLeaf(leaf))
                }

                onSuccess(entriesF) { entries =>
                  concat(
                    pathEnd {
                      onSuccess(chainsForFile(leaf)) { chains =>
                        complete(html.file(hash, fileName, leaf, entries, chainSetForChains(chains)))
                      }
                    },
                    path("download") {
                      respondWithHeader(headers.`Content-Disposition`(ContentDispositionTypes.attachment, Map("filename" -> fileName))) {
                        complete(HttpEntity(ContentTypes.`application/octet-stream`, reader.dataForLeaf(leaf)))
                      }
                    },
                    path("show") {
                      respondWithHeader(headers.`Content-Disposition`(ContentDispositionTypes.inline, Map("filename" -> fileName))) {
                        complete(dataEntity)
                      }
                    },
                    path("hexdump") {
                      val fullData = reader.dataForLeaf(leaf).runWith(Sink.fold(ByteString.empty)(_ ++ _))
                      onSuccess(fullData) { data =>
                        complete(html.hexdump(hash, fileName, printBytes(data.toArray[Byte])))
                      }
                    }
                  )
                }
              }
            }
          )
        },
        (pathPrefix("host" / Segment / Segments) & redirectToTrailingSlashIfMissing(StatusCodes.Found)) { (host, segments) =>
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
        snaps
          .map(_._2).groupBy(s => (s.hostname, s.paths.toSet, s.flatTags.toSet))
          .map {
            case ((host, paths, tags), snaps) => SnapshotInfo(host, paths, tags, snaps)
          }
          .toVector
      }

  def chainSetForChains(chains: Seq[Chain]): ChainSet = {
    def hostPathSnapshot(chain: Chain): Option[HostPathSnapshot] = chain.chain.reverse match {
      case SnapshotNode(id, snap) :: remaining =>
        val path = remaining.map(_.asInstanceOf[TreeChainNode].tree.name)
        Some(HostPathSnapshot(HostPath(snap.hostname, path), snap))
      case _ => None // orphan, let's ignore those for now
    }
    ChainSet(
      chains
        .flatMap(hostPathSnapshot)
        .groupBy(_.hostPath)
        .mapValues(x => SnapshotSet(x.map(_.snapshot)))
        .toSeq
        .sortBy(_._2.lastSeen)
        .reverse
    )
  }

  def chainsForFile(leaf: TreeLeaf): Future[Seq[Chain]] =
    if (leaf.content.isEmpty)
      Future.successful(Nil)
    else {
      reader.backreferences.chainsFor(leaf.content.head).map { chains =>
        chains.filter { c =>
          c.chain.head match {
            case TreeChainNode(_: TreeBranch) => true
            case TreeChainNode(_: TreeLink)   => true
            case TreeChainNode(l: TreeLeaf)   => l.content == leaf.content
          }

        }
      }
    }
}

case class HostPath(
    host: String,
    path: Seq[String]
) {
  def asString: String = s"$host:/${path.mkString("", "/", "")}"
  def asUrl: String = s"$host/${path.mkString("/")}"
}
case class HostPathSnapshot(hostPath: HostPath, snapshot: Snapshot)
case class ChainSet(
    hostPathChainsWithSnapshots: Seq[(HostPath, SnapshotSet)]
)