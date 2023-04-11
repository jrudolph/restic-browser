package net.virtualvoid.restic

import org.apache.pekko.actor.ActorSystem
import org.apache.pekko.http.caching.LfuCache
import org.apache.pekko.http.caching.scaladsl.{ CachingSettings, LfuCacheSettings }
import org.apache.pekko.util.ByteString

import java.io.{ File, FileInputStream, FileOutputStream, InputStream, OutputStream }
import java.security.MessageDigest
import java.util.concurrent.atomic.AtomicLong
import java.util.zip.{ GZIPInputStream, GZIPOutputStream }
import scala.concurrent.Future

object Utils {
  def writeString(os: OutputStream, string: String): Unit =
    try os.write(string.getBytes("utf8"))
    finally os.close()
  def writeString(file: File, string: String): Unit =
    writeString(new FileOutputStream(file), string)
  def writeStringGzipped(file: File, string: String): Unit =
    writeString(new GZIPOutputStream(new FileOutputStream(file)), string)
  def readString(is: InputStream): String = try {
    var out = ByteString.empty
    val buffer = new Array[Byte](65536)
    var read = 0
    do {
      read = is.read(buffer)
      if (read >= 0)
        out ++= ByteString.fromArray(buffer, 0, read)
    } while (read > 0)
    out.utf8String
  } finally is.close()
  def readString(file: File): String =
    readString(new FileInputStream(file))
  def readStringGzipped(file: File): String =
    readString(new GZIPInputStream(new FileInputStream(file)))

  def sha256sum(str: String): String = {
    val sha = MessageDigest.getInstance("SHA-256")
    val bytes = sha.digest(str.getBytes("utf8"))
    bytes.map("%02x".format(_)).mkString
  }

  def memoized[T, U](f: T => Future[U])(implicit system: ActorSystem): T => Future[U] = {
    val cache = LfuCache[T, U](CachingSettings(system).withLfuCacheSettings(LfuCacheSettings(system).withMaxCapacity(50000).withInitialCapacity(10000)))

    val hits = new AtomicLong()
    val misses = new AtomicLong()

    t => {
      def report(): Unit = {
        val hs = hits.get()
        val ms = misses.get()
        println(f"Hits: $hs%10d Misses: $ms%10d hit rate: ${hs.toFloat / (hs + ms)}%5.2f")
      }

      if (cache.get(t).isDefined) {
        if (hits.incrementAndGet() % 10000 == 0) report()
      } else if (misses.incrementAndGet() % 10000 == 0) report()

      cache(t, () => f(t))
    }
  }
}
