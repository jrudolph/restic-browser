package net.virtualvoid.restic

import akka.actor.ActorSystem
import akka.stream.scaladsl.Source
import org.scalatest.BeforeAndAfterAll
import org.scalatest.concurrent.ScalaFutures
import org.scalatest.freespec.AnyFreeSpec
import org.scalatest.matchers.should.Matchers

import java.io.File
import scala.concurrent.duration._
import scala.util.Random

class IndexSpec extends AnyFreeSpec with Matchers with BeforeAndAfterAll with ScalaFutures {
  implicit val system = ActorSystem()
  override implicit val patienceConfig = PatienceConfig(5.seconds)

  val random = new Random
  def randomHash(): Hash = Hash.unsafe(random.nextBytes(32))
  val (indexed, index) = {
    implicit val indexEntrySerializer = PackBlobSerializer
    def randomPackEntry(): PackEntry = {
      val target = randomHash()
      val pack = randomHash()
      val offset = random.nextInt(Int.MaxValue)
      val length = random.nextInt(Int.MaxValue)
      val tpe = if (random.nextBoolean()) BlobType.Tree else BlobType.Data
      PackEntry(pack, target, tpe, offset, length)
    }

    val data = Vector.fill(10000)(randomPackEntry())
    val indexed = data.groupBy(_.id).view.mapValues(_.head).toMap
    val tmpFile = File.createTempFile("index", ".idx", new File("/tmp"))
    tmpFile.deleteOnExit()
    val index = Index.createIndex(tmpFile, Source(indexed)).futureValue

    (indexed, index)
  }

  "Index should" - {
    "work for pack index" - {
      "find existing keys" in {
        indexed.keys.foreach { k =>
          index.lookup(k) shouldEqual indexed(k)
        }
      }
      "fail when key is missing" in {
        (1 to 100).foreach { _ =>
          val missing = randomHash()
          if (!indexed.contains(missing)) {
            the[NoSuchElementException] thrownBy (index.lookup(missing))
          }
        }
      }
    }
    "handle conflicts after first 60 bits of hash" in pending
    "support returning more than one match" in pending
    "don't fail when interpolation goes outside of range if nothing is found" in pending
  }

  override protected def afterAll(): Unit = system.terminate()
}
