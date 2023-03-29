package net.virtualvoid.restic

import org.apache.pekko.actor.ActorSystem
import org.apache.pekko.stream.scaladsl.Source
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
  def randomPackEntry(): PackEntry = {
    val target = randomHash()
    val pack = randomHash()
    val offset = random.nextInt(Int.MaxValue)
    val length = random.nextInt(Int.MaxValue)
    val tpe = if (random.nextBoolean()) BlobType.Tree else BlobType.Data
    val un = if (random.nextBoolean()) None else Some(random.nextInt(Int.MaxValue))
    PackEntry(pack, target, tpe, offset, length, un)
  }

  def indexFor(data: Seq[PackEntry]): (Map[Hash, Seq[PackEntry]], Index[PackEntry]) = {
    implicit val indexEntrySerializer = PackBlobSerializer
    val indexed = data.groupBy(_.id)
    val tmpFile = File.createTempFile("index", ".idx", new File("/tmp"))
    tmpFile.deleteOnExit()
    val index = Index.createIndex(tmpFile, Source(data.map(e => e.id -> e))).futureValue

    (indexed, index)
  }

  "Index should" - {
    "work for pack index" - {
      val data = Vector.fill(10000)(randomPackEntry())
      val (indexed, index) = indexFor(data)

      "find existing keys" in {
        indexed.keys.foreach { k =>
          indexed(k).toSet(index.lookup(k)) shouldBe true
        }
      }
      "fail when key is missing" in {
        (1 to 100).foreach { _ =>
          val missing = randomHash()
          if (!indexed.contains(missing)) {
            index.lookupOption(missing) shouldBe None
          }
        }
      }
    }
    "support returning more than one match" - {
      val data0 = Vector.fill(10000)(randomPackEntry())
      val duplicates =
        Seq(
          data0.head.copy(offset = 23),
          data0.head.copy(offset = 42)
        )

      val data = data0 ++ duplicates
      val (indexed, index) = indexFor(data)

      "find single for existing key" in {
        indexed.filter(_._2.size == 1).foreach {
          case (id, v) =>
            index.lookupAll(id) shouldEqual v
        }
      }
      "find multiple for existing key" in {
        index.lookupAll(data0.head.id).map(_.offset).toSet shouldEqual Set(23, 42, data0.head.offset)
      }
      "find non for non-existing key" in {
        (1 to 100).foreach { _ =>
          val missing = randomHash()
          if (!indexed.contains(missing)) {
            index.lookupAll(missing) shouldEqual Nil
          }
        }
      }
    }
    "handle conflicts after first 60 bits of hash" in pending
    "don't fail when interpolation goes outside of range if nothing is found" in pending
  }

  override protected def afterAll(): Unit = system.terminate()
}
