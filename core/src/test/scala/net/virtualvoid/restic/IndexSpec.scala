package net.virtualvoid.restic

import org.scalatest.freespec.AnyFreeSpec
import org.scalatest.matchers.should.Matchers

import java.io.File
import scala.util.Random

class IndexSpec extends AnyFreeSpec with Matchers {
  "Index should" - {
    "work for pack index" in {
      implicit val indexEntrySerializer = PackBlobSerializer
      val random = new Random
      def randomPackEntry(): PackEntry = {
        val target = Hash.unsafe(random.nextBytes(32))
        val pack = Hash.unsafe(random.nextBytes(32))
        val offset = random.nextInt(Int.MaxValue)
        val length = random.nextInt(Int.MaxValue)
        val tpe = if (random.nextBoolean()) BlobType.Tree else BlobType.Data
        PackEntry(pack, target, tpe, offset, length)
      }

      val data = Vector.fill(10000)(randomPackEntry())
      val indexed = data.groupBy(_.id).view.mapValues(_.head).toMap
      val tmpFile = File.createTempFile("index", "idx", new File("/tmp"))
      tmpFile.deleteOnExit()
      Index.writeIndexFile(tmpFile, indexed)
      val index = Index.load(tmpFile)

      indexed.keys.foreach { k =>
        index.lookup(k) shouldEqual indexed(k)
      }
    }
  }
}
