package net.virtualvoid.restic

import net.virtualvoid.restic.Hash.T

import java.io.{ BufferedOutputStream, File, FileOutputStream }
import java.nio.ByteOrder
import java.nio.channels.FileChannel
import java.nio.channels.FileChannel.MapMode
import scala.annotation.tailrec

trait Index {
  def lookup(blobId: Hash.T): (Hash.T, PackBlob)

  def find(blobId: Hash.T): (Int, Int)
  def allKeys: IndexedSeq[Hash.T]
}

object Index {
  def writeIndexFile(indexFile: File, index: Map[String, (Hash.T, PackBlob)]): Unit = {
    val keys = index.keys.toVector.sorted
    val fos = new BufferedOutputStream(new FileOutputStream(indexFile), 1000000)

    def uint32le(value: Int): Unit = {
      fos.write(value)
      fos.write(value >> 8)
      fos.write(value >> 16)
      fos.write(value >> 24)
    }
    def hash(hash: String): Unit = {
      require(hash.length == 64)
      var i = 0
      while (i < 32) {
        fos.write(Integer.parseInt(hash, i * 2, i * 2 + 2, 16))
        i += 1
      }
    }

    keys.foreach { blobId =>
      val (packId, packBlob) = index(blobId)
      hash(blobId)
      val isTree = if (packBlob.isTree) 1 else 0
      require(packBlob.offset <= Int.MaxValue)
      hash(packId)
      uint32le(packBlob.offset.toInt | (isTree << 31))
      uint32le(packBlob.length)
    }
    fos.close()
  }

  def load(indexFile: File): Index = {
    val HeaderSize = 0
    val EntrySize = 72 /* 2 * 32 + 4 + 4 */

    val file = FileChannel.open(indexFile.toPath)
    val indexBuffer = file.map(MapMode.READ_ONLY, 0, file.size()).order(ByteOrder.LITTLE_ENDIAN)
    val intBuffer = indexBuffer.duplicate().order(ByteOrder.LITTLE_ENDIAN).asIntBuffer()
    val numEntries = ((indexFile.length() - HeaderSize) / EntrySize).toInt
    println(s"Found $numEntries")

    val longBEBuffer = indexBuffer.duplicate().order(ByteOrder.BIG_ENDIAN).asLongBuffer()

    def keyAt(idx: Int): Long = longBEBuffer.get((idx * EntrySize) >> 3) >>> 4 // only use 60 bits

    new Index {
      override def allKeys: IndexedSeq[Hash.T] = new IndexedSeq[Hash.T] {
        override def length: Int = numEntries

        override def apply(i: Int): T = {
          val targetPackHashBytes = {
            val dst = new Array[Byte](32)
            indexBuffer.asReadOnlyBuffer.position(i * EntrySize).get(dst)
            dst
          }
          targetPackHashBytes.map("%02x".format(_)).mkString.asInstanceOf[Hash.T]
        }
      }

      override def lookup(blobId: Hash.T): (Hash.T, PackBlob) = {
        def entryAt(idx: Int, step: Int): (Hash.T, PackBlob) = {
          val targetBaseOffset = idx * EntrySize
          val targetPackHashBytes = {
            val dst = new Array[Byte](32)
            indexBuffer.asReadOnlyBuffer.position(targetBaseOffset + 32).get(dst)
            dst
          }
          val targetPackHash = targetPackHashBytes.map("%02x".format(_)).mkString.asInstanceOf[Hash.T]
          val offsetAndType = intBuffer.get((targetBaseOffset + 64) >> 2)
          val length = intBuffer.get((targetBaseOffset + 68) >> 2)
          val offset = offsetAndType & 0x7fffffff
          val tpe = if ((offsetAndType & 0x80000000) != 0) BlobType.Tree else BlobType.Data

          val res = (targetPackHash, PackBlob(blobId, tpe, offset, length))
          //println(f"[${blobId.take(15)}] step: $step%2d   at: $idx%8d found: $res")
          res
        }

        val (idx, step) = find(blobId)
        entryAt(idx, step)
      }

      def find(blobId: Hash.T): (Int, Int) = {
        val targetKey = java.lang.Long.parseLong(blobId.take(15), 16)

        def interpolate(left: Int, right: Int): Int = {
          val leftKey = keyAt(left)
          val rightKey = keyAt(right)
          left + ((targetKey - leftKey).toFloat * (right - left) / (rightKey - leftKey)).toInt
        }

        // https://www.sciencedirect.com/science/article/pii/S221509862100046X
        // hashes should be uniformly distributed, so interpolation search is fastest
        @tailrec def rec(leftIndex: Int, rightIndex: Int, guess: Int, step: Int): (Int, Int) = {
          val guessKey = keyAt(guess)
          //println(f"[$targetKey%015x] step: $step%2d left: $leftIndex%8d right: $rightIndex%8d range: ${rightIndex - leftIndex}%8d guess: $guess%8d ($guessKey%015x)")
          if (guessKey == targetKey) (guess, step)
          else if (leftIndex == rightIndex) throw new IllegalStateException
          else { // interpolation step
            val newLeft = if (targetKey < guessKey) leftIndex else guess + 1
            val newRight = if (targetKey < guessKey) guess - 1 else rightIndex

            rec(newLeft, newRight, interpolate(newLeft, newRight), step + 1)
          }
        }

        //val firstGuess = numEntries / 2
        val firstGuess = interpolate(0, numEntries - 1)
        rec(0, numEntries - 1, firstGuess, 1)
      }
    }
  }
}