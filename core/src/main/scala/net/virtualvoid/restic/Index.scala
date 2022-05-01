package net.virtualvoid.restic

import java.io.{ BufferedOutputStream, File, FileOutputStream }
import java.nio.ByteOrder
import java.nio.channels.FileChannel
import java.nio.channels.FileChannel.MapMode
import scala.annotation.tailrec

trait Writer {
  def uint32le(value: Int): Unit
  def hash(hash: Hash): Unit
}
trait Reader {
  def uint32le(): Int
  def hash(): Hash
}

trait Serializer[T] {
  def entrySize: Int

  def write(id: Hash, t: T, writer: Writer): Unit
  def read(id: Hash, reader: Reader): T
}

trait Index[T] {
  def lookup(id: Hash): T

  def find(id: Hash): (Int, Int)
  def allKeys: IndexedSeq[Hash]
}

object Index {
  def writeIndexFile[T: Serializer](indexFile: File, data: Iterable[(Hash, T)]): Unit = {
    val serializer = implicitly[Serializer[T]]
    val fos = new BufferedOutputStream(new FileOutputStream(indexFile), 1000000)

    val writer = new Writer {
      def uint32le(value: Int): Unit = {
        fos.write(value)
        fos.write(value >> 8)
        fos.write(value >> 16)
        fos.write(value >> 24)
      }

      def hash(hash: Hash): Unit = {
        require(hash.bytes.length == 32)
        fos.write(hash.bytes)
      }
    }

    data.toVector.sortBy(_._1).foreach {
      case (id, value) =>
        writer.hash(id)
        serializer.write(id, value, writer)
    }
    fos.close()
  }

  def load[T: Serializer](indexFile: File): Index[T] = {
    val serializer = implicitly[Serializer[T]]
    val HeaderSize = 0
    val EntrySize = 32 /* hash size */ + serializer.entrySize

    val file = FileChannel.open(indexFile.toPath)
    val indexBuffer = file.map(MapMode.READ_ONLY, 0, file.size()).order(ByteOrder.LITTLE_ENDIAN)
    val numEntries = ((indexFile.length() - HeaderSize) / EntrySize).toInt
    println(s"[${indexFile.getName}] Found $numEntries")

    val longBEBuffer = indexBuffer.duplicate().order(ByteOrder.BIG_ENDIAN).asLongBuffer()

    def keyAt(idx: Int): Long = longBEBuffer.get((idx * EntrySize) >> 3) >>> 4 // only use 60 bits

    new Index[T] {
      override def allKeys: IndexedSeq[Hash] = new IndexedSeq[Hash] {
        override def length: Int = numEntries

        override def apply(i: Int): Hash = {
          val targetPackHashBytes = {
            val dst = new Array[Byte](32)
            indexBuffer.asReadOnlyBuffer.position(i * EntrySize).get(dst)
            dst
          }
          Hash.unsafe(targetPackHashBytes)
        }
      }

      override def lookup(id: Hash): T = {
        def entryAt(idx: Int, step: Int): T = {
          val targetBaseOffset = idx * EntrySize
          val reader = new Reader {
            val buffer = indexBuffer.asReadOnlyBuffer().position(targetBaseOffset + 32)

            override def uint32le(): Int =
              ((buffer.get() & 0xff)) |
                ((buffer.get() & 0xff) << 8) |
                ((buffer.get() & 0xff) << 16) |
                ((buffer.get() & 0xff) << 24)
            override def hash(): Hash = {
              val dst = new Array[Byte](32)
              buffer.get(dst)
              Hash.unsafe(dst)
            }
          }
          serializer.read(id, reader)
        }

        val (idx, step) = find(id)
        entryAt(idx, step)
      }

      def find(id: Hash): (Int, Int) = {
        val targetKey = id.prefix60AsLong

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
          else if (leftIndex > rightIndex) throw new NoSuchElementException(id.toString)
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