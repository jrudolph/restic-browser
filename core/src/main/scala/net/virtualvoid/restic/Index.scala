package net.virtualvoid.restic

import akka.stream.Materializer
import akka.stream.scaladsl.Source

import java.io.{ BufferedOutputStream, File, FileOutputStream, OutputStream }
import java.nio.ByteOrder
import java.nio.channels.FileChannel
import java.nio.channels.FileChannel.MapMode
import scala.annotation.tailrec
import scala.concurrent.Future

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
  def lookupAll(id: Hash): Seq[T]

  def find(id: Hash): (Int, Int)
  def allKeys: IndexedSeq[Hash]
}

object Index {
  def trace(msg: String): Unit = Console.err.println(msg)
  def createIndex[T: Serializer](indexFile: File, data: Source[(Hash, T), Any])(implicit mat: Materializer): Future[Index[T]] = {
    val serializer = implicitly[Serializer[T]]
    import mat.executionContext

    def osWriter(os: OutputStream): Writer =
      new Writer {
        def uint32le(value: Int): Unit = {
          os.write(value)
          os.write(value >> 8)
          os.write(value >> 16)
          os.write(value >> 24)
        }

        def hash(hash: Hash): Unit = {
          require(hash.bytes.length == 32)
          os.write(hash.bytes)
        }
      }
    // strategy:
    // 1: create two files:
    //    * value file (with key-value tuples)
    //    * index with index_into_values
    // 2: use flashsort for sorting mapped index file
    //    - create bucket histogram (either into mapped file or directly in memory)
    //    - create cumulative bucket index
    //    - (in parallel) put values into right places into index file (using VarHandle to update bucket index array)
    //    - (in parallel) finally sort buckets
    // 3: optionally concat index and values (dropping the extra index)

    def streamDataOut(): Future[File] = {
      val tmpDataFile = File.createTempFile(s".${indexFile.getName}-values", ".tmp", indexFile.getParentFile)
      tmpDataFile.deleteOnExit()

      trace(s"[${indexFile.getName}] Streaming data to tmp file ${tmpDataFile}")
      val dataOut = new BufferedOutputStream(new FileOutputStream(tmpDataFile), 1000000)
      val dataWriter = osWriter(dataOut)

      data.runForeach {
        case (id, value) =>
          dataWriter.hash(id)
          serializer.write(id, value, dataWriter)
      }.map { _ =>
        dataOut.close()
        tmpDataFile
      }
    }

    def storeIndex(tmpDataFile: File): Index[T] = {
      val HeaderSize = 0
      val EntrySize = 32 /* hash size */ + serializer.entrySize
      require((serializer.entrySize & 0x07) == 0)

      val dataChannel = FileChannel.open(tmpDataFile.toPath)
      val buffer = dataChannel.map(MapMode.READ_ONLY, 0, tmpDataFile.length())
      val longBEBuffer = buffer.duplicate().order(ByteOrder.BIG_ENDIAN).asLongBuffer()

      def keyAt(idx: Int): Long = longBEBuffer.get((idx * EntrySize) >> 3) >>> 4 // only use 60 bits

      val numEntries = ((tmpDataFile.length() - HeaderSize) / EntrySize).toInt

      // round down to previous power of 2
      val bucketBits = math.max(1, 32 - Integer.numberOfLeadingZeros(numEntries) - 4)
      val buckets = 1 << bucketBits
      trace(s"[${indexFile.getName}] Sorting into $buckets buckets ($bucketBits bits) for $numEntries entries")

      // create histogram - size: ~ buckets bytes
      val bucketHistogram = Array.fill[Int](buckets)(0) // TODO: use one array for offsets + bucketHistogram
      (0 until numEntries).foreach { i =>
        val key = keyAt(i)
        val bucket = (key >>> (60 - bucketBits)).toInt
        val n = bucketHistogram(bucket)
        //trace(f"Bucket for key ${key}%015x: $bucket%015x histo: $n")
        bucketHistogram(bucket) = (n + 1)
      }
      // find cumulative offsets - size: ~ buckets * 4 bytes
      val offsets = bucketHistogram.scanLeft(0)(_ + _) // .dropRight(1) - right-most entry can be ignored

      // populate results - size: ~ numEntries * 4 bytes
      val indices = Array.fill[Int](numEntries)(0)
      (0 until numEntries).foreach { i =>
        val key = keyAt(i)
        val bucket = (key >>> (60 - bucketBits)).toInt
        val o = offsets(bucket)
        indices(o) = i
        offsets(bucket) += 1
      }

      // sort buckets
      trace(s"[${indexFile.getName}] Sort buckets");
      //
      {
        var at = 0
        var bucket = 0
        while (bucket < buckets) {
          val end = offsets(bucket)
          while (at < end) {
            //trace(f"at: $at%10d bucket: $bucket%10d")
            val targetKey = keyAt(indices(at))

            var pos = at - 1
            while (pos >= 0 && {
              val cand = keyAt(indices(pos))
              //trace(f"at: $at%10d end: $end%10d bucket: $bucket%10d pos: $pos%10d targetKey: $targetKey%015x $cand%015x")
              targetKey < cand
            }) {

              // swap down to find final position
              val x = indices(pos)
              indices(pos) = indices(pos + 1)
              indices(pos + 1) = x
              pos -= 1
            }

            at += 1
          }

          bucket += 1
        }
      }

      trace(s"[${indexFile.getName}] Creating final index")

      // Copy out entries in the right order
      val tmpIndexFile = File.createTempFile(s".${indexFile.getName}-indices", ".tmp", indexFile.getParentFile)
      val out = new BufferedOutputStream(new FileOutputStream(tmpIndexFile), 1000000) //
      val b = new Array[Byte](EntrySize)
      indices.foreach { i =>
        buffer.position(i * EntrySize).get(b)
        out.write(b)
      }
      out.close()

      dataChannel.close()
      tmpDataFile.delete()
      tmpIndexFile.renameTo(indexFile)
      load(indexFile)
    }
    streamDataOut().map(storeIndex)
  }

  def load[T: Serializer](indexFile: File): Index[T] = {
    val serializer = implicitly[Serializer[T]]
    val HeaderSize = 0
    val EntrySize = 32 /* hash size */ + serializer.entrySize

    val file = FileChannel.open(indexFile.toPath)
    val indexBuffer = file.map(MapMode.READ_ONLY, 0, file.size()).order(ByteOrder.LITTLE_ENDIAN)
    val numEntries = ((indexFile.length() - HeaderSize) / EntrySize).toInt
    trace(s"[${indexFile.getName}] Found $numEntries")

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

      private def entryAt(id: Hash, idx: Int, step: Int): T = {
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

      override def lookupAll(id: Hash): Seq[T] = try {
        val (idx, step) = find(id)
        val targetKey = id.prefix60AsLong
        @tailrec def it(at: Int, step: Int): Int =
          if (at >= 0 && at < numEntries && keyAt(at) == targetKey) it(at + step, step)
          else at - step

        val first = it(idx, -1)
        val last = it(idx, +1)
        (first to last).map(entryAt(id, _, step))
      } catch {
        case _: NoSuchElementException => Nil
      }

      override def lookup(id: Hash): T = {
        val (idx, step) = find(id)
        entryAt(id, idx, step)
      }

      def find(id: Hash): (Int, Int) = {
        val targetKey = id.prefix60AsLong

        def interpolate(left: Int, right: Int): Int = {
          val leftKey = keyAt(left)
          val rightKey = keyAt(right)
          left + ((targetKey - leftKey).toFloat * (right - left) / (rightKey - leftKey)).toInt
        }
        //val firstGuess = numEntries / 2
        val firstGuess = interpolate(0, numEntries - 1)

        // https://www.sciencedirect.com/science/article/pii/S221509862100046X
        // hashes should be uniformly distributed, so interpolation search is fastest
        @tailrec def rec(leftIndex: Int, rightIndex: Int, guess: Int, step: Int, trace: Boolean = false): (Int, Int) =
          {
            val guessKey = keyAt(guess)
            if (trace) Index.trace(f"[$targetKey%015x] step: $step%2d left: $leftIndex%8d right: $rightIndex%8d range: ${rightIndex - leftIndex}%8d guess: $guess%8d ($guessKey%015x)")
            if (guessKey == targetKey) (guess, step)
            else if (leftIndex > rightIndex || guess < leftIndex || guess > rightIndex) throw new NoSuchElementException(id.toString)
            else if (step > 50) // 10 + log2(numEntries)
              if (!trace) rec(0, numEntries - 1, firstGuess, 1, trace = true)
              else throw new IllegalStateException(f"didn't converge after $step steps: [$targetKey%015x] step: $step%2d left: $leftIndex%8d right: $rightIndex%8d range: ${rightIndex - leftIndex}%8d guess: $guess%8d (${keyAt(guess)}%015x)")
            else {
              val newLeft = if (targetKey < guessKey) leftIndex else guess + 1
              val newRight = if (targetKey < guessKey) guess - 1 else rightIndex

              val nextGuess =
                if (step < 10) interpolate(newLeft, newRight) // interpolation
                else (newLeft + newRight) / 2 // binary search

              rec(newLeft, newRight, nextGuess, step + 1, trace)
            }
          }

        rec(0, numEntries - 1, firstGuess, 1)
      }
    }
  }
}
