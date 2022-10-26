package net.virtualvoid.restic

import java.io.File

object FileExtension {
  implicit class FileImplicits(val f: File) extends AnyVal {
    def resolved: File =
      if (f.getName.length == 64) f // shortcut when full hashes are used
      else if (f.exists()) f
      else {
        val cands = Option(f.getParentFile.listFiles()).toSeq.flatten.filter(_.getName startsWith f.getName)
        cands.size match {
          case 1 => cands.head
          case 0 => f // no resolution possible return original
          case _ => throw new RuntimeException(s"Ambiguous ref: $f can be resolved to all of [${cands.mkString(", ")}]")
        }
      }
  }
}