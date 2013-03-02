package de.pc2.dedup.fschunk.handler.direct

import java.security.MessageDigest

import scala.collection.mutable.ListBuffer
import scala.collection.mutable.Map
import scala.collection.mutable.Set

import de.pc2.dedup.chunker.Digest
import de.pc2.dedup.chunker.Chunk
import de.pc2.dedup.chunker.File
import de.pc2.dedup.chunker.FilePart
import de.pc2.dedup.fschunk.handler.FileDataHandler
import de.pc2.dedup.util.FileSizeCategory
import de.pc2.dedup.util.Log
import de.pc2.dedup.util.StorageUnit

class FullFileRedundancyHandler() extends FileDataHandler with Log {
  var lock: AnyRef = new Object()
  val fileDigestSet = Set.empty[Digest]
  
  var totalFileCount : Long = 0
  var totalFileSize : Long = 0
  var uniqueFileCount : Long = 0
  var uniqueFileSize : Long = 0

  val openFileMap = Map.empty[String, MessageDigest]

  def getFileDigestBuilder(filename: String): MessageDigest = {
    if (openFileMap.contains(filename)) {
      openFileMap(filename)
    } else {
      val md = MessageDigest.getInstance("MD5")
      openFileMap += (filename -> md)
      md
    }
  }
  
  def handle(fp: FilePart) {
    lock.synchronized {
      for (chunk <- fp.chunks) {
          getFileDigestBuilder(fp.filename).update(chunk.fp.digest)
      }
    }
  }

  def handle(f: File) {
    lock.synchronized {
      logger.debug("Handle file %s".format(f.filename))

      for (chunk <- f.chunks) {
          getFileDigestBuilder(f.filename).update(chunk.fp.digest)
      }
      val fullFileFingerprint : Digest = new Digest(getFileDigestBuilder(f.filename).digest())

      totalFileCount += 1
      totalFileSize += f.fileSize

      if (fileDigestSet.contains(fullFileFingerprint)) {
        uniqueFileCount += 1
        uniqueFileSize += f.fileSize
      } else {
        fileDigestSet += fullFileFingerprint
      }

      openFileMap -= f.filename
    }
  }

  override def quit() {

    val msg = new StringBuffer()
    msg.append("\tTotal File Count\t%s\t(%s)\n".format(StorageUnit(totalFileCount), totalFileCount))
    msg.append("\tTotal File Size\t%sB\t(%s)\n".format(StorageUnit(totalFileSize), totalFileSize))
    msg.append("\n")
    msg.append("\tUnique File Count\t%s\t(%s)\n".format(StorageUnit(uniqueFileCount), uniqueFileCount))
    msg.append("\tUnique File Size\t%sB\t(%s)\n".format(StorageUnit(uniqueFileSize), uniqueFileSize))
    msg.append("\n")
    val dedupilicationRatio = uniqueFileSize.toDouble / totalFileSize.toDouble
    msg.append("\tDeduplication Ratio\t%s\n".format(dedupilicationRatio))

    println(msg.toString())
  }
}
