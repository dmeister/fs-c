package de.pc2.dedup.fschunk.handler.direct

import scala.collection.mutable.ListBuffer
import scala.collection.mutable.Map

import de.pc2.dedup.chunker.Chunk
import de.pc2.dedup.chunker.File
import de.pc2.dedup.chunker.FilePart
import de.pc2.dedup.fschunk.handler.FileDataHandler
import de.pc2.dedup.util.Log
import de.pc2.dedup.util.StorageUnit

/**
 * Handler to do a in-memory analysis of the trace.
 */
class InMemoryChunkHandler(silent: Boolean, d: ChunkIndex, chunkerName: Option[String]) extends FileDataHandler with Log {
  var totalFileSize = 0L
  var totalChunkSize = 0L
  var totalChunkCount = 0L

  var lock: AnyRef = new Object()
  val filePartialMap = Map.empty[String, ListBuffer[Chunk]]
  val startTime = System.currentTimeMillis()

  override def quit() {
    report()
  }

  override def report() {
    lock.synchronized {
      val stop = System.currentTimeMillis()
      val seconds = (stop - startTime) / 1000

      val totalRedundancy = totalFileSize - totalChunkSize
      val msg = new StringBuffer()
      msg.append("\n")
      if (!chunkerName.isEmpty) {
    	  msg.append("Chunker " + chunkerName + "\n")
      }
      msg.append("Total Size: " + StorageUnit(totalFileSize) + "\n")
      msg.append("Chunk Size: " + StorageUnit(totalChunkSize) + "\n")
      msg.append("Redundancy: " + StorageUnit(totalRedundancy))
      if (totalFileSize > 0) {
        msg.append(" (%.2f%%)".format(100.0 * totalRedundancy / totalFileSize))
      }
      msg.append("%nChunks: %d".format(totalChunkCount))
      if (totalChunkCount > 0) {
        msg.append(" (%s/Chunk)".format(StorageUnit(totalFileSize / totalChunkCount)))
      }
      msg.append("%nTime: %ds%n".format(seconds))
      if (seconds > 0) {
        msg.append("Throughput: %s/s".format(StorageUnit(totalFileSize / seconds)))
      } else {
        msg.append("Throughput: N/A")
      }
      logger.info(msg)
    }
  }

  def handle(fp: FilePart) {
    lock.synchronized {
      val fileMapBuffer = filePartialMap.get(fp.filename) match {
        case Some(l) => l
        case None =>
          val l = new ListBuffer[Chunk]()
          filePartialMap += (fp.filename -> l)
          l
      }
      fp.chunks.foreach(chunk => fileMapBuffer.append(chunk))
    }
  }

  def handle(f: File) {
    lock.synchronized {
      var chunkCount = 0L
      var chunkFileSize = 0L
      var chunkSize = 0L
      val allFileChunks = gatherAllFileChunks(f)
      for (chunk <- allFileChunks) {
        chunkCount += 1
        chunkFileSize += chunk.size
        if (!d.check(chunk.fp)) {
          // New Chunk
          d.update(chunk.fp)
          chunkSize += chunk.size
        }
      }
      totalChunkCount += chunkCount
      totalFileSize += f.fileSize
      totalChunkSize += chunkSize

      formatOutput(f, chunkFileSize, chunkSize)
    }
  }

  private def formatOutput(f: de.pc2.dedup.chunker.File, chunkFileSize: Long, chunkSize: Long) {

    val redundancy = f.fileSize - chunkSize
    val patchSize = f.fileSize - redundancy
    val illegalFile = patchSize < 0 || redundancy < 0 || f.fileSize != chunkFileSize

    val msg = new StringBuffer("%s - %s".format(f.filename, chunkerName))
    if (!silent || illegalFile) {
      msg.append("\nSize: %s (%d Byte)%n".format(
        StorageUnit(f.fileSize), f.fileSize))
      if (illegalFile) {
        msg.append("\nChunks size: %s (%d Byte)%n".format(
          StorageUnit(chunkFileSize), chunkFileSize))
      }
      msg.append("Redundancy: %s  (%d Byte)".format(StorageUnit(redundancy), redundancy))
      if (chunkFileSize > 0) {
        msg.append(" (%.2f%%)".format(100.0 * redundancy / chunkFileSize))
      }
      msg.append("%nPatch Size: %s (%d Byte)".format(
        StorageUnit(chunkFileSize - redundancy), chunkFileSize - redundancy))
    }
    if (illegalFile) {
      throw new Exception("Illegal file: %s".format(msg))
    } else {
      logger.info(msg)
    }
  }

  private def gatherAllFileChunks(f: de.pc2.dedup.chunker.File): Seq[de.pc2.dedup.chunker.Chunk] = {

    val allFileChunks: Seq[Chunk] = if (filePartialMap.contains(f.filename)) {
      val partialChunks = filePartialMap(f.filename)
      filePartialMap -= f.filename
      List.concat(partialChunks.toList, f.chunks)
    } else {
      f.chunks
    }
    allFileChunks
  }
}
