package de.pc2.dedup.fschunk.handler.direct

import de.pc2.dedup.chunker.File
import de.pc2.dedup.chunker.FilePart
import de.pc2.dedup.fschunk.handler.FileDataHandler
import de.pc2.dedup.util.Log
import de.pc2.dedup.util.StorageUnit

class StandardReportingHandler() extends FileDataHandler with Log {
  var lock: AnyRef = new Object()
  val startTime = System.currentTimeMillis()

  var chunkCount: Long = 0
  var fileCount: Long = 0
  var dataSize: Long = 0

  override def quit() {
    report()
  }

  override def report() {
    lock.synchronized {
      val stop = System.currentTimeMillis()
      val seconds = (stop - startTime) / 1000

      val tp = if (seconds > 0) {
        "%s/s".format(StorageUnit(dataSize / seconds))
      } else {
        "N/A"
      }
      val fps = if (seconds > 0) {
        "%s/s".format(StorageUnit(fileCount / seconds))
      } else {
        "N/A"
      }
      logger.info("Data size %sB (%s), file count %s (%s), chunks %s".format(StorageUnit(dataSize),
        tp,
        StorageUnit(fileCount),
        fps,
        StorageUnit(chunkCount)))
    }
  }

  def handle(fp: FilePart) {
    lock.synchronized {
      chunkCount += fp.chunks.size
    }
  }

  def handle(f: File) {
    lock.synchronized {
      chunkCount += f.chunks.size
      fileCount += 1
      dataSize += f.fileSize
    }
  }
}