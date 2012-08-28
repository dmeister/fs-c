package de.pc2.dedup.fschunk.handler.harnik

import de.pc2.dedup.chunker._
import java.util.concurrent.atomic.AtomicLong
import java.util.concurrent.ConcurrentHashMap
import scala.collection.mutable.ListBuffer
import de.pc2.dedup.util.StorageUnit
import scala.actors.Actor
import scala.actors.Actor._
import de.pc2.dedup.util.Log
import scala.collection.mutable.Map
import scala.collection.mutable.Set
import de.pc2.dedup.fschunk.handler.FileDataHandler
import scala.collection.mutable.ArrayBuffer
import de.pc2.dedup.util.FileSizeCategory
import java.io.BufferedWriter
import java.io.FileWriter

class HarnikEstimationScanHandler(val sample: HarnikEstimationSample, val chunkerName: String) extends FileDataHandler with Log {
  var lock: AnyRef = new Object()

  var totalChunkCount: Long = 0

  val estimator = new HarnikEstimationSampleCounter(sample)

  def getSizeCategory(fileSize: Long): String = {
    return FileSizeCategory.getCategory(fileSize).toString()
  }

  override def quit() {
    lock.synchronized {
      val totalSize = estimator.totalChunkSize
      val deduplicationRatio = estimator.deduplicationRatio()
      val totalChunkSize = ((1 - deduplicationRatio) * totalSize).toLong
      val totalRedundancy = totalSize - totalChunkSize
      val msg = new StringBuffer()
      msg.append("\n")
      msg.append("Chunker " + chunkerName + "\n")
      msg.append("Total Size: " + StorageUnit(totalSize) + "\n")
      msg.append("Chunk Size: " + StorageUnit(totalChunkSize) + "\n")
      msg.append("Redundancy: " + StorageUnit(totalRedundancy))
      if (totalSize > 0) {
        msg.append(" (%.2f%%)".format(100.0 * totalRedundancy / totalSize))
      }
      msg.append("%nChunks: %s".format(StorageUnit(totalChunkCount)))
      if (totalChunkCount > 0) {
        msg.append(" (%s/Chunk)".format(
          StorageUnit(totalSize / totalChunkCount)))
      }
      logger.info(msg)
    }
  }

  override def report() {
  }

  def handleChunk(chunk: Chunk) {
    estimator.record(chunk.fp, chunk.size)
    totalChunkCount += 1
  }

  def handle(fp: FilePart) {
    lock.synchronized {
      for (chunk <- fp.chunks) {
        handleChunk(chunk)
      }
    }
  }

  def handle(f: File) {
    lock.synchronized {
      for (chunk <- f.chunks) {
        handleChunk(chunk)
      }
    }
  }
}