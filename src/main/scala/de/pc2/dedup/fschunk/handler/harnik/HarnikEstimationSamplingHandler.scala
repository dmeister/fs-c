package de.pc2.dedup.fschunk.handler.harnik

import scala.collection.mutable.ArrayBuffer
import scala.collection.mutable.Map

import org.apache.commons.math3.random.MersenneTwister
import org.apache.commons.math3.random.RandomDataImpl

import de.pc2.dedup.chunker.Chunk
import de.pc2.dedup.chunker.Digest
import de.pc2.dedup.chunker.File
import de.pc2.dedup.chunker.FilePart
import de.pc2.dedup.fschunk.handler.FileDataHandler
import de.pc2.dedup.util.Log

case class HarnikReserviorEntry(val digest: Digest, val chunkSize: Int, val baseSampleCount: Int, val scanCount: Int) {

}

/**
 * Handler implementing the sampling phase of Harnik's deduplication estimation appraoch using
 * Reseviour sampling
 */
class HarnikEstimationSamplingHandler(val configuredSampleSize: Option[Int], output: Option[String]) extends FileDataHandler with Log {
  var lock: AnyRef = new Object()

  val sampleSize = configuredSampleSize match {
    case Some(i) => i
    case None => output match {
      case None =>
        445570; // good enough for an estimation with 1% error in 1:3 compression, but not enough for file types (usually)
      case Some(s) =>
        4 * 1237938 // much more! Needed so that at least the major file types/size categories get enough samples
    }
  }

  val reserviorBuffer = new ArrayBuffer[HarnikReserviorEntry](sampleSize)
  val rng = new RandomDataImpl(new MersenneTwister())

  var processedChunkCount: Long = 0
  var processedDataCount: Long = 0

  lazy val estimationSample = getEstimationSample()

  private def getEstimationSample(): HarnikEstimationSample = {

    logger.debug("Finish sampling")

    val entryMap = Map.empty[Digest, HarnikSamplingEntry]
    for (entry <- reserviorBuffer) {
      if (entryMap.contains(entry.digest)) {
        val existingEntry = entryMap(entry.digest)
        entryMap.update(entry.digest, HarnikSamplingEntry(existingEntry.baseSampleCount + 1, existingEntry.chunkSize))
      } else {
        entryMap += (entry.digest -> HarnikSamplingEntry(1, entry.chunkSize))
      }
    }
    reserviorBuffer.clear()
    return new HarnikEstimationSample(entryMap.toMap)
  }

  def handleChunk(chunk: Chunk) {
    if (reserviorBuffer.size < sampleSize) {
      // still filling up the reservior

      val entry = HarnikReserviorEntry(chunk.fp, chunk.size, 1, 0)
      reserviorBuffer.append(entry)
    } else {

      val r = rng.nextLong(0, processedChunkCount) // endpoints included
      if (r < sampleSize) {
        val index = r.asInstanceOf[Int]
        val entry = HarnikReserviorEntry(chunk.fp, chunk.size, 1, 0)
        reserviorBuffer.update(index, entry)
      }
    }
    processedChunkCount += 1
    processedDataCount += chunk.size
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