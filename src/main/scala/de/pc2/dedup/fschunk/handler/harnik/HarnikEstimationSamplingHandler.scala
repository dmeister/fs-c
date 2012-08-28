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
import org.apache.commons.math3.random.RandomDataImpl
import org.apache.commons.math3.random.MersenneTwister

case class HarnikReserviorEntry(val chunkId: Long, val digest: Digest, val chunkSize: Int, val baseSampleCount: Int, val scanCount: Int) {

}

/**
 * Handler implementing the sampling phase of Harnik's deduplication estimation appraoch using
 * Reseviour sampling
 */
class HarnikEstimationSamplingHandler(val sampleSize: Int, val chunkerName: String) extends FileDataHandler with Log {
  var lock: AnyRef = new Object()
  val startTime = System.currentTimeMillis()

  val reserviorSampleSize = sampleSize * 4
  val reserviorBuffer = new ArrayBuffer[HarnikReserviorEntry](reserviorSampleSize)
  val rng = new RandomDataImpl(new MersenneTwister())
  var processedChunkCount: Long = 0
  var processedChunkPartCount: Long = 0

  override def quit() {
    report()
  }

  def getEstimationSample(): HarnikEstimationSample = {

    logger.debug("Finish sampling")

    var acceptedSampleCount: Long = 0
    val chunkIdSet = Set.empty[Long]
    val entryMap = Map.empty[Digest, HarnikSamplingEntry]
    for (entry <- reserviorBuffer) {
      if (chunkIdSet.contains(entry.chunkId)) {
        // This exact chunk is already in the sample, do not add it twice
      } else {

        logger.debug("Process reservior entry %s".format(entry))

        if (entryMap.contains(entry.digest)) {
          val existingEntry = entryMap(entry.digest)
          entryMap.update(entry.digest, HarnikSamplingEntry(existingEntry.baseSampleCount + 1))
        } else {
          entryMap += (entry.digest -> HarnikSamplingEntry(1))
        }

        acceptedSampleCount += 1
        chunkIdSet += entry.chunkId
      }
    }

    if (acceptedSampleCount < sampleSize) {
      logger.warn("Insufficient sample reservior: accepted samples %s, sample size %s".format(acceptedSampleCount, sampleSize))
    }
    return new HarnikEstimationSample(entryMap.toMap)
  }

  override def report() {
  }

  def handleChunkPart(chunk: Chunk) {
    if (reserviorBuffer.size < reserviorSampleSize) {
      // still filling up the reservior

      val entry = HarnikReserviorEntry(processedChunkCount, chunk.fp, chunk.size, 1, 0)
      logger.debug("Filling up reserviour %s (%s/%s)".format(entry, reserviorBuffer.size, reserviorSampleSize))

      reserviorBuffer.append(entry)
      processedChunkPartCount += 1
    } else {

      val r = rng.nextLong(0, processedChunkPartCount) // endpoints included
      if (r < reserviorSampleSize) {
        val index = r.asInstanceOf[Int]

        val entry = HarnikReserviorEntry(processedChunkCount, chunk.fp, chunk.size, 1, 0)
        logger.debug("Replace reserviour %s (%s)".format(entry, index))
        reserviorBuffer.update(index, entry)
      }
      processedChunkPartCount += 1
    }
  }

  def getTossesPerChunk(chunk: Chunk) : Int = {
	  val a = chunk.size / 1024
	  val b = chunk.size % 1024
	  
	  if (b == 0) {
	    a
	  } else {
	    a + 1
	  }
  }
  
  def handleChunk(chunk: Chunk) {
    for (i <- 0 to getTossesPerChunk(chunk)) {
      handleChunkPart(chunk)
    }
    processedChunkCount += 1
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