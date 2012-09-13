package de.pc2.dedup.fschunk.handler.harnik

import scala.collection.mutable.{Map => MutableMap}
import scala.collection.mutable.OpenHashMap
import scala.collection.Map

import de.pc2.dedup.chunker.Digest
import de.pc2.dedup.util.Log

case class HarnikSamplingEntry(val baseSampleCount: Int, val chunkSize: Int) {
}

class HarnikEstimationSample(val map: Map[Digest, HarnikSamplingEntry]) {

  def contains(fp: Digest): Boolean = {
    map.contains(fp)
  }

  def baseSampleCount(fp: Digest): Option[Int] = {
    if (map.contains(fp)) {
      Some(map(fp).baseSampleCount)
    } else {
      None
    }
  }

  def chunkSize(fp: Digest): Option[Int] = {
    if (map.contains(fp)) {
      Some(map(fp).chunkSize)
    } else {
      None
    }
  }
  
  def totalSampleCount: Long = {
    var s: Long = 0
    for ((k, v) <- map) {
      s += v.baseSampleCount
    }
    s
  }
}

class HarnikEstimationSampleCounter(val sample: HarnikEstimationSample) extends Log {
  val map = new OpenHashMap[Digest, Int](sample.totalSampleCount.toInt)
  var totalChunkSize: Long = 0

  def record(fp: Digest, size: Long): Boolean = {
    val r = if (sample.contains(fp)) {
      map.update(fp, map.getOrElse(fp, 0) + 1)
      true
    } else {
      false
    }

    totalChunkSize += size
    r
  }

  def recordNoCheck(fp: Digest, size: Long, isInSample: Boolean) {
    if (isInSample) {
      map.update(fp, map.getOrElse(fp, 0) + 1)
    }
    totalChunkSize += size
  }

  lazy val deduplicationRatio = calculateDeduplicationRatio()

  /**
   * Returns the deduplication ratio
   */
  private def calculateDeduplicationRatio(): Double = {
    /**
     * Calculate the number of samples to ensure the confidence in the results.
     * Formulate taken over from the research paper
     */
    def necessaryNumberOfSamples(ratio: Double): Long = {
      val a = scala.math.log(2) + scala.math.log(1.0 / 0.0001)
      val b = 2.0 * scala.math.pow(0.01, 2.0) * scala.math.pow(ratio, 2.0)

      (a / b).toLong
    }
    var weightedSampleCount: Double = 0.0
    var sampleCount: Int = 0
    var sum: Double = 0.0

    /**
     * We have there if difference to always use the faster way of calculation
     */
    if (sample.map.size < map.size) {
      for ((fp, entry) <- sample.map) {
        if (map.contains(fp)) {
          sum += (1.0 * entry.baseSampleCount * entry.chunkSize / map(fp))
          weightedSampleCount += entry.baseSampleCount * entry.chunkSize
          sampleCount += entry.baseSampleCount
        }
      }
    } else {
      for ((fp, scanCount) <- map) {
        if (sample.contains(fp)) {
          val baseSampleCount = sample.baseSampleCount(fp).get
          val chunkSize = sample.chunkSize(fp).get
          sum += (1.0 * baseSampleCount * chunkSize / scanCount)
          weightedSampleCount += baseSampleCount * chunkSize
          sampleCount += baseSampleCount
        }
      }
    }

    if (sampleCount == 0) {
      return Double.NaN
    }

    val ratio = 1.0 - (sum / weightedSampleCount)
    if (sampleCount < necessaryNumberOfSamples(ratio)) {
      logger.info("Not enought samples: estimated ratio %s, actual samples %s, necesary samples %s".format(ratio, sampleCount, necessaryNumberOfSamples(ratio)))
      logger.debug("Not enought samples: ratio %s, %s/%s, actual samples %s, necesary samples %s".format(ratio, sum, weightedSampleCount, sampleCount, necessaryNumberOfSamples(ratio)))
      Double.NaN
    } else {
      logger.debug("Ratio %s, %s/%s, actual samples %s, necesary samples %s".format(ratio, sum, weightedSampleCount, sampleCount, necessaryNumberOfSamples(ratio)))
      ratio
    }
  }

}
