package de.pc2.dedup.fschunk.handler.harnik

import de.pc2.dedup.chunker.Digest
import scala.collection.Map
import scala.collection.mutable.{ Map => MutableMap }

case class HarnikSamplingEntry(val baseSampleCount: Int) {

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
}

class HarnikEstimationSampleCounter(val sample: HarnikEstimationSample) {
  val map = MutableMap.empty[Digest, Int]
  var totalChunkSize: Long = 0

  def record(fp: Digest, size: Long): Boolean = {
    val r = if (sample.contains(fp)) {
      if (map.contains(fp)) {
        map.update(fp, map(fp) + 1)
      } else {
        map += (fp -> 1)
      }
      true
    } else {
      false
    }

    totalChunkSize += size
    r
  }

  def recordNoCheck(fp: Digest, size: Long, isInSample: Boolean) {
    if (isInSample) {
      if (map.contains(fp)) {
        map.update(fp, map(fp) + 1)
      } else {
        map += (fp -> 1)
      }
    }

    totalChunkSize += size
  }

  def deduplicationRatio(): Double = {
    var sampleCount: Int = 0
    var sum: Double = 0.0

    if (sample.map.size < map.size) {
      for ((fp, entry) <- sample.map) {
        if (map.contains(fp)) {
          sum += (1.0 * entry.baseSampleCount / map(fp))
          sampleCount += entry.baseSampleCount
        }
      }
    } else {
      for ((fp, scanCount) <- map) {
        if (sample.contains(fp)) {
          val baseSampleCount = sample.baseSampleCount(fp).get
          sum += (1.0 * baseSampleCount / scanCount)
          sampleCount += baseSampleCount
        }
      }
    }

    if (sampleCount == 0) {
      return Double.NaN
    }

    return 1.0 - (sum / sampleCount)
  }

}
