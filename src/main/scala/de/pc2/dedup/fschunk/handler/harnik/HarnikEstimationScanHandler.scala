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

class HarnikEstimationScanHandler(val sample: HarnikEstimationSample, output: Option[String], val chunkerName: String) extends FileDataHandler with Log {
  var lock: AnyRef = new Object()
  val startTime = System.currentTimeMillis()

  val recordDetailedInformation = output match {
    case Some(s) => true
    case None => false
  }
  var totalChunkCount: Long = 0

  val estimator = new HarnikEstimationSampleCounter(sample)
  val typeMap = Map.empty[String, HarnikEstimationSampleCounter]
  val sizeCategoryMap = Map.empty[String, HarnikEstimationSampleCounter]

  val filePartialMap = Map.empty[String, ListBuffer[Chunk]]

  def getSizeCategory(fileSize: Long): String = {
    return FileSizeCategory.getCategory(fileSize).toString()
  }

  override def quit() {
    lock.synchronized {
      val totalSize = estimator.totalChunkSize
      val deduplicationRatio = estimator.deduplicationRatio
      val totalChunkSize = ((1 - deduplicationRatio) * totalSize).toLong
      val totalRedundancy = totalSize - totalChunkSize
      val msg = new StringBuffer()
      msg.append("\n")
      msg.append("Chunker %s (based on %s samples)\n".format(chunkerName, sample.totalSampleCount))
      msg.append("Total Size: " + StorageUnit(totalSize) + "\n")
      msg.append("Chunk Size: " + StorageUnit(totalChunkSize) + "\n")
      msg.append("Redundancy: " + StorageUnit(totalRedundancy))
      if (totalSize > 0) {
        msg.append(" (%.2f%%)".format(100.0 * totalRedundancy / totalSize))
      }
      msg.append("\n\nNote: A NaN entry usually indicates that it was not possible to provide an estimate with a\n")
      msg.append("confidence higher than 99%. Consider increasing the sample size by using --harnik-sample-size\n")
      logger.info(msg)
    }

    typeMap += ("ALL" -> estimator)
    sizeCategoryMap += ("ALL" -> estimator)

    output match {
      case Some("--") =>
        println()
        outputMapToConsole(typeMap, "File type categories: %s".format(chunkerName), orderingForTypes)
        println()
        outputMapToConsole(sizeCategoryMap, "File size categories: %s".format(chunkerName), orderingForSizeCategories)
      case Some(runName) =>
        writeMapToFile(typeMap, "%s-%s-ir-type.csv".format(runName, chunkerName), orderingForTypes)
        writeMapToFile(sizeCategoryMap, "%s-%s-ir-size.csv".format(runName, chunkerName), orderingForSizeCategories)
      case None =>
      // pass
    }
  }

  override def report() {
    lock.synchronized {
      val stop = System.currentTimeMillis()
      val seconds = (stop - startTime) / 1000

      val tp = if (seconds > 0) {
        "%s/s".format(StorageUnit(estimator.totalChunkSize / seconds))
      } else {
        "N/A"
      }
      logger.info("Scanning: Data size %sB (%s), chunks %s".format(StorageUnit(estimator.totalChunkSize),
        tp,
        StorageUnit(totalChunkCount)))
    }
  }

  private def getTypeEstimator(fileType: String): HarnikEstimationSampleCounter = {
    typeMap.get(fileType) match {
      case None =>
        val e = new HarnikEstimationSampleCounter(sample)
        typeMap += (fileType -> e)
        e
      case Some(e) => e
    }
  }

  private def getSizeCategoryEstimator(fileSize: Long): HarnikEstimationSampleCounter = {
    val sizeCategory = getSizeCategory(fileSize)
    sizeCategoryMap.get(sizeCategory) match {
      case None =>
        val e = new HarnikEstimationSampleCounter(sample)
        sizeCategoryMap += (sizeCategory -> e)
        e
      case Some(e) => e
    }

  }

  private def gatherAllFileChunks(f: de.pc2.dedup.chunker.File): List[de.pc2.dedup.chunker.Chunk] = {
    val allFileChunks = if (filePartialMap.contains(f.filename)) {
      val partialChunks = filePartialMap(f.filename)
      filePartialMap -= f.filename
      List.concat(partialChunks.toList, f.chunks)
    } else {
      f.chunks
    }
    allFileChunks
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
    def handleChunk(chunk: Chunk) {
      estimator.record(chunk.fp, chunk.size)
      
      if (recordDetailedInformation) {
        getTypeEstimator(f.fileType).record(chunk.fp, chunk.size)
        getSizeCategoryEstimator(f.fileSize).record(chunk.fp, chunk.size)
      }
      totalChunkCount += 1
    }

    lock.synchronized {
      val chunkList = gatherAllFileChunks(f)
      chunkList.foreach(handleChunk)
    }
  }

  private def orderingForTypes(value: (String, HarnikEstimationSampleCounter)): (Long, String) = {
    if (value._1 == "ALL") {
      return (1L, value._1)
    }
    return (0L, value._1)
  }

  private def orderingForSizeCategories(value: (String, HarnikEstimationSampleCounter)): (Long, String) = {
    if (value._1 == "ALL") {
      return (java.lang.Long.MAX_VALUE, value._1)
    }
    return (StorageUnit.fromString(value._1), value._1)
  }

  private def outputMapToConsole(m: Map[String, HarnikEstimationSampleCounter], title: String, ord: ((String, HarnikEstimationSampleCounter)) => (Long, String)) {
    def storageUnitIfPossible(k: String): String = {
      try {
        return StorageUnit(k.toLong).toString() + "B"
      } catch {
        case _ =>
        // pass
      }
      return k
    }
    println(title)
    println()
    println("%-20s %14s %14s %-8s".format("", "Real Size", "Total Size", "Dedup Ratio"))

    //  {_._1}
    val valueList = m.toList sortBy (ord)
    for ((k, v) <- valueList) {
      val deduplicationRatio = v.deduplicationRatio

      if (deduplicationRatio.isNaN()) {
        println("%-20s %15s %15s %8.2f".format(
          storageUnitIfPossible(k),
          "---",
          "---",
          Double.NaN))
      } else {
        val totalSize = v.totalChunkSize
        val realSize = (v.totalChunkSize * deduplicationRatio).toLong

        println("%-20s %14sB %14sB %8.2f%%".format(
          storageUnitIfPossible(k),
          StorageUnit(realSize),
          StorageUnit(totalSize),
          100 * deduplicationRatio))
      }
    }
  }

  private def writeMapToFile(m: Map[String, HarnikEstimationSampleCounter], f: String, ord: ((String, HarnikEstimationSampleCounter)) => (Long, String)) {
    val w = new BufferedWriter(new FileWriter(new java.io.File(f)))
    val valueList = m.toList sortBy ord
    for ((k, v) <- valueList) {
      val deduplicationRatio = v.deduplicationRatio
      val totalSize = v.totalChunkSize
      val realSize = v.totalChunkSize * deduplicationRatio

      w.write("\"" + k + "\";" + realSize + ";" + totalSize)
      w.newLine()
    }
    w.flush()
    w.close()
  }
}