package de.pc2.dedup.fschunk.handler.harnik

import java.io.BufferedWriter
import java.io.FileWriter

import scala.collection.mutable.ListBuffer
import scala.collection.mutable.Map

import de.pc2.dedup.chunker.Chunk
import de.pc2.dedup.chunker.File
import de.pc2.dedup.chunker.FilePart
import de.pc2.dedup.fschunk.handler.FileDataHandler
import de.pc2.dedup.util.FileSizeCategory
import de.pc2.dedup.util.Log
import de.pc2.dedup.util.StorageUnit

object HarnikEstimationScanHandler {
  def outputTemporalScanResult(sample: HarnikEstimationSample, 
    totalEstimator: HarnikEstimationSampleCounter, 
    firstGenerationEstimator: HarnikEstimationSampleCounter) {
    
      val secondTotalSize = totalEstimator.totalChunkSize - firstGenerationEstimator.totalChunkSize
      if (totalEstimator.deduplicationRatio.isNaN() || firstGenerationEstimator.deduplicationRatio.isNaN()) {
        outputScanResult(sample, secondTotalSize, Double.NaN)
      } else {
      val totalChunkSize = totalEstimator.totalChunkSize * totalEstimator.deduplicationRatio
      val firstGenChunkSize = firstGenerationEstimator.totalChunkSize * firstGenerationEstimator.deduplicationRatio
      val secondGenChunkSize = totalChunkSize - firstGenChunkSize
      val secondGenDeduplicationRatio = secondGenChunkSize.toDouble / secondTotalSize.toDouble
      outputScanResult(sample, secondTotalSize, secondGenDeduplicationRatio)
    }
  }

  def outputScanResult(sample: HarnikEstimationSample, totalSize: Long, deduplicationRatio: Double) {
      val msg = new StringBuffer()
      msg.append("\n")
      msg.append("Harnik's Estimation Results: (based on %s samples)\n".format(sample.totalSampleCount))
      if (deduplicationRatio.isNaN()) {
    	  msg.append("Total Size: " + StorageUnit(totalSize) + "\n")
    	  msg.append("Chunk Size: NaN\n")
    	  msg.append("Redundancy: NaN (NaN)")
      } else {
    	  val totalChunkSize = ((1 - deduplicationRatio) * totalSize).toLong	  
    	  val totalRedundancy = totalSize - totalChunkSize
    	  msg.append("Total Size: " + StorageUnit(totalSize) + "\n")
    	  msg.append("Chunk Size: " + StorageUnit(totalChunkSize) + "\n")
    	  msg.append("Redundancy: " + StorageUnit(totalRedundancy))
    	  msg.append(" (%.2f%%)".format(100.0 * deduplicationRatio))
      }
      println(msg)
    }

  def outputNaNWarning() {
      val msg = new StringBuffer()
      msg.append("\n\nNote: A NaN entry usually indicates that it was not possible to provide an estimate with a\n")
      msg.append("confidence higher than 99%. Consider increasing the sample size by using --harnik-sample-size\n")
      println(msg)
  }
}

class HarnikEstimationScanHandler(val sample: HarnikEstimationSample, output: Option[String]) extends FileDataHandler with Log {
  var lock: AnyRef = new Object()

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
      
      HarnikEstimationScanHandler.outputScanResult(sample, totalSize, deduplicationRatio)
      HarnikEstimationScanHandler.outputNaNWarning()
    }

    typeMap += ("ALL" -> estimator)
    sizeCategoryMap += ("ALL" -> estimator)

    output match {
      case Some("--") =>
        println()
        outputMapToConsole(typeMap, "File type categories:", orderingForTypes)
        println()
        outputMapToConsole(sizeCategoryMap, "File size categories:", orderingForSizeCategories)
      case Some(runName) =>
        writeMapToFile(typeMap, "%s-ir-type.csv".format(runName), orderingForTypes)
        writeMapToFile(sizeCategoryMap, "%s-ir-size.csv".format(runName), orderingForSizeCategories)
      case None =>
      // pass
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

  private def gatherAllFileChunks(f: de.pc2.dedup.chunker.File): scala.collection.Seq[Chunk] = {
    val allFileChunks = if (filePartialMap.contains(f.filename)) {
      val partialChunks = filePartialMap(f.filename)
      filePartialMap -= f.filename
      List.concat(partialChunks, f.chunks)
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
