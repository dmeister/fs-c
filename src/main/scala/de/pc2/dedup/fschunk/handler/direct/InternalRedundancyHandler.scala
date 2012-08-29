package de.pc2.dedup.fschunk.handler.direct

import scala.collection.mutable._
import de.pc2.dedup.util.FileSizeCategory
import de.pc2.dedup.chunker._
import de.pc2.dedup.fschunk.parse._
import java.io.BufferedWriter
import java.io.FileWriter
import scala.actors.Actor
import de.pc2.dedup.util.StorageUnit
import de.pc2.dedup.fschunk.handler.FileDataHandler
import de.pc2.dedup.util.Log
import scala.math.Ordering

class InternalRedundancyHandler(output: Option[String], d: ChunkIndex, chunkerName: String) extends FileDataHandler with Log {
  var lock: AnyRef = new Object()
  val typeMap = Map.empty[String, (Long, Long)]
  val sizeCategoryMap = Map.empty[String, (Long, Long)]

  val filePartialMap = Map.empty[String, ListBuffer[Chunk]]

  def getSizeCategory(fileSize: Long): String = {
    return FileSizeCategory.getCategory(fileSize).toString()
  }

  typeMap.clear
  typeMap += ("ALL" -> (0L, 0L))
  sizeCategoryMap.clear
  sizeCategoryMap += ("ALL" -> (0L, 0L))

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
      logger.debug("Handle file %s".format(f.filename))
      val sizeCategory = getSizeCategory(f.fileSize)
      var currentRealSize = 0
      var currentFileSize = 0
      val allFileChunks = gatherAllFileChunks(f)
      for (chunk <- allFileChunks) {
        if (!d.check(chunk.fp)) {
          d.update(chunk.fp)
          currentRealSize += chunk.size
        }
        currentFileSize += chunk.size
      }
      
      if (!typeMap.contains(f.fileType)) {
        typeMap += (f.fileType -> (0L, 0L))
      }
      if (!sizeCategoryMap.contains(sizeCategory)) {
        sizeCategoryMap += (sizeCategory -> (0L, 0L))
      }
      typeMap += (f.fileType -> (typeMap(f.fileType)._1 + currentRealSize, typeMap(f.fileType)._2 + currentFileSize))
      typeMap += ("ALL" -> (typeMap("ALL")._1 + currentRealSize, typeMap("ALL")._2 + currentFileSize))

      sizeCategoryMap += (sizeCategory -> (sizeCategoryMap(sizeCategory)._1 + currentRealSize, sizeCategoryMap(sizeCategory)._2 + currentFileSize))
      sizeCategoryMap += ("ALL" -> (sizeCategoryMap("ALL")._1 + currentRealSize, sizeCategoryMap("ALL")._2 + currentFileSize))

    }
  }

  override def quit() {
    output match {
      case Some(runName) =>
        writeMapToFile(typeMap, "%s-%s-ir-type.csv".format(runName, chunkerName), orderingForTypes)
        writeMapToFile(sizeCategoryMap, "%s-%s-ir-size.csv".format(runName, chunkerName), orderingForSizeCategories)
      case None =>
        outputMapToConsole(typeMap, "File type categories: %s".format(chunkerName), orderingForTypes)
        println()
        outputMapToConsole(sizeCategoryMap, "File size categories: %s".format(chunkerName), orderingForSizeCategories)
    }
  }

  private def orderingForTypes(value: (String, (Long, Long))): (Long, String) = {
    if (value._1 == "ALL") {
      return (1L, value._1)
    }
    return (0L, value._1)
  }

  private def orderingForSizeCategories(value: (String, (Long, Long))): (Long, String) = {
    if (value._1 == "ALL") {
      return (java.lang.Long.MAX_VALUE, value._1)
    }
    return (StorageUnit.fromString(value._1), value._1)
  }

  private def outputMapToConsole(m: Map[String, (Long, Long)], title: String, ord: ((String, (Long, Long))) => (Long, String)) {
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
      val (realSize, totalSize) = v
      val dedupRatio = if (totalSize > 0) {
        100.0 * (1.0 - (1.0 * realSize / totalSize))
      } else {
        0.0
      }
      println("%-20s %14sB %14sB %8.2f%%".format(
        storageUnitIfPossible(k),
        StorageUnit(realSize),
        StorageUnit(totalSize),
        dedupRatio))
    }
  }

  private def writeMapToFile(m: Map[String, (Long, Long)], f: String, ord: ((String, (Long, Long))) => (Long, String)) {
    val w = new BufferedWriter(new FileWriter(new java.io.File(f)))
    val valueList = m.toList sortBy ord
    for ((k, v) <- valueList) {
      val (realSize, totalSize) = v
      w.write("\"" + k + "\";" + realSize + ";" + totalSize)
      w.newLine()
    }
    w.flush()
    w.close()
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
}
