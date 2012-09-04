package de.pc2.dedup.fschunk.handler.direct

import scala.collection.mutable._
import java.io.BufferedWriter
import java.io.FileWriter
import de.pc2.dedup.util.FileSizeCategory
import de.pc2.dedup.chunker._
import scala.actors.Actor
import scala.actors.Actor._
import de.pc2.dedup.util.StorageUnit
import de.pc2.dedup.util.Log
import de.pc2.dedup.fschunk.handler.FileDataHandler

class TemporalRedundancyHandler(output: Option[String], d: ChunkIndex, chunkerName: String) extends FileDataHandler with Log {
  var lock: AnyRef = new Object()
  val typeMap = Map.empty[String, (Long, Long)]
  val sizeCategoryMap = Map.empty[String, (Long, Long)]

  var currentSizeCategory: String = ""
  var currentFileType: String = ""
  var currentRealSize: Long = 0
  var currentFileSize: Long = 0

  val filePartialMap = Map.empty[String, ListBuffer[Chunk]]

  private def getSizeCategory(fileSize: Long): String = {
    return FileSizeCategory.getCategory(fileSize).toString()
  }

  typeMap.clear
  typeMap += ("ALL" -> (0L, 0L))
  sizeCategoryMap.clear
  sizeCategoryMap += ("ALL" -> (0L, 0L))

  def handle(fp: FilePart) {
    lock.synchronized {
      if (!filePartialMap.contains(fp.filename)) {
        filePartialMap += (fp.filename -> new ListBuffer[Chunk]())
      }
      for (chunk <- fp.chunks) {
        filePartialMap(fp.filename).append(chunk)
      }
    }
  }

  def handle(f: File) {
    lock.synchronized {
      var sizeCategory = getSizeCategory(f.fileSize)
      var currentRealSize = 0
      var currentFileSize = 0
      val allFileChunks = gatherAllFileChunks(f)
      for (chunk <- allFileChunks) {
        if (!d.check(chunk.fp)) {
          d.update(chunk.fp)
          currentRealSize += chunk.size;
        }
        currentFileSize += chunk.size
      }

      if (!typeMap.contains(currentFileType)) {
        typeMap += (currentFileType -> (0L, 0L))
      }
      if (!sizeCategoryMap.contains(currentSizeCategory)) {
        sizeCategoryMap += (currentSizeCategory -> (0L, 0L))
      }
      typeMap += (currentFileType -> (typeMap(currentFileType)._1 + currentRealSize, typeMap(currentFileType)._2 + currentFileSize))
      typeMap += ("ALL" -> (typeMap("ALL")._1 + currentRealSize, typeMap("ALL")._2 + currentFileSize))

      sizeCategoryMap += (currentSizeCategory -> (sizeCategoryMap(currentSizeCategory)._1 + currentRealSize, sizeCategoryMap(currentSizeCategory)._2 + currentFileSize))
      sizeCategoryMap += ("ALL" -> (sizeCategoryMap("ALL")._1 + currentRealSize, sizeCategoryMap("ALL")._2 + currentFileSize))
    }
  }

  override def quit() {
    output match {
      case Some(runName) =>
        writeMapToFile(typeMap, runName + "-" + chunkerName + "-tr-type.csv")
        writeMapToFile(sizeCategoryMap, runName + "-" + chunkerName + "-tr-size.csv")
      case None =>
        outputMapToConsole(sizeCategoryMap, "File Size Categories: %s".format(chunkerName))
    }
  }

  private def outputMapToConsole(m: Map[String, (Long, Long)], title: String) {
    val msg = new StringBuffer(title);
    msg.append("\t\tReal Size\tTotal Size\tPatch Ratio\n")
    for (k <- m.keySet) {
      val (realSize, totalSize) = m(k)
      val patchRatio = if (totalSize > 0) {
        100.0 * (realSize / totalSize)
      } else {
        100.0
      }
      msg.append("%s\t%s\t%s\t%.2f%n".format(
        k,
        StorageUnit(realSize),
        StorageUnit(totalSize),
        patchRatio))
    }
    logger.info(msg)
  }

  private def writeMapToFile(m: Map[String, (Long, Long)], f: String) {
    val w = new BufferedWriter(new FileWriter(new java.io.File(f)))
    for (k <- m.keySet) {
      val (realSize, totalSize) = m(k)
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
