package de.pc2.dedup.fschunk.handler.direct

import scala.collection.mutable._
import de.pc2.dedup.util.FileSizeCategory 
import de.pc2.dedup.chunker._
import de.pc2.dedup.fschunk.parse._
import java.io.BufferedWriter
import java.io.FileWriter
import scala.actors.Actor
import de.pc2.dedup.util.StorageUnit
 
class InternalRedundancyHandler(output: Option[String], d: ChunkIndex) extends Actor {
  trapExit = true
  val typeMap = Map.empty[String, (Long, Long)]
  val sizeCategoryMap = Map.empty[String, (Long, Long)]
   
  def getSizeCategory(fileSize: Long) : String = {
    return FileSizeCategory.getCategory(fileSize).toString()
  }
   
  def act() {
    typeMap.clear
    typeMap += ("ALL" -> (0L,0L))
    sizeCategoryMap.clear
    sizeCategoryMap += ("ALL" -> (0L,0L))
      
    while(true) {
      receive { 
        case File(filename, fileSize, fileType, chunks) =>
          println(filename)
          val sizeCategory = getSizeCategory(fileSize)
          var currentRealSize = 0 
          var currentFileSize = 0
          for(chunk <- chunks) {
            if(!d.check(chunk.fp)) { 
              d.update(chunk.fp)
              currentRealSize += chunk.size
            }
            currentFileSize += chunk.size
          }
          if(!typeMap.contains(fileType)) {
            typeMap += (fileType -> (0L,0L))
          } 
          if(!sizeCategoryMap.contains(sizeCategory)) {
            sizeCategoryMap += (sizeCategory -> (0L, 0L))
          }
          typeMap += (fileType -> (typeMap(fileType)._1 + currentRealSize, typeMap(fileType)._2 + currentFileSize))
          typeMap += ("ALL" -> (typeMap("ALL")._1 + currentRealSize, typeMap("ALL")._2 + currentFileSize))
      
          sizeCategoryMap += (sizeCategory -> (sizeCategoryMap(sizeCategory)._1 + currentRealSize, sizeCategoryMap(sizeCategory)._2 + currentFileSize))
          sizeCategoryMap += ("ALL" -> (sizeCategoryMap("ALL")._1 + currentRealSize, sizeCategoryMap("ALL")._2 + currentFileSize))
        case Quit =>
          output match {
            case Some(runName) =>
              writeMapToFile(typeMap, runName + "-ir-type.csv")
              writeMapToFile(sizeCategoryMap, runName + "-ir-size.csv")
            case None =>
              outputMapToConsole(sizeCategoryMap, "File size categories")
          }
          exit() 
      }
    }
  }
  
  def outputMapToConsole(m: Map[String,(Long,Long)],title: String) {
    println(title)
    println()
    println("\tReal Size\tTotal Size\tDedup Ratio")
    for(k <- m.keySet) {
      val (realSize, totalSize) = m(k)
      val dedupRatio = if(totalSize > 0) {
        100.0 * (1.0 - realSize / totalSize)
      } else {
        0.0
      }
      println("%s\t%s\t%s\t%.2f".format(
        StorageUnit(k.toLong),
        StorageUnit(realSize),
        StorageUnit(totalSize),
        dedupRatio
      ))
    }
  }
  
  def writeMapToFile(m: Map[String,(Long,Long)], f: String) {
    val w = new BufferedWriter(new FileWriter(new java.io.File(f)))
    for(k <- m.keySet) {
      val (realSize, totalSize) = m(k)
      w.write("\"" + k + "\";" + realSize + ";" + totalSize)
      w.newLine()
    }
    w.flush()
    w.close()
  }
}
