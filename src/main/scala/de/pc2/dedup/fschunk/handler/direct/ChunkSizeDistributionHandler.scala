package de.pc2.dedup.fschunk.handler.direct

import de.pc2.dedup.chunker._
import java.util.concurrent.atomic.AtomicLong
import java.util.concurrent.ConcurrentHashMap
import scala.collection.mutable.ListBuffer
import de.pc2.dedup.util.StorageUnit
import scala.actors.Actor
import scala.actors.Actor._
import de.pc2.dedup.util.Log
import scala.collection.mutable.Map
import de.pc2.dedup.fschunk.handler.FileDataHandler
import de.pc2.dedup.util.udf.CountingMap
import collection.JavaConversions._

/**
 * A file data handler used to build statistics about the chunk size distribtuion
 */
class ChunkSizeDistributionHandler() extends FileDataHandler with Log {
  var lock: AnyRef = new Object()
  val chunkSizeMap = new CountingMap[Int]()
  val largeFileChunkSizeMap = new CountingMap[Int]()
  val mb = 1024 * 1024

  override def report() {
    lock.synchronized {
    }
  }

  def handle(fp: FilePart) {
    def addChunkToMap(c: Chunk) {
      chunkSizeMap.add(c.size)
      largeFileChunkSizeMap.add(c.size)
    }

    lock.synchronized {
      fp.chunks.foreach(addChunkToMap)
    }
  }

  def handle(f: File) {
    def addChunkToMap(c: Chunk) {
      chunkSizeMap.add(c.size)

      if (f.fileSize > mb) {
        largeFileChunkSizeMap.add(c.size)
      }
    }
    lock.synchronized {
      f.chunks.foreach(addChunkToMap)
    }
  }

  override def quit() {
    outputMapToConsole(chunkSizeMap, orderingBySize)
  }

  def orderingBySize(value: (Int, java.lang.Long)): (Int) = {
    return value._1
  }

  def outputMapToConsole(m: Map[Int, java.lang.Long], ord: ((Int, java.lang.Long)) => (Int)) {
    println()
    //  {_._1}
    val valueList = m.toList sortBy (ord)
    for ((chunkSize, count) <- valueList) {
      println("%d\t%d\t%d".format(
        chunkSize,
        count,
        largeFileChunkSizeMap.get(chunkSize)))
    }
  }
}