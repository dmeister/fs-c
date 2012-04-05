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
    lock.synchronized {
      for (chunk <- fp.chunks) {
        chunkSizeMap.add(chunk.size)

        // By defintiion, a file part from a large file
        largeFileChunkSizeMap.add(chunk.size)
      }
    }
  }

  def handle(f: File) {
    lock.synchronized {
      for (chunk <- f.chunks) {
        chunkSizeMap.add(chunk.size)

        if (f.fileSize > mb) {
          largeFileChunkSizeMap.add(chunk.size)
        }
      }
    }
  }

  override def quit() {
    outputMapToConsole(chunkSizeMap, orderingBySize)
  }
  
  def orderingBySize( value : (Int, java.lang.Long) ) : (Int) = {
    return value._1
  }
  
  def outputMapToConsole(m: Map[Int, java.lang.Long], ord: ((Int, java.lang.Long)) => (Int)) {
    println()
    //  {_._1}
    val valueList = m.toList sortBy(ord)
    for ( (chunkSize, count) <- valueList) {
      println("%d\t%d\t%d".format(
        chunkSize,
        count,
        largeFileChunkSizeMap.get(chunkSize)))
    }
  }
}