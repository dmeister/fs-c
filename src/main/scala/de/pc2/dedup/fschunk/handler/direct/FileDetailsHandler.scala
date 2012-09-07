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
import java.nio.ByteBuffer
import de.pc2.dedup.chunker.rabin._
import com.google.common.primitives.Longs
import java.util.Arrays
import com.google.common.base.Preconditions

class FileDetail {
  var fileSizeList = new Array[Long](3)
  var fileCount: Int = 0
  var totalFileCapacity: Long = 0

  private def ensureFileSizeListCapacity() {
    fileSizeList = Longs.ensureCapacity(fileSizeList, fileCount + 16, fileCount)
  }

  def addFile(f: File) {
    ensureFileSizeListCapacity()
    fileSizeList(fileCount) = f.fileSize

    fileCount += 1
    totalFileCapacity += f.fileSize
  }

  def getCummulatedFileSizeList(l: Array[Long]): Array[Long] = {
    val cl = new Array[Long](l.length)
    if (l.length > 0) {
      cl(0) = l.head

      var i: Int = 1
      for (v <- l.tail) {
        cl(i) = cl(i - 1) + v
        i += 1
      }
    }
    cl
  }

  def getPercentile(l: Array[Long], p: Int): Long = {
    Preconditions.checkArgument(p > 0 && p < 100)

    val i = (p / 100.0 * l.length).toInt
    l(i)
  }

  def output(title: String): String = {
    fileSizeList = Arrays.copyOfRange(fileSizeList, 0, fileCount)
    Arrays.sort(fileSizeList)
    val cummulatedFileSizeList = getCummulatedFileSizeList(fileSizeList)

    val msg = new StringBuffer()
    msg.append("\n")
    msg.append(title)
    msg.append("\n")
    msg.append("\tFile Count %s (%s)\n".format(StorageUnit(fileCount), fileCount))
    msg.append("\tFile Capacity %sB (%s)\n".format(StorageUnit(totalFileCapacity), totalFileCapacity))
    msg.append("\tMean File Size %sB (%s)\n".format(StorageUnit(totalFileCapacity / fileCount), totalFileCapacity / fileCount))
    msg.append("\tMax File Size %sB (%s)\n".format(StorageUnit(fileSizeList(fileSizeList.length - 1)), fileSizeList(fileSizeList.length - 1)))
    msg.append("\tMedian File Size %sB (%s)\n".format(StorageUnit(getPercentile(fileSizeList, 50)), getPercentile(fileSizeList, 50)))
    msg.append("\tCummulated Median File Size %sB (%s)".format(StorageUnit(getPercentile(cummulatedFileSizeList, 50)), getPercentile(cummulatedFileSizeList, 50)))

    msg.toString()
  }
}

object FileDetail {
  def orderingForTypes(value: (String, FileDetail)): String = {
    value._1
  }

  def orderingForSizeCategories(value: (String, FileDetail)): Long = {
    StorageUnit.fromString(value._1)
  }
}

class FileDetailsHandler() extends FileDataHandler with Log {
  var lock: AnyRef = new Object()
  val fileTypeDetailMap = Map.empty[String, FileDetail]
  val fileSizeDetailMap = Map.empty[String, FileDetail]

  def handle(fp: FilePart) {
    // I really do not care about file parts here
  }

  def getSizeCategory(fileSize: Long): String = {
    return FileSizeCategory.getCategory(fileSize).toString()
  }

  def getFileTypeDetail(t: String): FileDetail = {
    fileTypeDetailMap.get(t) match {
      case Some(d) => d
      case None =>
        val d = new FileDetail()
        fileTypeDetailMap += (t -> d)
        d
    }
  }

  def getFileSizeDetail(s: Long): FileDetail = {
    val sz = getSizeCategory(s)
    fileSizeDetailMap.get(sz) match {
      case Some(d) => d
      case None =>
        val d = new FileDetail()
        fileSizeDetailMap += (sz -> d)
        d
    }
  }

  def handle(f: File) {
    lock.synchronized {
      getFileTypeDetail(f.fileType).addFile(f)
      getFileSizeDetail(f.fileSize).addFile(f)
    }
  }

  override def quit() {
    println("File Detail Results:\n")

    val typeDetails = fileTypeDetailMap.toList sortBy (FileDetail.orderingForTypes)
    for ((k, v) <- typeDetails) {
      println(v.output(k))
    }
    println()
    val sizeDetails = fileSizeDetailMap.toList sortBy (FileDetail.orderingForSizeCategories)
    for ((k, v) <- sizeDetails) {
      println(v.output(StorageUnit(k.toLong)))
    }
  }
}