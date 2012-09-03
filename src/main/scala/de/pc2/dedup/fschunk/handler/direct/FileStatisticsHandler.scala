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

class FileStatisticsHandler(val chunkerName: String) extends FileDataHandler with Log {
  var lock: AnyRef = new Object()

  var fileCount: Int = 0
  var totalFileCapacity: Long = 0
  var fileSizeList = new Array[Long](16)

  def handle(fp: FilePart) {
    // I really do not care about file parts here
  }

  private def ensureFileSizeListCapacity() {
    fileSizeList = Longs.ensureCapacity(fileSizeList, fileCount + 16, fileCount)

  }

  def handle(f: File) {
    lock.synchronized {
      ensureFileSizeListCapacity()
      fileSizeList(fileCount) = f.fileSize

      fileCount += 1
      totalFileCapacity += f.fileSize
    }
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

  override def quit() {
    // ok, we now have the file list

    fileSizeList = Arrays.copyOfRange(fileSizeList, 0, fileCount)
    Arrays.sort(fileSizeList)
    val cummulatedFileSizeList = getCummulatedFileSizeList(fileSizeList)

    val msg = new StringBuffer()
    msg.append("\n")
    msg.append("Chunker " + chunkerName + "\n")
    msg.append("File Count %s (%s)\n".format(StorageUnit(fileCount), fileCount))
    msg.append("File Capacity %sB (%s)\n".format(StorageUnit(totalFileCapacity), totalFileCapacity))
    msg.append("Mean File Size %sB (%s)\n".format(StorageUnit(totalFileCapacity / fileCount), totalFileCapacity / fileCount))
    msg.append("Max File Size %sB (%s)\n".format(StorageUnit(fileSizeList(fileSizeList.length - 1)), fileSizeList(fileSizeList.length - 1)))

    msg.append("\nFile Size Percentilies\n")
    msg.append("10%% - %sB (%s)\n".format(StorageUnit(getPercentile(fileSizeList, 10)), getPercentile(fileSizeList, 10)))
    msg.append("25%% - %sB (%s)\n".format(StorageUnit(getPercentile(fileSizeList, 25)), getPercentile(fileSizeList, 25)))
    msg.append("50%% - %sB (%s)\n".format(StorageUnit(getPercentile(fileSizeList, 50)), getPercentile(fileSizeList, 50)))
    msg.append("75%% - %sB (%s)\n".format(StorageUnit(getPercentile(fileSizeList, 75)), getPercentile(fileSizeList, 75)))
    msg.append("90%% - %sB (%s)\n".format(StorageUnit(getPercentile(fileSizeList, 90)), getPercentile(fileSizeList, 90)))
    msg.append("\nCummulated File Size Percentilies\n")
    msg.append("10%% - %sB (%s)\n".format(StorageUnit(getPercentile(cummulatedFileSizeList, 10)), getPercentile(cummulatedFileSizeList, 10)))
    msg.append("25%% - %sB (%s)\n".format(StorageUnit(getPercentile(cummulatedFileSizeList, 25)), getPercentile(cummulatedFileSizeList, 25)))
    msg.append("50%% - %sB (%s)\n".format(StorageUnit(getPercentile(cummulatedFileSizeList, 50)), getPercentile(cummulatedFileSizeList, 50)))
    msg.append("75%% - %sB (%s)\n".format(StorageUnit(getPercentile(cummulatedFileSizeList, 75)), getPercentile(cummulatedFileSizeList, 75)))
    msg.append("90%% - %sB (%s)\n".format(StorageUnit(getPercentile(cummulatedFileSizeList, 90)), getPercentile(cummulatedFileSizeList, 90)))
    logger.info(msg)
  }

  override def report() {

  }
}