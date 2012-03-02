package de.pc2.dedup.fschunk.handler

import scala.collection.mutable._
import de.pc2.dedup.util.FileSizeCategory
import de.pc2.dedup.chunker._
import de.pc2.dedup.fschunk.parse._
import java.io.BufferedWriter
import java.io.FileWriter
import scala.actors.Actor
import de.pc2.dedup.util.StorageUnit
import scalax.io._
import scalax.io.CommandLineParser
import de.pc2.dedup.util.SystemExitException
import org.apache.hadoop.fs._
import org.apache.hadoop.io._
import org.apache.hadoop.conf.Configuration
import java.net.URI
import org.apache.commons.codec.binary.Base64
import de.pc2.dedup.util.Log
import de.pc2.dedup.fschunk.format.Format
import java.io.OutputStream
import org.apache.hadoop.io.compress.BZip2Codec
import org.clapper.argot._
import de.pc2.dedup.fschunk._

/**
 * Handler to view a trace file
 */
class ViewHandler() extends Reporting with FileDataHandler with Log {
  val filePartialMap = Map.empty[String, ListBuffer[Chunk]]
  val startTime = System.currentTimeMillis()

  logger.debug("Start")

  override def report() {
  }

  def handle(fp: FilePart) {
    logger.debug("View file %s (partial)".format(fp.filename))
    if (!filePartialMap.contains(fp.filename)) {
      filePartialMap += (fp.filename -> new ListBuffer[Chunk]())
    }
    for (chunk <- fp.chunks) {
      filePartialMap(fp.filename).append(chunk)
    }
  }

  def handle(f: File) {
    logger.debug("View file %s, chunks %s".format(f.filename, f.chunks.size))

    val allFileChunks: List[Chunk] = if (filePartialMap.contains(f.filename)) {
        val partialChunks = filePartialMap(f.filename)
        filePartialMap -= f.filename
        List.concat(partialChunks.toList, f.chunks)
    } else {
        f.chunks
    }

    val msg = f.label match {
      case Some(l) => "File %s, size %s, type %s, label %s".format(f.filename, f.fileSize, f.fileType, l)
      case None => "File %s, size %s, type %s".format(f.filename, f.fileSize, f.fileType)
    }
    logger.info(msg)

    var offset = 0L
    for (chunk <- allFileChunks) {
      val msg = chunk.chunkHash match {
        case Some(ch) =>
          "Chunk %s, offset %s, size %s, chunk hash %s".format(chunk.fp, offset, chunk.size, ch)
        case None =>
          "Chunk %s, offset %s, size %s".format(chunk.fp, offset, chunk.size)
      }
      offset += chunk.size
      logger.info(msg)
    }
  }

  override def quit() {
    logger.debug("Exit")
  }
}

object View {
  def main(args: Array[String]): Unit = {
    import ArgotConverters._

    val parser = new ArgotParser("fs-c view", preUsage = Some("Version 0.3.9"))
    val optionFilenames = parser.multiOption[String](List("f", "filename"), "filenames", "Filename to parse")
    parser.parse(args)

    val format = "protobuf"

    for (filename <- optionFilenames.value) {
      val viewHandler = new ViewHandler()
      val reader = Format(format).createReader(filename, viewHandler)
      reader.parse()
      viewHandler.quit()
    }
  }
}

