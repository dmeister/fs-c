package de.pc2.dedup.fschunk.handler

import org.clapper.argot.ArgotParser
import org.clapper.argot.ArgotConverters

import de.pc2.dedup.chunker.File
import de.pc2.dedup.chunker.FilePart
import de.pc2.dedup.fschunk.format.Format
import de.pc2.dedup.fschunk.Reporter
import de.pc2.dedup.fschunk.Reporting
import de.pc2.dedup.util.Log
import de.pc2.dedup.util.StorageUnit

/**
 * Handler to validate a trace file
 */
class ValidateHandler() extends Reporting with FileDataHandler with Log {
  var totalFileSize = 0L
  var totalFileCount = 0L
  var totalChunkCount = 0L
  val startTime = System.currentTimeMillis()

  logger.debug("Start")

  override def report() {
    val secs = ((System.currentTimeMillis() - startTime) / 1000)
    if (secs > 0) {
      val mbs = totalFileSize / secs
      val fps = totalFileCount / secs
      logger.info("File Count: %d (%d f/s), File Size %s (%s/s), Chunk Count: %d".format(
        totalFileCount,
        fps,
        StorageUnit(totalFileSize),
        StorageUnit(mbs),
        totalChunkCount))
    }
  }

  def handle(fp: FilePart) {
    logger.debug("Validate file %s (partial)".format(fp.filename))
    totalChunkCount += fp.chunks.size
  }

  def handle(f: File) {
    logger.debug("Validate file %s, chunks %s".format(f.filename, f.chunks.size))
    totalFileSize += f.fileSize
    totalFileCount += 1
    totalChunkCount += f.chunks.size
  }

  override def quit() {
    report()
    logger.debug("Exit")

  }
}

object Validate {
  def main(args: Array[String]): Unit = {
    import ArgotConverters._

    val parser = new ArgotParser("fs-c validate", preUsage = Some("Version 0.3.13"))
    val optionFilenames = parser.multiOption[String](List("f", "filename"), "filenames", "Filename to parse")
    val optionReport = parser.option[Int](List("r", "report"), "report", "Interval between progess reports in seconds (Default: 1 minute, 0 = no report)")
    parser.parse(args)

    val reportInterval = optionReport.value
    val format = "protobuf"

    for (filename <- optionFilenames.value) {
      val validateHandler = new ValidateHandler()
      val reader = Format(format).createReader(filename, validateHandler)
      val reporter = new Reporter(validateHandler, reportInterval).start()
      reader.parse()

      reporter.quit()
      validateHandler.quit()
    }
  }
}

