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

    val parser = new ArgotParser("fs-c validate", preUsage = Some("Version 0.3.14"))
    val optionFormat = parser.option[String](List("format"), "trace file format", "Trace file format (expert)")
    val optionFilenames = parser.multiOption[String](List("f", "filename"), "filenames", "Filename to parse (deprecated)")
    val optionReport = parser.option[Int](List("r", "report"), "report", "Interval between progess reports in seconds (Default: 1 minute, 0 = no report)")
    val parameterFilenames = parser.multiParameter[String]("input filenames",
      "Input trace files files to parse",
      true) {
        (s, opt) =>
          val file = new java.io.File(s)
          if (!file.exists) {
            parser.usage("Input file \"" + s + "\" does not exist.")
          }
          s
      }
    parser.parse(args)

    val reportInterval = optionReport.value
    val format = optionFormat.value match {
      case Some(s) =>
        if (!Format.isFormat(s)) {
          parser.usage("Invalid fs-c file format")
        }
        s
      case None => "protobuf"
    }

    val filenames = if (optionFilenames.value.isEmpty && parameterFilenames.value.isEmpty) {
      parser.usage("Provide at least one trace file")
    } else if (!optionFilenames.value.isEmpty && !parameterFilenames.value.isEmpty) {
      parser.usage("Provide files by -f (deprecated) or by positional parameter, but not both")
    } else if (!optionFilenames.value.isEmpty) {
      optionFilenames.value.toList
    } else {
      parameterFilenames.value.toList
    }

    for (filename <- filenames) {
      val validateHandler = new ValidateHandler()
      val reader = Format(format).createReader(filename, validateHandler)
      val reporter = new Reporter(validateHandler, reportInterval).start()
      reader.parse()

      reporter.quit()
      validateHandler.quit()
    }
  }
}

