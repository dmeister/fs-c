package de.pc2.dedup.fschunk.handler.gp

import scala.collection.mutable._
import java.io.BufferedWriter
import java.io.FileWriter
import java.io.Writer
import java.io.BufferedWriter
import java.io.OutputStreamWriter
import java.util.concurrent._
import java.util.concurrent.atomic._
import java.io.OutputStream
import org.clapper.argot._
import de.pc2.dedup.fschunk._
import java.util.concurrent.atomic._
import java.io.FileInputStream
import de.pc2.dedup.fschunk.handler.direct.StandardReportingHandler
import de.pc2.dedup.chunker.FilePart
import de.pc2.dedup.fschunk.handler.FileDataHandler
import de.pc2.dedup.chunker.File
import de.pc2.dedup.fschunk.parse.Parser
import de.pc2.dedup.fschunk.Reporter
import de.pc2.dedup.util.Log
import de.pc2.dedup.chunker.Chunk
import org.apache.commons.codec.binary.Base64

/**
 * Handler to import a file into a Greenplum database
 */
class GPImportHandler(
  output: String) extends FileDataHandler with Log {

  val fileOutputStream = new BufferedWriter(new FileWriter("%s-gp-file.csv".format(output)))
  val chunkOutputStream = new BufferedWriter(new FileWriter("%s-gp-chunk.csv".format(output)))
  val base64 = new Base64()

  private def handleChunk(filename: String, chunk: Chunk) {
    val encodedDigest = base64.encodeToString(chunk.fp.digest)
    val line = "\"%s\"|\"%s\"|%s\n".format(filename, encodedDigest, chunk.size)
    chunkOutputStream.write(line)

  }

  def handle(fp: FilePart) {
    for (chunk <- fp.chunks) {
      handleChunk(fp.filename, chunk)
    }
  }

  def handle(f: File) {
    for (chunk <- f.chunks) {
      handleChunk(f.filename, chunk)
    }

    val label = f.label match {
      case None => ""
      case Some(l) => l
    }
    val line = "\"%s\"|%s|\"%s\"|\"%s\"\n".format(f.filename, f.fileSize, f.fileType, label)
    fileOutputStream.write(line)
  }

  override def quit() {
    fileOutputStream.close()
    chunkOutputStream.close()
  }
}

object GPImport {
  def main(args: Array[String]): Unit = {
    import ArgotConverters._

    val parser = new ArgotParser("fs-c gpimport", preUsage = Some("Version 0.3.13"))
    val optionFilenames = parser.multiOption[String](List("f", "filename"), "filenames", "Filename to parse")
    val optionOutput = parser.option[String](List("o", "output"), "output", "Output file prefix")
    val optionReport = parser.option[Int](List("r", "report"), "report", "Interval between progess reports in seconds (Default: 1 minute, 0 = no report)")
    parser.parse(args)
    
    val output = optionOutput.value match {
      case Some(f) => f
      case None => throw new Exception("--output has to be configured")
    }
    val reportInterval = optionReport.value
    val format = "protobuf"
    if (optionFilenames.value.size == 0) {
      throw new Exception("Provide at least one file via -f")
    }
    val filenames = for { f <- optionFilenames.value } yield f

    val importHandler = new GPImportHandler(output)
    val handlerList = List(importHandler, new StandardReportingHandler())

    for (filename <- filenames) {
      val p = new Parser(filename, format, handlerList)
      val reporter = new Reporter(p, reportInterval).start()
      p.parse()
      reporter.quit()
    }

    for { handler <- handlerList } {
      handler.quit()
    }

  }
}
