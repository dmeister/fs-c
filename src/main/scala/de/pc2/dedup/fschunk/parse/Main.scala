package de.pc2.dedup.fschunk.parse

import scalax.io._
import scalax.io.CommandLineParser
import de.pc2.dedup.fschunk.handler.direct._
import java.io.File
import de.pc2.dedup.fschunk._
import de.pc2.dedup.util.SystemExitException
import de.pc2.dedup.fschunk.format.Format
import org.clapper.argot._
import de.pc2.dedup.fschunk.Reporter
import de.pc2.dedup.fschunk.handler.FileDataHandler
import de.pc2.dedup.fschunk.handler.harnik.HarnikEstimationSamplingHandler
import de.pc2.dedup.fschunk.handler.harnik.HarnikEstimationScanHandler

/**
 * Main object for the parser.
 * The parser is used to replay trace of chunking runs
 */
object Main {

  def getCustomHandler(handlerName: String): FileDataHandler = {
    try {
      Class.forName(handlerName).newInstance.asInstanceOf[FileDataHandler]
    } catch {
      case ioe: ClassNotFoundException => Class.forName("de.pc2.dedup.fschunk.handler.direct." + handlerName).newInstance.asInstanceOf[FileDataHandler]
    }
  }

  /**
   * @param args the command line arguments
   */
  def main(args: Array[String]): Unit = {
    try {
      import ArgotConverters._

      val parser = new ArgotParser("fs-c parse", preUsage = Some("Version 0.3.13"))
      val optionType = parser.option[String](List("t", "type"), "type", "Handler Type (simple, ir, tr, harnik, file-stats, a custom class")
      val optionOutput = parser.option[String](List("o", "output"), "output", "Run name")
      val optionFilenames = parser.multiOption[String](List("f", "filename"), "filenames", "Filename to parse")
      val optionReport = parser.option[Int](List("r", "report"), "report", "Interval between progess reports in seconds (Default: 1 minute, 0 = no report)")
      val optionChunkerNames = parser.multiOption[String](List("c", "chunker"), "chunker", "Chunker to use")
      val optionHarnikSampleCount = parser.option[Int](List("harnik-sample-size"), "harnik sample size", "Number of sample in the harnik sample size (only harnik type)")
      parser.parse(args)

      val output: Option[String] = optionOutput.value match {
        case Some(o) => Some(o)
        case None => None
      }
      val format = "protobuf"
      val handlerType = optionType.value match {
        case Some(t) => t
        case None => "simple"
      }
      val reportInterval = optionReport.value match {
        case None => 60 * 1000
        case Some(i) => i * 1000
      }
      val chunkerNames = if (optionChunkerNames.value.size > 0) {
        for { chunkerName <- optionChunkerNames.value } yield chunkerName
      } else {
        List("cdc8")
      }
      if (optionFilenames.value.size == 0) {
        throw new Exception("Provide at least one file via -f")
      }
      val filenames = for { f <- optionFilenames.value } yield f

      handlerType match {
        case "simple" =>
          output match {
            case None =>
            case _ => throw new Exception("Output paramter is not supported by simple handler type")
          }
          val handlerList = for { c <- chunkerNames } yield new InMemoryChunkHandler(false, new ChunkIndex(), c)
          val p = new Parser(filenames(0), format, handlerList)
          val reporter = new Reporter(p, reportInterval).start()
          p.parse()
          reporter ! Quit
          for { handler <- handlerList } {
            handler.quit()
          }
        case "ir" =>
          val handlerList = for { c <- chunkerNames } yield new InternalRedundancyHandler(output, new ChunkIndex(), c)
          val p = new Parser(filenames(0), format, handlerList)
          val reporter = new Reporter(p, reportInterval).start()
          p.parse()
          reporter ! Quit
          for { handler <- handlerList } {
            handler.quit()
          }
        case "file-stats" =>
          val handlerList = for { c <- chunkerNames } yield new FileStatisticsHandler(c)

          for (filename <- filenames) {
            val p = new Parser(filename, format, handlerList)
            val reporter = new Reporter(p, reportInterval).start()
            p.parse()
            reporter ! Quit
          }

          for { handler <- handlerList } {
            handler.quit()
          }
        case "harniks" =>
          val handlerList1 = for { c <- chunkerNames } yield new HarnikEstimationSamplingHandler(optionHarnikSampleCount.value, output, c)

          for (filename <- filenames) {
            val p1 = new Parser(filename, format, handlerList1)
            val reporter1 = new Reporter(p1, reportInterval).start()
            p1.parse()
            reporter1 ! Quit
          }

          val handlerList3 = for { h <- handlerList1 } yield new HarnikEstimationScanHandler(h.estimationSample, output, h.chunkerName)

          for (filename <- filenames) {
            val p2 = new Parser(filename, format, handlerList3)
            val reporter2 = new Reporter(p2, reportInterval).start()
            p2.parse()
            reporter2 ! Quit
          }

          for { handler <- handlerList3 } {
            handler.quit()
          }

        case "tr" =>
          val handlerList1 = for { c <- chunkerNames } yield new DeduplicationHandler(new ChunkIndex(), c)
          val p1 = new Parser(filenames(0), format, handlerList1)
          val reporter1 = new Reporter(p1, reportInterval).start()
          p1.parse()
          reporter1 ! Quit

          val handlerList3 = for { h <- handlerList1 } yield new TemporalRedundancyHandler(output, h.d, h.chunkerName)
          val p2 = new Parser(filenames(1), format, handlerList3)
          val reporter2 = new Reporter(p2, reportInterval).start()
          p2.parse()
          reporter2 ! Quit

          for { handler <- handlerList3 } {
            handler.quit()
          }
        case customName =>
          val handlerList = for { c <- chunkerNames } yield getCustomHandler(customName)
          val p = new Parser(filenames(0), format, handlerList)
          val reporter = new Reporter(p, reportInterval).start()
          p.parse()
          reporter ! Quit
          for { handler <- handlerList } {
            handler.quit()
          }
      }
    } catch {
      case e: SystemExitException => System.exit(1)
    }
  }
}
