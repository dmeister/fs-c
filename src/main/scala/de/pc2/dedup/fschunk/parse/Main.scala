package de.pc2.dedup.fschunk.parse

import scala.collection.mutable.ListBuffer
import org.clapper.argot.ArgotParser
import org.clapper.argot.ArgotConverters
import de.pc2.dedup.fschunk.handler.direct.ChunkIndex
import de.pc2.dedup.fschunk.handler.direct.ChunkSizeDistributionHandler
import de.pc2.dedup.fschunk.handler.direct.DeduplicationHandler
import de.pc2.dedup.fschunk.handler.direct.FileDetailsHandler
import de.pc2.dedup.fschunk.handler.direct.FileStatisticsHandler
import de.pc2.dedup.fschunk.handler.direct.InMemoryChunkHandler
import de.pc2.dedup.fschunk.handler.direct.InternalRedundancyHandler
import de.pc2.dedup.fschunk.handler.direct.StandardReportingHandler
import de.pc2.dedup.fschunk.handler.direct.TemporalRedundancyHandler
import de.pc2.dedup.fschunk.handler.direct.ZeroChunkDeduplicationHandler
import de.pc2.dedup.fschunk.handler.harnik.HarnikEstimationSample
import de.pc2.dedup.fschunk.handler.harnik.HarnikEstimationSamplingHandler
import de.pc2.dedup.fschunk.handler.harnik.HarnikEstimationScanHandler
import de.pc2.dedup.fschunk.handler.FileDataHandler
import de.pc2.dedup.fschunk.Reporter
import de.pc2.dedup.util.SystemExitException
import de.pc2.dedup.fschunk.GCReporting
import de.pc2.dedup.fschunk.format.Format
import java.io.File
import org.clapper.argot.ArgotUsageException
import de.pc2.dedup.util.Log

/**
 * Main object for the parser.
 * The parser is used to replay trace of chunking runs
 */
object Main extends Log {

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
      val optionType = parser.multiOption[String](List("t", "type"), "type",
        "Handler Type (simple,\n\tir,\n\ttr,\n\tharnik,\n\tfile-stats,\n\tfile-details,\n\tchunk-size-stats,\n\tzero-chunk,\n\ta custom class")
      val optionOutput = parser.option[String](List("o", "output"), "output", "Run name")
      val optionFormat = parser.option[String](List("format"), "trace file format", "Trace file format (expert)")
      val optionFilenames = parser.multiOption[String](List("f", "filename"), "filenames", "Filename to parse (deprecated)")
      val optionReport = parser.option[Int](List("r", "report"), "report", "Interval between progess reports in seconds (Default: 1 minute, 0 = no report)")
      val optionHarnikSampleCount = parser.option[Int](List("harnik-sample-size"), "harnik sample size", "Number of sample in the harnik sample size (only harnik type)")
      val optionMemoryReporting = parser.flag[Boolean]("report-memory-usage", false, "Report memory usage (expert)")
      val parameterFilenames = parser.multiParameter[String]("input filenames",
                                        "Input trace files files to parse",
                                        true)  {
    	  	(s, opt) =>
    	  		val file = new File(s)
    	  		if (! file.exists) {
    	  			parser.usage("Input file \"" + s + "\" does not exist.")
      			}	
      		s
    		}
      parser.parse(args)

      val output: Option[String] = optionOutput.value match {
        case Some(o) => Some(o)
        case None => None
      }
      val format = optionFormat.value match {
        case Some(s) => 
          if (!Format.isFormat(s)) {
            parser.usage("Invalid fs-c file format")
          }
          s
        case None => "protobuf"
      }
      val handlerTypeList = if (optionType.value.size > 0) {
        for { t <- optionType.value } yield t
      } else {
        List("simple")
      }
      val reportInterval = optionReport.value
      val reportMemoryUsage = optionMemoryReporting.value match {
        case Some(b) => b
        case None => false
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

      val memoryUsageReporter = if (reportMemoryUsage) {
        Some(new Reporter(new GCReporting(), reportInterval).start())
      } else {
        None
      }
      
      if (!handlerTypeList.contains("tr")) {
        val handlerList = gatherHandlerList(handlerTypeList, optionHarnikSampleCount.value, output)
        executeParsing(handlerList, format, reportInterval, filenames)

        if (handlerTypeList.contains("harniks")) {
          // we need a second run
          val estimationSample = getHarnikEstimationHandler(handlerList)

          val handlerList2 = List(new HarnikEstimationScanHandler(estimationSample, output), new StandardReportingHandler())
          executeParsing(handlerList2, format, reportInterval, filenames)
          
          handlerList.foreach(_.quit())
          handlerList2.foreach(_.quit())
        } else {
          handlerList.foreach(_.quit())
        }
      } else {
        if (handlerTypeList.size > 1) {
          parser.usage("Illegal type configuration: tr cannot only be used alone")
        }
        if (filenames.size != 2) {
          parser.usage("tr type has to be started with two -f entries")
        }
        val handler = new DeduplicationHandler(new ChunkIndex())
        executeParsing(List(handler, new StandardReportingHandler()), format, reportInterval, List(filenames(0)))
        handler.quit()

        val handler2 = new TemporalRedundancyHandler(output, handler.d)
        executeParsing(List(handler2, new StandardReportingHandler()), format, reportInterval, List(filenames(1)))
        handler2.quit()
      }
      
      memoryUsageReporter match {
        case Some(r) => r.quit()
        case None => //pass
      }
    } catch {
      case e: ArgotUsageException => logger.error(e.message)
      case e: SystemExitException => System.exit(1)
    }
  }

  private def getHarnikEstimationHandler(handlerList: Seq[FileDataHandler]): HarnikEstimationSample = {
    for (handler <- handlerList) {
      if (handler.isInstanceOf[HarnikEstimationSamplingHandler]) {
        val samplingHandler = handler.asInstanceOf[HarnikEstimationSamplingHandler]
        return samplingHandler.estimationSample
      }
    }
    throw new Exception("Failed to find estimation sample")
  }

  private def gatherHandlerList(handlerTypeList: Seq[String], optionHarnikSampleCount: Option[Int], output: Option[String]): List[FileDataHandler] = {
    val handlerList = new ListBuffer[FileDataHandler]()
    handlerList += new StandardReportingHandler()
    for (handlerType <- handlerTypeList) {
      handlerType match {
        case "simple" =>
          output match {
            case None =>
            case _ => throw new Exception("Output paramter is not supported by simple handler type")
          }
          handlerList += new InMemoryChunkHandler(false, new ChunkIndex(), None)

        case "ir" =>
          handlerList += new InternalRedundancyHandler(output, new ChunkIndex())
        case "file-stats" =>
          handlerList += new FileStatisticsHandler()
        case "file-details" =>
          handlerList += new FileDetailsHandler()
        case "chunk-size-stats" =>
          handlerList += new ChunkSizeDistributionHandler()
        case "zero-chunk" =>
          handlerList += new ZeroChunkDeduplicationHandler()
        case "harniks" =>
          // phase 1
          handlerList += new HarnikEstimationSamplingHandler(optionHarnikSampleCount, output)
        case "tr" =>
          throw new Exception("tr needs special treatment")
        case customName =>
          handlerList += getCustomHandler(customName)
      }
    }
    handlerList.toList
  }

  private def executeParsing(handlerList: List[FileDataHandler], format: String, reportInterval: Option[Int], filenames: Seq[String]) {
    for (filename <- filenames) {
      val p = new Parser(filename, format, handlerList)
      val reporter = new Reporter(p, reportInterval).start()
      p.parse()
      reporter.quit()
    }
  }
}
