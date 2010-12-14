package de.pc2.dedup.fschunk.parse

import scalax.io._
import scalax.io.CommandLineParser
import de.pc2.dedup.fschunk.handler.direct._
import java.io.File
import pc2.dedup.util.SystemExitException
import de.pc2.dedup.fschunk.format.Format

/**
 * Main object for the parser.
 * The parser is used to replay trace of chunking runs
 */
object Main {

  /**
   * @param args the command line arguments
   */ 
  def main(args: Array[String]) : Unit = {
    object Options extends CommandLineParser {
      val handlerType = new StringOption('t',"type", "Handler Type") with AllowAll
      val output =  new StringOption('o',"output", "Run name") with AllowAll
      val filename =  new StringOption('f',"filename", "Trace filename") with AllowAll
      val report = new IntOption(None, "report", "Interval between progess reports in seconds (Default: 1 Minute, 0 = no report)") with AllowAll
      val format = new StringOption(None, "format", "Input/Output Format (protobuf, legacy, default: protobuf)") with AllowAll
      val chunker = new StringOption('c', "chunker", "Chunker type") with AllowAll
    } 
    try {
      Options.parseOrHelp(args) { cmd =>
        try {
          val filenames = cmd.all(Options.filename)
          val output = cmd(Options.output)
                val format = cmd(Options.format) match {
        case None => "protobuf"
        case Some(s) => if(Format.isFormat(s)) {
          s
        } else {
          throw new MatchError("Unsupported format")
        }
      }
          val handlerType = cmd(Options.handlerType) match {
            case Some(t) => t
            case None => "simple"
          }     
          val reportInterval = cmd(Options.report) match {
          	case None => 60 * 1000
          	case Some(i) => i * 1000
          }
          val chunkerNames = cmd.all(Options.chunker)
          handlerType match { 
            case "simple" => 
              val handlerList = for {c <- chunkerNames} yield new InMemoryChunkHandler(false, new ChunkIndex(), c).start()
              val p = new Parser(filenames(0), format, handlerList).start()
              val reporter = new Reporter(p, reportInterval).start()  
              p
            case "ir" => 
              val handlerList = for {c <- chunkerNames} yield new InternalRedundancyHandler(output, new ChunkIndex(), c).start()
              val p = new Parser(filenames(0), format, handlerList).start()
              val reporter = new Reporter(p, reportInterval).start()
              p
            case "tr" =>
              val handlerList1 = for {c <- chunkerNames} yield new DeduplicationHandler(new ChunkIndex(), c)
              val handlerList2 = for {h <- handlerList1} yield h.start()
              val p1 = new Parser(filenames(0), format, handlerList2).start()
              val reporter1 = new Reporter(p1, reportInterval).start()
              
              val handlerList3 = for {h <- handlerList1} yield new TemporalRedundancyHandler(output, h.d, h.chunkerName).start
              val p2 = new Parser(filenames(1),  format, handlerList2).start()
              val reporter2 = new Reporter(p1, reportInterval).start()
              p2
          }         
        } catch {
          case e: MatchError => 
            Options.showHelp(System.out)
            throw new SystemExitException()
        }
      }
    } catch {
      case e: SystemExitException => System.exit(1)
    }
  }
}
