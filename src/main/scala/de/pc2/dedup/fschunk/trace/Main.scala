package de.pc2.dedup.fschunk.trace

import scalax.io._
import scalax.io.CommandLineParser
import de.pc2.dedup.chunker._
import de.pc2.dedup.chunker.rabin._ 
import de.pc2.dedup.chunker.fixed._
import de.pc2.dedup.util.SystemExitException
import de.pc2.dedup.fschunk.handler.direct.InMemoryChunkHandler
import de.pc2.dedup.fschunk.handler.direct.ChunkIndex
import de.pc2.dedup.fschunk.format.Format
import de.pc2.dedup.fschunk.handler.hadoop._

object Main {
  def main(args: Array[String]) : Unit = {
    try {
      object Options extends CommandLineParser {
      val filename = new StringOption('f',"filename", "Filename to parse") with AllowAll   
      val minSize = new IntOption(None, "min-size", "Minimal chunk size") with AllowAll
      val maxSize = new IntOption(None, "max-size", "Maximal chunk size") with AllowAll
      val avgSize = new IntOption('s', "avg-size", "Average chunk size") with AllowAll
      var chunker = new StringOption('c', "chunker", "Special File Format") with AllowAll 
      val output = new StringOption('o', "output", "Output file (optional)") with AllowAll
      val threads = new IntOption('t', "threads", "Number of concurrent threads") with AllowAll
      val silent = new Flag(None, "silent", "Reduced output") with AllowAll
      val listing = new Flag('l', "file-listing", "File contains a listing of files") with AllowAll
      val privacy = new Flag('p',"privacy", "Privacy Mode") with AllowAll
      val digestLength = new IntOption(None, "digest-length", "Length of Digest (Fingerprint") with AllowAll
      val digestType = new StringOption(None, "digest-type", "Type of Digest (Fingerprint") with AllowAll
      val report = new IntOption(None, "report", "Interval between progess reports in seconds (Default: 1 Minute, 0 = no report)") with AllowAll
      val format = new StringOption(None, "format", "Input/Output Format (protobuf, legacy, default: protobuf)") with AllowAll
      val noDefaultIgnores = new Flag(None, "--no-default-ignores", "Avoid using the default ignore list") with AllowAll
    } 
    Options.parseOrHelp(args) { cmd =>
      val chunking = try {
      val filename = cmd.all(Options.filename)
      val minSize = cmd(Options.minSize) match {
        case None => 2 * 1024 
        case Some(i) => i
      }
      val maxSize = cmd(Options.maxSize) match {
        case None => 32 * 1024
        case Some(i) => i
      }
      val avgSize = cmd(Options.avgSize) match {
        case None => 8 * 1024
        case Some(i) => i
      }
      val threads = cmd(Options.threads) match {
        case None => 1 
        case Some(i) => i
      }
      val digestLength = cmd(Options.digestLength) match {
    	  case None => 20
    	  case Some(i) => i
      }
      val digestType = cmd(Options.digestType) match {
    	  case None => "SHA-1"
    	  case Some(s) => s
      }
      val reportInterval = cmd(Options.report) match {
    	  case None => 60 * 1000
    	  case Some(i) => i * 1000
      }
      val privacyMode = cmd(Options.privacy)
      val format = cmd(Options.format) match {
        case None => "protobuf"
        case Some(s) => if(Format.isFormat(s)) {
          s
        } else {
          throw new MatchError("Unsupported format")
        }
      }
      val useIgnoreList = !cmd(Options.noDefaultIgnores)
      val chunker = cmd(Options.chunker) match {
        case None => new RabinChunker(minSize, avgSize, maxSize, new DigestFactory(digestType, digestLength))
        case Some("rabin") => new RabinChunker(minSize, avgSize, maxSize, new DigestFactory(digestType, digestLength))
        case Some("cdc") => new RabinChunker(minSize, avgSize, maxSize, new DigestFactory(digestType, digestLength))
        case Some("fixed") => new FixedChunker(avgSize, new DigestFactory(digestType, digestLength))
        case Some("static") => new FixedChunker(avgSize, new DigestFactory(digestType, digestLength))
      }
      val fileListing : FileListingProvider = if(cmd(Options.listing)) {
        FileListingProvider.fromListingFile(filename)        
      } else {
        FileListingProvider.fromDirectFile(filename)  
      }  
      val handler = cmd(Options.output) match { 
        case None => 
          val d = new ChunkIndex()
          new InMemoryChunkHandler(cmd(Options.silent), d).start :: Nil
        case Some(o) => 
          if(o.startsWith("hdfs://")) {
            new ImportHandler(o,o).start() :: Nil
          } else {
        	  Format(format).createWriter(o, privacyMode) :: Nil
          }
      }
      
      val chunking = new FileSystemChunking(   
				fileListing,   
				chunker,     
				handler,   
                threads, useIgnoreList)
      val reporter = new Reporter(chunking, reportInterval).start()
      chunking
      } catch {
        case e: MatchError =>  
          Options.showHelp(System.out)
          throw new SystemExitException()
      }
      chunking.start()
    } 
    } catch {
      case e: SystemExitException => System.exit(1)
    } 
  } 
} 