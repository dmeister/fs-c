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
import de.pc2.dedup.util.Log
import scala.actors.Actor 

object Main extends Log {
  def main(args: Array[String]) : Unit = {
    try {
      object Options extends CommandLineParser {
      val filename = new StringOption('f',"filename", "Filename to parse") with AllowAll   
      var chunker = new StringOption('c', "chunker", "Special File Format") with AllowAll 
      val output = new StringOption('o', "output", "Output file (optional)") with AllowAll
      val threads = new IntOption('t', "threads", "Number of concurrent threads") with AllowAll
      val silent = new Flag(None, "silent", "Reduced output") with AllowAll
      val listing = new Flag('l', "file-listing", "File contains a listing of files") with AllowAll
      val privacy = new Flag('p',"privacy", "Privacy Mode") with AllowAll
      val digestLength = new IntOption(None, "digest-length", "Length of Digest (Fingerprint)") with AllowAll
      val digestType = new StringOption(None, "digest-type", "Type of Digest (Fingerprint)") with AllowAll
      val report = new IntOption(None, "report", "Interval between progess reports in seconds (Default: 1 Minute, 0 = no report)") with AllowAll
      val format = new StringOption(None, "format", "Input/Output Format (protobuf, legacy, default: protobuf)") with AllowAll
      val noDefaultIgnores = new Flag(None, "no-default-ignores", "Avoid using the default ignore list") with AllowAll
	  val followSymlinks = new Flag(None, "follow-symlinks", "Follow symlinks") with AllowAll
      val label = new StringOption(None, "label", "File label") with AllowAll
      val progressFile = new StringOption(None, "progress-file", "File containing all processed filenames") with AllowAll
    } 
    Options.parseOrHelp(args) { cmd =>
      val chunking = try {
      val filename = cmd.all(Options.filename)
      if (filename.size == 0) {
    	  throw new MatchError("Provide at least one file via -f")
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
	  val followSymlinks = cmd(Options.followSymlinks)
      val privacyMode = cmd(Options.privacy)
      val format = cmd(Options.format) match {
        case None => "protobuf"
        case Some(s) => if(Format.isFormat(s)) {
          s
        } else {
          throw new Exception("Unsupported format")
        }
      }
      val useIgnoreList = !cmd(Options.noDefaultIgnores)
      val chunkerNames = cmd.all(Options.chunker)


    def getChunker(chunkerName: String) : (Chunker, List[Actor]) = {
        val handler = cmd(Options.output) match { 
            case None => 
                new InMemoryChunkHandler(cmd(Options.silent), new ChunkIndex, chunkerName).start() :: Nil
            case Some(o) => 
                if(o.startsWith("hdfs://")) {
                    new ImportHandler(o, o + "-" + chunkerName).start() :: Nil
                } else {
                    Format(format).createWriter(o + "-" + chunkerName, privacyMode) :: Nil
                }
            }
          val c: Chunker = chunkerName match {
            case "cdc8" => new RabinChunker(2 * 1024, 8 * 1024, 32 * 1024, new DigestFactory(digestType, digestLength), "c8")
            case "cdc4" => new RabinChunker(1 * 1024, 4 * 1024, 16 * 1024, new DigestFactory(digestType, digestLength), "c4")
            case "cdc16" => new RabinChunker(4 * 1024, 16 * 1024, 64 * 1024, new DigestFactory(digestType, digestLength), "c16")
            case "cdc32" => new RabinChunker(128 * 1024, 32 * 1024, 128 * 1024, new DigestFactory(digestType, digestLength), "c32")
            case "cdc2" => new RabinChunker(512, 2 * 1024, 8 * 1024, new DigestFactory(digestType, digestLength), "c2")

            case "fixed8" => new FixedChunker(8 * 1024, new DigestFactory(digestType, digestLength), "f8")
            case "fixed16" => new FixedChunker(16 * 1024, new DigestFactory(digestType, digestLength), "f16")
            case "fixed32" => new FixedChunker(32 * 1024, new DigestFactory(digestType, digestLength), "f32")
            case "fixed4" => new FixedChunker(4 * 1024, new DigestFactory(digestType, digestLength), "f4")
            case "fixed2" => new FixedChunker(2 * 1024, new DigestFactory(digestType, digestLength), "f2")
          }
          logger.debug("Found chunker " + chunkerName)
          (c, handler)
      }

      val chunker = if (chunkerNames.size >= 0) {  
          for {chunkerName <- chunkerNames} yield getChunker(chunkerName)
      } else {
          for {chunkerName <- List("cdc8")} yield getChunker(chunkerName)
      }
      val fileListing : FileListingProvider = if(cmd(Options.listing)) {
        FileListingProvider.fromListingFile(filename, cmd(Options.label))        
      } else {
        FileListingProvider.fromDirectFile(filename, cmd(Options.label))  
      }  
      
      val progressHandler = cmd(Options.progressFile) match {
          case Some(filename) => 
            val ph = new FileProgressHandler(filename)
            ph.progress _
          case None =>
                def dummyProgressHandler(f: de.pc2.dedup.chunker.File) : Unit = {
                    // empty
                }
                dummyProgressHandler _
      }
      
      val chunking = new FileSystemChunking(   
				fileListing,   
				chunker,     
                threads, useIgnoreList, followSymlinks, progressHandler)
      val reporter = new Reporter(chunking, reportInterval).start()
      chunking
      } catch {
        case e: MatchError =>  
          logger.error(e)
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
