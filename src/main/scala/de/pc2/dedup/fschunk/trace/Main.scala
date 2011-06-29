package de.pc2.dedup.fschunk.trace

import org.clapper.argot._
import de.pc2.dedup.chunker._
import de.pc2.dedup.chunker.rabin._
import de.pc2.dedup.chunker.fixed._
import de.pc2.dedup.util.SystemExitException
import de.pc2.dedup.fschunk.handler.direct.InMemoryChunkHandler
import de.pc2.dedup.fschunk.handler.direct.ChunkIndex
import de.pc2.dedup.fschunk.format.Format
import de.pc2.dedup.fschunk.handler.hadoop._
import de.pc2.dedup.fschunk.handler.FileDataHandler
import de.pc2.dedup.util.Log
import de.pc2.dedup.fschunk._
import scala.actors.Actor

object Main extends Log {
  def main(args: Array[String]): Unit = {
    try {
      import ArgotConverters._
      
      val parser = new ArgotParser("fs-c trace", preUsage = Some("Version 3.5.0"))

      val optionFilenames = parser.multiOption[String](List("f", "filename"), "filenames", "Filename to parse")
      val optionChunkerNames = parser.multiOption[String](List("c", "chunker"), "chunker", "Chunker to use")
      val optionOutput = parser.option[String](List("o", "output"), "output", "Output file (optional)")
      val optionThreads = parser.option[Int](List("t", "threads"), "threads", "number of concurrent threads")
      val optionSilent = parser.flag[Boolean](List("s", "silent"), "Reduced output")
      val optionListing = parser.flag[Boolean](List("l", "listing"), "File contains a listing of files")
      val optionPrivacy = parser.flag[Boolean](List("p", "privacy"), "Privacy Mode")
      val optionDigestLength = parser.option[Int]("digest-length", "digestLength", "Length of Digest (Fingerprint)")
      val optionDigestType = parser.option[String]("digest-type", "digestType", "Type of Digest (Fingerprint)")
      val optionNoDefaultIgnores = parser.flag[Boolean]("no-default-ignores", false, "Avoid using the default ignore list")
      val optionFollowSymlinks = parser.flag[Boolean]("follow-symlinks", false, "Follow symlinks")
      val optionLabel = parser.option[String]("label", "label", "File label")
      val optionProgressFile = parser.option[String]("progress-file", "progressFile", "File containing all processed filenames")
      val optionReport = parser.option[Int](List("r", "report"), "report", "Interval between progess reports in seconds (Default: 1 minute, 0 = no report)")
      
      if (optionFilenames.value.size == 0) {
          throw new Exception("Provide at least one file via -f")
      }
      val threadCount = optionThreads.value match {
          case Some(t) => t
          case None => 1
      }
      val digestLength = optionDigestLength.value match {
          case Some(l) => l
          case None => 20
      }
      val digestType = optionDigestType.value match {
          case Some(t) => t
          case None => "SHA-1"
      }
      val reportInterval = optionReport.value match {
        case None => 60 * 1000
        case Some(i) => i * 1000
      }
      val silent = optionSilent.value match {
          case Some(b) => b
          case None => false
      }
      val privacyMode = optionPrivacy.value match {
          case Some(b) => b
          case None => false
      }
      val followSymlinks = optionFollowSymlinks.value match {
          case Some(b) => b
          case None => false
      }
      val format = "protobuf"
      val useIgnoreList = optionNoDefaultIgnores.value match {
          case Some(b) => !b
          case None => true
      }
      
      def getChunker(chunkerName: String): (Chunker, List[FileDataHandler]) = {
        val handler = optionOutput.value match {
          case None =>
            new InMemoryChunkHandler(silent, new ChunkIndex, chunkerName) :: Nil
          case Some(o) =>
            Format(format).createWriter(o + "-" + chunkerName, privacyMode) :: Nil
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

      val chunker = if (optionChunkerNames.value.size > 0) {
        for { chunkerName <- optionChunkerNames.value } yield getChunker(chunkerName)
      } else {
        for { chunkerName <- List("cdc8") } yield getChunker(chunkerName)
      }
      val listing = optionListing.value match {
          case Some(b) => b
          case None => false
      }
      val fileListing: FileListingProvider = if (listing) {
        FileListingProvider.fromListingFile(optionFilenames.value, optionLabel.value)
      } else {
        FileListingProvider.fromDirectFile(optionFilenames.value, optionLabel.value)
      }  
      val progressHandler = optionProgressFile.value match {
          case Some(filename) =>
            val ph = new FileProgressHandler(filename)
            ph.progress _
          case None =>
            def dummyProgressHandler(f: de.pc2.dedup.chunker.File): Unit = {
              // empty
            }
            dummyProgressHandler _
        }
      
      val chunking = new FileSystemChunking(
        fileListing,
        chunker,
        threadCount, useIgnoreList, followSymlinks, progressHandler)
      val reporter = new Reporter(chunking, reportInterval).start()
      chunking.start()
      reporter ! Quit
      chunking.report()

      /*object Options extends CommandLineParser {
        val filename = new StringOption('f', "filename", "Filename to parse") with AllowAll
        var chunker = new StringOption('c', "chunker", "Special File Format") with AllowAll
        val output = new StringOption('o', "output", "Output file (optional)") with AllowAll
        val threads = new IntOption('t', "threads", "Number of concurrent threads") with AllowAll
        val silent = new Flag(None, "silent", "Reduced output") with AllowAll
        val listing = new Flag('l', "file-listing", "File contains a listing of files") with AllowAll
        val privacy = new Flag('p', "privacy", "Privacy Mode") with AllowAll
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
            case Some(s) => if (Format.isFormat(s)) {
              s
            } else {
              throw new Exception("Unsupported format")
            }
          }
          val useIgnoreList = !cmd(Options.noDefaultIgnores)
          val chunkerNames = cmd.all(Options.chunker)

          def getChunker(chunkerName: String): (Chunker, List[FileDataHandler]) = {
            val handler = cmd(Options.output) match {
              case None =>
                new InMemoryChunkHandler(cmd(Options.silent), new ChunkIndex, chunkerName) :: Nil
              case Some(o) =>
                Format(format).createWriter(o + "-" + chunkerName, privacyMode) :: Nil
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

          val chunker = if (chunkerNames.size > 0) {
            for { chunkerName <- chunkerNames } yield getChunker(chunkerName)
          } else {
            for { chunkerName <- List("cdc8") } yield getChunker(chunkerName)
          }
          val fileListing: FileListingProvider = if (cmd(Options.listing)) {
            FileListingProvider.fromListingFile(filename, cmd(Options.label))
          } else {
            FileListingProvider.fromDirectFile(filename, cmd(Options.label))
          }

          val progressHandler = cmd(Options.progressFile) match {
            case Some(filename) =>
              val ph = new FileProgressHandler(filename)
              ph.progress _
            case None =>
              def dummyProgressHandler(f: de.pc2.dedup.chunker.File): Unit = {
                // empty
              }
              dummyProgressHandler _
          }

          val chunking = new FileSystemChunking(
            fileListing,
            chunker,
            threads, useIgnoreList, followSymlinks, progressHandler)
          val reporter = new Reporter(chunking, reportInterval).start()
          chunking.start()
          reporter ! Quit
          chunking
        } catch {
          case e: MatchError =>
            logger.error(e)
            Options.showHelp(System.out)
            throw new SystemExitException()
        }
        chunking.report()
      }*/
    } catch {
      case e: SystemExitException => System.exit(1)
    }
  }
}
