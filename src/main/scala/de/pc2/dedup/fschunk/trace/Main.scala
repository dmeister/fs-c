package de.pc2.dedup.fschunk.trace

import org.clapper.argot.ArgotParser
import org.clapper.argot.ArgotConverters
import com.hazelcast.core.Hazelcast

import de.pc2.dedup.chunker.fixed.FixedChunker
import de.pc2.dedup.chunker.rabin.RabinChunker
import de.pc2.dedup.chunker.Chunker
import de.pc2.dedup.chunker.DigestFactory
import de.pc2.dedup.fschunk.format.Format
import de.pc2.dedup.fschunk.handler.direct.ChunkIndex
import de.pc2.dedup.fschunk.handler.direct.InMemoryChunkHandler
import de.pc2.dedup.fschunk.handler.FileDataHandler
import de.pc2.dedup.fschunk.GCReporting
import de.pc2.dedup.fschunk.Reporter
import de.pc2.dedup.util.Log
import de.pc2.dedup.util.SystemExitException

object Main extends Log {
  def main(args: Array[String]): Unit = {
    try {
      import ArgotConverters._

      val parser = new ArgotParser("fs-c trace", preUsage = Some("Version 0.3.13"))

      val optionFilenames = parser.multiOption[String](List("f", "filename"), "filenames", "Filename to trace (deprecated)")
      val optionChunkerNames = parser.multiOption[String](List("c", "chunker"), "chunker", "Chunker to use")
      val optionOutput = parser.option[String](List("o", "output"), "output", "Output file (optional)")
      val optionThreads = parser.option[Int](List("t", "threads"), "threads", "number of concurrent threads")
      val optionSilent = parser.flag[Boolean](List("s", "silent"), "Reduced output")
      val optionListing = parser.flag[Boolean](List("l", "listing"), "File contains a listing of files")
      val optionPrivacy = parser.flag[Boolean](List("p", "privacy"), "Privacy Mode")
      val optionPrivacyMode = parser.option[String](List("privacy-mode"), "privacy-mode", "Privacy mode (full-default (default), full-sha1, directory-sha1")
      val optionSalt = parser.option[String](List("salt"), "salt", "Salt the fingerprints")
      val optionDigestLength = parser.option[Int]("digest-length", "digestLength", "Length of Digest (Fingerprint)")
      val optionDigestType = parser.option[String]("digest-type", "digestType", "Type of Digest (Fingerprint)")
      val optionCustomHandler = parser.option[String]("custom-handler", "custom chunk handler", "Full classname of a custom chunk handler")
      val optionNoDefaultIgnores = parser.flag[Boolean]("no-default-ignores", false, "Avoid using the default ignore list")
      val optionFollowSymlinks = parser.flag[Boolean]("follow-symlinks", false, "Follow symlinks")
      val optionChunkHashes = parser.flag[Boolean]("log-chunker-hashes", false, "Logs chunker hashes (e.g. rabin fingerprints) and stores them in trace files")
      val optionLabel = parser.option[String]("label", "label", "File label")
      val optionProgressFile = parser.option[String]("progress-file", "progressFile", "File containing all processed filenames")
      val optionReport = parser.option[Int](List("r", "report"), "report", "Interval between progess reports in seconds (Default: 1 minute, 0 = no report)")
      val optionDistributed = parser.flag[Boolean]("cluster", false, "Distributed mode")
      val optionRelativePaths = parser.flag[Boolean]("store-relative-path", false, "Stores only relative path names (hashes only the relative pathes if privacy mode is used")
      val optionUseJavaDirectoryListing = parser.flag[Boolean]("use-java-directory-listing", false, "Uses Java buildin directory listing instead of the default method (expert)")
      val optionMemoryReporting = parser.flag[Boolean]("report-memory-usage", false, "Report memory usage (expert)")
      val parameterFilenames = parser.multiParameter[String]("input filenames",
        "Input trace files files to trace",
        true) {
          (s, opt) =>
            val file = new java.io.File(s)
            if (!file.exists) {
              parser.usage("Input file \"" + s + "\" does not exist.")
            }
            s
        }
      parser.parse(args)

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
      val reportInterval = optionReport.value
      val silent = optionSilent.value match {
        case Some(b) => b
        case None => false
      }

      val distributedMode = optionDistributed.value match {
        case Some(b) => b
        case None => false
      }
      val followSymlinks = optionFollowSymlinks.value match {
        case Some(b) => b
        case None => false
      }
      val privacyMode = optionPrivacyMode.value match {
        case None => 
          if (optionPrivacy.value.getOrElse(false)) 
            PrivacyMode.FlatDefault
          else
              PrivacyMode.NoPrivacy
        case Some(s) =>
            if (!optionPrivacy.value.getOrElse(false)) 
            parser.usage("Cannot use --privacy-mode without --privacy")
            else {
              s match {
                case "flat-default" => PrivacyMode.FlatDefault
                case "flat-sha1" => PrivacyMode.FlatSHA1
                case "directory-sha1" => PrivacyMode.DirectorySHA1
                case _ =>
                  parser.usage("Invalid privacy-mode")
              }
            }
      }
      val format = "protobuf"
      val useIgnoreList = optionNoDefaultIgnores.value match {
        case Some(b) => !b
        case None => true
      }
      val logChunkHashes = optionChunkHashes.value match {
        case Some(b) => b
        case None => false
      }
      val useRelativePaths = optionRelativePaths.value match {
        case Some(b) => b
        case None => false
      }
      val useJavaDirectoryListing = optionUseJavaDirectoryListing.value match {
        case Some(b) => b
        case None => false
      }
      val reportMemoryUsage = optionMemoryReporting.value match {
        case Some(b) => b
        case None => false
      }
      def getChunker(chunkerName: String): (Chunker, List[FileDataHandler]) = {
        val handler = optionOutput.value match {
          case None =>
            optionCustomHandler.value match {
              case Some(className) =>
                try {
                  val customHandler = Class.forName(className).newInstance.asInstanceOf[FileDataHandler]
                  customHandler :: Nil
                } catch {
                  case e: Exception => throw new Exception("Failed to instanciate custom chunk handler %s: %s".format(className, e))
                }
              case None =>
                new InMemoryChunkHandler(silent, new ChunkIndex, Some(chunkerName)) :: Nil
            }
          case Some(o) =>
            optionCustomHandler.value match {
              case Some(className) => throw new Exception("Cannot use custom chunk handler with output option")
              case None => // ook
            }
            val outputFilename = if (distributedMode) {
              val memberId = Hazelcast.getCluster().getLocalMember().getInetSocketAddress().getHostName()
              "%s-%s-%s".format(o, chunkerName, memberId)
            } else {
              "%s-%s".format(o, chunkerName)
            }
            Format(format).createWriter(outputFilename, privacyMode) :: Nil
        }

        val c: Chunker = chunkerName match {
          case "cdc2" => new RabinChunker(512, 2 * 1024, 8 * 1024, logChunkHashes, new DigestFactory(digestType, digestLength, optionSalt.value), "c2")
          case "cdc4" => new RabinChunker(1 * 1024, 4 * 1024, 16 * 1024, logChunkHashes, new DigestFactory(digestType, digestLength, optionSalt.value), "c4")
          case "cdc8" => new RabinChunker(2 * 1024, 8 * 1024, 32 * 1024, logChunkHashes, new DigestFactory(digestType, digestLength, optionSalt.value), "c8")
          case "cdc16" => new RabinChunker(4 * 1024, 16 * 1024, 64 * 1024, logChunkHashes, new DigestFactory(digestType, digestLength, optionSalt.value), "c16")
          case "cdc32" => new RabinChunker(8 * 1024, 32 * 1024, 128 * 1024, logChunkHashes, new DigestFactory(digestType, digestLength, optionSalt.value), "c32")
          case "cdc64" => new RabinChunker(16 * 1024, 64 * 1024, 256 * 1024, logChunkHashes, new DigestFactory(digestType, digestLength, optionSalt.value), "c64")

          case "fixed2" => new FixedChunker(2 * 1024, new DigestFactory(digestType, digestLength, optionSalt.value), "f2")
          case "fixed4" => new FixedChunker(4 * 1024, new DigestFactory(digestType, digestLength, optionSalt.value), "f4")
          case "fixed8" => new FixedChunker(8 * 1024, new DigestFactory(digestType, digestLength, optionSalt.value), "f8")
          case "fixed16" => new FixedChunker(16 * 1024, new DigestFactory(digestType, digestLength, optionSalt.value), "f16")
          case "fixed32" => new FixedChunker(32 * 1024, new DigestFactory(digestType, digestLength, optionSalt.value), "f32")
          case "fixed64" => new FixedChunker(64 * 1024, new DigestFactory(digestType, digestLength, optionSalt.value), "f64")
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
      val filenames = if (optionFilenames.value.isEmpty && parameterFilenames.value.isEmpty) {
        parser.usage("Provide at least one trace file")
      } else if (!optionFilenames.value.isEmpty && !parameterFilenames.value.isEmpty) {
        parser.usage("Provide files by -f (deprecated) or by positional parameter, but not both")
      } else if (!optionFilenames.value.isEmpty) {
        optionFilenames.value.toList
      } else {
        parameterFilenames.value.toList
      }
      val fileListing: FileListingProvider = if (listing) {
        FileListingProvider.fromListingFile(filenames, optionLabel.value)
      } else {
        FileListingProvider.fromDirectFile(filenames, optionLabel.value)
      }
      val progressHandler = optionProgressFile.value match {
        case Some(filename) =>
          val outputFilename = if (distributedMode) {
            val memberId = Hazelcast.getCluster().getLocalMember().getInetSocketAddress().getHostName()
            "%s-%s".format(filename, memberId)
          } else {
            filename
          }
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
        threadCount, useIgnoreList, followSymlinks, useRelativePaths, useJavaDirectoryListing, distributedMode,
        progressHandler)
      val reporter = new Reporter(chunking, reportInterval).start()

      val memoryUsageReporter = if (reportMemoryUsage) {
        Some(new Reporter(new GCReporting(), reportInterval).start())
      } else {
        None
      }

      chunking.start()
      reporter.quit()

      memoryUsageReporter match {
        case Some(r) => r.quit()
        case None => //pass
      }

      chunking.report()
      chunking.quit()
    } catch {
      case e: SystemExitException => System.exit(1)
    }
  }
}
