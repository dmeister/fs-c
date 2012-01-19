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
import com.hazelcast.core.Hazelcast

object Main extends Log {
  def main(args: Array[String]): Unit = {
    try {
      import ArgotConverters._

      val parser = new ArgotParser("fs-c trace", preUsage = Some("Version 0.3.9"))

      val optionFilenames = parser.multiOption[String](List("f", "filename"), "filenames", "Filename to parse")
      val optionChunkerNames = parser.multiOption[String](List("c", "chunker"), "chunker", "Chunker to use")
      val optionOutput = parser.option[String](List("o", "output"), "output", "Output file (optional)")
      val optionThreads = parser.option[Int](List("t", "threads"), "threads", "number of concurrent threads")
      val optionSilent = parser.flag[Boolean](List("s", "silent"), "Reduced output")
      val optionListing = parser.flag[Boolean](List("l", "listing"), "File contains a listing of files")
      val optionPrivacy = parser.flag[Boolean](List("p", "privacy"), "Privacy Mode")
      val optionDigestLength = parser.option[Int]("digest-length", "digestLength", "Length of Digest (Fingerprint)")
      val optionDigestType = parser.option[String]("digest-type", "digestType", "Type of Digest (Fingerprint)")
      val optionCustomHandler = parser.option[String]("custom-handler", "custom chunk handler", "Full classname of a custom chunk handler")
      val optionNoDefaultIgnores = parser.flag[Boolean]("no-default-ignores", false, "Avoid using the default ignore list")
      val optionFollowSymlinks = parser.flag[Boolean]("follow-symlinks", false, "Follow symlinks")
      val optionLabel = parser.option[String]("label", "label", "File label")
      val optionProgressFile = parser.option[String]("progress-file", "progressFile", "File containing all processed filenames")
      val optionReport = parser.option[Int](List("r", "report"), "report", "Interval between progess reports in seconds (Default: 1 minute, 0 = no report)")
      val optionDistributed = parser.flag[Boolean]("cluster", false, "Distributed mode")
      parser.parse(args)

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
      val distributedMode = optionDistributed.value match {
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
            optionCustomHandler.value match {
                case Some(className) => 
                    try {
                        val customHandler = Class.forName(className).newInstance.asInstanceOf[FileDataHandler]
                        customHandler :: Nil
                    } catch {
                        case e: Exception => throw new Exception("Failed to instanciate custom chunk handler %s: %s".format(className, e))
                    }
                case None => 
                    new InMemoryChunkHandler(silent, new ChunkIndex, chunkerName) :: Nil
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
          case "cdc2" => new RabinChunker(512, 2 * 1024, 8 * 1024, new DigestFactory(digestType, digestLength), "c2")
          case "cdc4" => new RabinChunker(1 * 1024, 4 * 1024, 16 * 1024, new DigestFactory(digestType, digestLength), "c4")
          case "cdc8" => new RabinChunker(2 * 1024, 8 * 1024, 32 * 1024, new DigestFactory(digestType, digestLength), "c8")
          case "cdc16" => new RabinChunker(4 * 1024, 16 * 1024, 64 * 1024, new DigestFactory(digestType, digestLength), "c16")
          case "cdc32" => new RabinChunker(8 * 1024, 32 * 1024, 128 * 1024, new DigestFactory(digestType, digestLength), "c32")
          case "cdc64" => new RabinChunker(16 * 1024, 64 * 1024, 256 * 1024, new DigestFactory(digestType, digestLength), "c64")

          case "fixed2" => new FixedChunker(2 * 1024, new DigestFactory(digestType, digestLength), "f2")
          case "fixed4" => new FixedChunker(4 * 1024, new DigestFactory(digestType, digestLength), "f4")
          case "fixed8" => new FixedChunker(8 * 1024, new DigestFactory(digestType, digestLength), "f8")
          case "fixed16" => new FixedChunker(16 * 1024, new DigestFactory(digestType, digestLength), "f16")
          case "fixed32" => new FixedChunker(32 * 1024, new DigestFactory(digestType, digestLength), "f32")
          case "fixed64" => new FixedChunker(64 * 1024, new DigestFactory(digestType, digestLength), "f64")
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
        threadCount, useIgnoreList, followSymlinks, distributedMode,
        progressHandler)
      val reporter = new Reporter(chunking, reportInterval).start()
      chunking.start()
      reporter ! Quit
      chunking.report()
      chunking.quit()
    } catch {
      case e: SystemExitException => System.exit(1)
    }
  }
}
