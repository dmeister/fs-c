package de.pc2.dedup.fschunk.trace

import java.io.File
import java.io.IOException

import de.pc2.dedup.chunker.Chunker
import de.pc2.dedup.fschunk.handler.FileDataHandler
import de.pc2.dedup.fschunk.Reporting
import de.pc2.dedup.util.Log

/**
 * Main tracing class
 */
class FileSystemChunking(listing: FileListingProvider,
  chunker: Seq[(Chunker, List[FileDataHandler])],
  maxThreads: Int,
  useDefaultIgnores: Boolean,
  followSymlinks: Boolean,
  useRelativePaths: Boolean,
  useJavaDirectoryListing: Boolean,
  clustered: Boolean,
  progressHandler: (de.pc2.dedup.chunker.File) => Unit) extends Log with Reporting {

  /**
   * Dispatching object
   */
  val dispatcher = if (clustered)
    new DistributedFileDispatcher(maxThreads, chunker, useDefaultIgnores, followSymlinks, useRelativePaths, useJavaDirectoryListing, progressHandler)
  else
    new ThreadPoolFileDispatcher(maxThreads, chunker, useDefaultIgnores, followSymlinks, useRelativePaths, useJavaDirectoryListing, progressHandler)
 

  def report() {
    dispatcher.report()
    for ((_, handlers) <- chunker) {
      handlers.foreach(h => h.report())
    }
  }

  logger.debug("Start chunking")

  // Append all files from listing to directory processor
  if (dispatcher.isLeader) {
    for (fl <- listing) {
      val f = getFile(fl.filename)
      dispatcher.dispatch(f, f.getCanonicalPath(), f.isDirectory(), fl.source, fl.label)
    }
  }

  def quit() {
    dispatcher.quit()
  }

  def start() {
    dispatcher.waitUntilFinished()
    logger.info("Tracing finished")

    for ((_, handlers) <- chunker) {
      handlers.foreach(h => h.quit())
    }
  }

  private def getFile(filename: String): File = {
    if (filename.equals(".")) {
      try {
        new File(filename).getCanonicalFile()
      } catch {
        case e: IOException =>
          new File(filename)
      }
    } else new File(filename)
  }
}
