package de.pc2.dedup.fschunk.trace

import de.pc2.dedup.chunker._
import de.pc2.dedup.util.Log
import de.pc2.dedup.fschunk.handler.FileDataHandler
import java.io.BufferedReader
import java.io.File
import java.io.FileNotFoundException
import java.io.FileReader
import java.io.IOException
import java.util.concurrent.ArrayBlockingQueue
import java.util.concurrent.Executors
import java.util.concurrent.ThreadPoolExecutor
import java.util.concurrent.TimeUnit
import java.lang.reflect.InvocationTargetException
import de.pc2.dedup.fschunk.Reporting
import scala.actors.Actor
import scala.actors.Actor._
import scala.actors.Exit
import com.hazelcast.core.Hazelcast

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
  val dispatcher = if (clustered) {
    new DistributedFileDispatcher(maxThreads, chunker, useDefaultIgnores, followSymlinks, useRelativePaths, useJavaDirectoryListing, progressHandler)
  } else {
    new ThreadPoolFileDispatcher(maxThreads, chunker, useDefaultIgnores, followSymlinks, useRelativePaths, useJavaDirectoryListing, progressHandler)
  }

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
    logger.debug("Finished waiting")

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
    } else {
      new File(filename)
    }
  }
}
