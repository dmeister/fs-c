package de.pc2.dedup.fschunk.trace

import scala.actors.Actor
import scala.actors.Actor._
import java.io.File
import de.pc2.dedup.chunker._
import de.pc2.dedup.util.Log
import java.util.concurrent.atomic._
import java.io.BufferedReader
import java.io.InputStreamReader
import java.io.IOException

case class Directory(f: File)

/**
 * Detects the Operating system
 */
object OSDetector {
  var detected: Boolean = false
  var runsOnWindowsCached: Boolean = false

  def runsOnWindows(): Boolean = {
    if (!detected) {
      runsOnWindowsCached = System.getProperty("os.name").contains("Windows")
      detected = true
    }
    return runsOnWindowsCached
  }
}

/**
 * Object for the directory processor
 */
object DirectoryProcessor {
  val activeCount = new AtomicLong(0L)
  val totalCount = new AtomicLong(0)

  /**
   * Portable, but slow for very large directories directory listener.
   * Calls the handler for every file
   */
  def listDirectoryPortable(directory: File, handler: (File) => Unit) {
    val files = directory.listFiles()
    files.foreach(handler)
  }

  /**
   * Linux specific directory listener
   */
  def listDirectoryLinux(directory: File, handler: (File) => Unit) {
    val pb = new ProcessBuilder("ls")
    pb.directory(directory)
    val p = pb.start()
    val reader = new BufferedReader(new InputStreamReader(p.getInputStream()))

    var line = reader.readLine()
    while (line != null) {
      val file = new File(directory, line)
      handler(file)
      line = reader.readLine()
    }
  }

  /**
   * returns the best matching directory listener
   */
  def directoryLister(): ((File, (File) => Unit) => Unit) = {
    if (OSDetector.runsOnWindows()) {
      return listDirectoryPortable
    } else {
      return listDirectoryLinux
    }
  }

  var dispatcher: FileDispatcher = null

  /**
   * Inits the object
   */
  def init(d: FileDispatcher) {
    synchronized {
      dispatcher = d
      this.notifyAll()
    }
  }
}

/**
 * Processes the directory tree.
 * If finds a file (not a directory) is forwards the file
 * to the dispatcher for chunking
 */
class DirectoryProcessor(directory: File,
  label: Option[String],
  useDefaultIgnores: Boolean,
  followSymlinks: Boolean) extends Runnable with Log with Serializable {
  
  /**
   * Checks if the file is a symlink
   */
  def mightBeSymlink(f: File): Boolean = {
    return f.getCanonicalPath() != f.getAbsolutePath()
  }
  val ignoreSet = Set("/dev", "/proc/")
  def isDefaultIgnoreDirectory(filePath: String): Boolean = {
    ignoreSet.contains(filePath)
  }

  /**
   * Process the file
   */
  def processFile(file: File) {
    val cp = file.getCanonicalPath()
    val ap = file.getAbsolutePath()
    logger.debug("cp %s, ap %s", cp, ap)
    if (!followSymlinks && cp != ap) {
      logger.debug("Skip symlink file %s".format(file))
    } else {
      if (!file.isDirectory) {
        DirectoryProcessor.dispatcher.dispatch(file, cp, false, label)
      } else {
        if (!useDefaultIgnores || !isDefaultIgnoreDirectory(cp)) {
          DirectoryProcessor.dispatcher.dispatch(file, cp, true, label)
        }
      }
    }
  }

  /**
   * Runs the directory processor
   */
  def run() {
    if (logger.isDebugEnabled) {
      logger.debug("Started Directory %s".format(directory))
    }

    // Wait until the directory processor object is inited
    DirectoryProcessor.synchronized {
      while (DirectoryProcessor.dispatcher == null) {
        DirectoryProcessor.wait()
      }
    }

    DirectoryProcessor.activeCount.incrementAndGet()
    DirectoryProcessor.totalCount.incrementAndGet()
    try {
      val listDirectory = DirectoryProcessor.directoryLister()
      listDirectory(directory, processFile)
    } catch {
      case ioe: IOException =>
        val e = ioe.getCause() match {
          case ioe2: IOException => ioe2
          case _ => ioe
        }
        logger.warn("Directory %s: %s".format(directory, e.getMessage()))
    } finally {
      DirectoryProcessor.activeCount.decrementAndGet()
    }
    if (logger.isDebugEnabled) {
      logger.debug("Finished Directory " + directory)
    }
  }
}
