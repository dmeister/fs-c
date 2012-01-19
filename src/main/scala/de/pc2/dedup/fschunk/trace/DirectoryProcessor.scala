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
import collection.JavaConversions._

case class Directory(f: File)

object JavaVersionDetector extends Log {
    var detected: Boolean = false
    var runsOnJava7Cached: Boolean = false

    def runsOnJava7(): Boolean = {
      if (!detected) {
        logger.info("Java version %s".format(System.getProperty("java.version")))
        runsOnJava7Cached = System.getProperty("java.version").contains("7")
        detected = true
      }
      return runsOnJava7Cached
    }    
}

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
object DirectoryProcessor extends Log {
  val activeCount = new AtomicLong(0L)
  val totalCount = new AtomicLong(0L)
  val skipCount = new AtomicLong(0L)

  /**
   * Portable, but slow for very large directories directory listener.
   * Calls the handler for every file
   */
  def listDirectoryPortable(directory: File, handler: (File) => Unit) : Long = {
    val files = directory.listFiles()
    if (files == null) {
        throw new Exception(directory + " is invalid or IO error occured")
    }
    files.foreach(handler)
    files.length
  }
  
  def listDirectoryJava7(directory: File, handler: (File) => Unit) : Long = {
        val dirstream = java.nio.file.Files.newDirectoryStream(directory.toPath)
        var filecount = 0
        for (path <- dirstream) {
            handler(path.toFile)
            filecount += 1
        }
        filecount
  }

  /**
   * Linux specific directory listener
   */
  def listDirectoryLinux(directory: File, handler: (File) => Unit) : Long = {
    val pb = new ProcessBuilder("ls")
    pb.directory(directory)
    val p = pb.start()
    val reader = new BufferedReader(new InputStreamReader(p.getInputStream()))

    var count = 0L
    var line = reader.readLine()
    while (line != null) {
      val file = new File(directory, line)
      handler(file)
      count += 1
      line = reader.readLine()
    }
    reader.close()
    p.destroy()
    count
  }

  lazy val directoryLister = getDirectoryLister()
  
  /**
   * returns the best matching directory listener
   */
  def getDirectoryLister(): ((File, (File) => Unit) => Long) = {
    if (JavaVersionDetector.runsOnJava7()) {
      logger.info("Using Java 7 directory listing")
        listDirectoryJava7
    } else {
        if (OSDetector.runsOnWindows()) {
        listDirectoryPortable
      } else {
        listDirectoryLinux
        }
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
    if (!followSymlinks && cp != ap) {
      DirectoryProcessor.skipCount.incrementAndGet()
    logger.debug("Skip symlink file %s".format(file))
    } else if (!file.canRead()) {
      DirectoryProcessor.skipCount.incrementAndGet()
      logger.debug("Skip unreadable file %s".format(file))
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
    val startMillis = System.currentTimeMillis()
    logger.debug("Started Directory %s".format(directory))

    // Wait until the directory processor object is inited
    DirectoryProcessor.synchronized {
      while (DirectoryProcessor.dispatcher == null) {
        logger.debug("%s: Waiting for dispatcher".format(directory))
        DirectoryProcessor.wait()
      }
    }

    DirectoryProcessor.activeCount.incrementAndGet()
    DirectoryProcessor.totalCount.incrementAndGet()
    var dirEntryCount = 0L
    try {
      dirEntryCount = DirectoryProcessor.directoryLister(directory, processFile)
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
    val endMillis = System.currentTimeMillis()
    val diffMillis = endMillis - startMillis
    logger.debug("Finished Directory %s: time %sms, %s files".format(directory, diffMillis, dirEntryCount))
  }
}
