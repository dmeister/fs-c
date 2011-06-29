package de.pc2.dedup.fschunk.trace

import scala.actors.Actor
import scala.actors.Actor._
import java.io.File
import de.pc2.dedup.chunker._
import de.pc2.dedup.util.Log
import java.util.concurrent.atomic._

case class Directory(f: File)

object DirectoryProcessor {
    val activeCount = new AtomicLong(0L)
    val totalCount = new AtomicLong(0)
}

/**
 * Processes the directory tree.
 * If finds a file (not a directory) is forwards the file
 * to the dispatcher for chunking
 */
class DirectoryProcessor(directory: File, label: Option[String], useDefaultIgnores: Boolean, dispatcher: FileDispatcher, followSymlinks: Boolean) extends Runnable with Log {
    def mightBeSymlink(f: File) : Boolean = {
        return f.getCanonicalPath() != f.getAbsolutePath()
    }
    val ignoreSet = Set("/dev", "/proc/")
    def isDefaultIgnoreDirectory(filePath: String) : Boolean = {
        ignoreSet.contains(filePath)
    }

    def processList(list: List[File]) {
        def processFile(file: File) {
            val cp = file.getCanonicalPath()
            val ap = file.getAbsolutePath()
            if (!followSymlinks && cp != ap) {
                logger.debug("Skip symlink file %s".format(file))	
            }
            if(!file.isDirectory) {
                dispatcher.dispatch(file, cp, false, label)
            } else {
                if(!useDefaultIgnores || !isDefaultIgnoreDirectory(cp)) {
                    dispatcher.dispatch(file, cp, true, label)
                }
            }
        }
        list.foreach(processFile)
    }

    def run() { 
        if(logger.isDebugEnabled) {
            logger.debug("Started Directory " + directory)
        }
        DirectoryProcessor.activeCount.incrementAndGet()
        DirectoryProcessor.totalCount.incrementAndGet()
        try {
            var fileList = directory.listFiles();
            if (fileList != null) {
                processList(fileList.toList)
            } else {
                logger.warn("Failed to list directory " + directory)
            }
        } finally {
            DirectoryProcessor.activeCount.decrementAndGet()
        }
        if(logger.isDebugEnabled) {
            logger.debug("Finished Directory " + directory)
        }
    }
}
