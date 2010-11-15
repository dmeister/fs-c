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
class DirectoryProcessor(directory: File, useDefaultIgnores: Boolean, dispatcher: FileDispatcher) extends Runnable with Log {
	def mightBeSymlink(f: File) : Boolean = {
			val r = f.getCanonicalPath() != f.getAbsolutePath()
			if(r) {
			  logger.debug("Skip Symlink %s".format(f))
			}
			r
	}
	val ignoreSet = Set("/dev", "/proc/")
	def isDefaultIgnoreDirectory(f: File) : Boolean = {
		ignoreSet.contains(f.getCanonicalPath)
	}

	def processList(list: List[File]) {
	  def processFile(file: File) {
	    if(!file.isDirectory) {
			if(!mightBeSymlink(file)) {
				dispatcher.dispatch(file)
			}
		}
	  }
	  def processDirectory(file: File) {
		if(file.isDirectory) {  
	    if(!useDefaultIgnores || !isDefaultIgnoreDirectory(file)) {
	   		if(!mightBeSymlink(file)) {
				dispatcher.dispatch(file)
			} 	
		  }
		}
	  }
		list.foreach(processFile)
		list.foreach(processDirectory)
	}

	def run() { 
	  	if(logger.isDebugEnabled) {
		  logger.debug("Started Directory " + directory)
		}
    	DirectoryProcessor.activeCount.incrementAndGet()
		DirectoryProcessor.totalCount.incrementAndGet()
	  	try {
	  		processList(directory.listFiles().toList)
	  	} finally {
	  		DirectoryProcessor.activeCount.decrementAndGet()
	  	}
		if(logger.isDebugEnabled) {
		  logger.debug("Finished Directory " + directory)
		}
	}
}
