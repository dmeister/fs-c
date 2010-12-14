package de.pc2.dedup.fschunk.trace

import java.io.File
import scala.actors.Actor
import scala.actors.Actor._
import de.pc2.dedup.chunker._
import scala.actors.Exit
import java.util.concurrent._
import de.pc2.dedup.util._
import java.util.concurrent.atomic.AtomicLong

trait FileDispatcher extends Actor {
	def dispatch(f: File, label: Option[String])
}

/**
* Dispatches files to a number of file processors that chunk the file contents
*/ 
class ThreadPoolFileDispatcher(processorNum: Int,chunker: List[(Chunker, List[Actor])], useDefaultIgnores: Boolean, followSymlinks: Boolean) extends FileDispatcher with Log {
	trapExit = true 
 	case object ExecutorFinished
  
 	def shouldShutdown : Boolean = {
 	  val dircount = direxecutor.getActiveCount() + direxecutor.getQueue().size()
 	  val filecount = fileexecutor.getActiveCount() + direxecutor.getQueue().size()
          logger.debug("Directory count %s\tfile count %s".format(dircount, filecount))
 	  return dircount + filecount == 1
 	}
  
	class DirectoryDispatcherThreadPoolExecutor(dispatcher: FileDispatcher) extends ThreadPoolExecutor(1, 4, 30, TimeUnit.SECONDS,
			new SynchronousQueue[Runnable](),
			new ThreadPoolExecutor.CallerRunsPolicy()) {
			logger.debug("Created Directory Threadpool with at most %d threads".format(4))
		override def afterExecute(r: Runnable, t: Throwable) {
			if(shouldShutdown) {
				dispatcher ! ExecutorFinished
			}
		}		  
	}
 
	class FileDispatcherThreadPoolExecutor(dispatcher: FileDispatcher) extends ThreadPoolExecutor(1, processorNum, 30, TimeUnit.SECONDS, 
			new ArrayBlockingQueue[Runnable](4096), 
			new ThreadPoolExecutor.CallerRunsPolicy()) {
		logger.debug("Created File Threadpool with at most %d threads".format(processorNum))
		override def afterExecute(r: Runnable, t: Throwable) {
			if(shouldShutdown) {
				dispatcher ! ExecutorFinished
			}
		}		
	}

	val fileexecutor = new FileDispatcherThreadPoolExecutor(this)
	val direxecutor = new DirectoryDispatcherThreadPoolExecutor(this)
 
	def dispatch(f: File, label: Option[String]) {
	  if(f.isDirectory()) {
	    direxecutor.execute(new DirectoryProcessor(f, label, useDefaultIgnores, this, followSymlinks))
	  } else {
	    fileexecutor.execute(new FileProcessor(f, label, chunker, 256 * 1024))
	  }
	}

	def report() {
		logger.debug("File (Total: %d, Data %s, Active: %d, Scheduled: %d), Directory (Total: %d, Active: %d), Queue: %d".format(
				FileProcessor.totalCount.get(),
				StorageUnit(FileProcessor.totalRead.get()),
				FileProcessor.activeCount.get(),
				fileexecutor.getQueue().size,
				DirectoryProcessor.totalCount.get(),
				DirectoryProcessor.activeCount.get(), 
				direxecutor.getQueue().size,
				mailboxSize))
	}

	def act() {
		logger.debug("Start")

		while(true) {
			receive {
                            case (f: File, label: Option[String]) =>  
			        dispatch(f, label)
			case ExecutorFinished =>
			
			    direxecutor.shutdown()
			    while(direxecutor.isTerminating()) {
				direxecutor.awaitTermination(60, TimeUnit.SECONDS)
			    }
		    	    fileexecutor.shutdown()
			    while(fileexecutor.isTerminating()) {
				fileexecutor.awaitTermination(60, TimeUnit.SECONDS)
			    }
			    report()
			    logger.debug("Exit")
			    exit()
			case Report =>
			    report()
			case msg: Any =>
			    logger.error("Unknown Message " + msg)
			} 
		}
	}
}
