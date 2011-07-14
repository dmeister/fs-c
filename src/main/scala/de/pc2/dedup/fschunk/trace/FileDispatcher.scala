package de.pc2.dedup.fschunk.trace

import java.io.File
import scala.actors.Actor
import scala.actors.Actor._
import de.pc2.dedup.chunker._
import scala.actors.Exit
import java.util.concurrent._
import de.pc2.dedup.util._
import java.util.concurrent.atomic.AtomicLong
import de.pc2.dedup.fschunk.handler.FileDataHandler
import de.pc2.dedup.fschunk.Reporting
import com.hazelcast.core.DistributedTask
import com.hazelcast.core.ExecutionCallback

/**
 * file dispatching trait
 */
trait FileDispatcher extends Reporting {
  def dispatch(f: File, path: String, isDir: Boolean, label: Option[String])

  def waitUntilFinished()

  def isLeader(): Boolean = {
    true
  }

  def quit(): Unit = {
  }
}

/**
 * Dispatches files to a number of file processors that chunk the file contents
 */
class ThreadPoolFileDispatcher(processorNum: Int,
  chunker: Seq[(Chunker, List[FileDataHandler])],
  useDefaultIgnores: Boolean,
  followSymlinks: Boolean,
  progressHandler: (de.pc2.dedup.chunker.File) => Unit) extends FileDispatcher with Log {
  var lock: AnyRef = new Object()
  var finished: Boolean = false
  val startTime = System.currentTimeMillis()

  val activeAllCount = new AtomicLong()
  val activeDirCount = new AtomicLong()
  val activeFileCount = new AtomicLong()

  def shouldShutdown(): Boolean = {
    return activeAllCount.get() == 0
  }

  class DirectoryDispatcherThreadPoolExecutor(dispatcher: ThreadPoolFileDispatcher) extends ThreadPoolExecutor(2, 2, 30, TimeUnit.SECONDS,
    new ArrayBlockingQueue[Runnable](processorNum * 2),
    new ThreadPoolExecutor.CallerRunsPolicy()) {
    logger.debug("Created directory threadpool with at most %d threads".format(4))
    override def afterExecute(r: Runnable, t: Throwable) {
      if (shouldShutdown) {
        dispatcher.executorFinished()
      }
    }
  }

  class DirectoryParentRunnable(runnable: Runnable) extends Runnable {
    override def run() {
      try {
        runnable.run()
      } finally {
        activeDirCount.decrementAndGet()
        activeAllCount.decrementAndGet()
      }
    }
  }

  class FileParentRunnable(runnable: Runnable) extends Runnable {
    override def run() {
      try {
        runnable.run()
      } finally {
        activeFileCount.decrementAndGet()
        activeAllCount.decrementAndGet()
      }
    }
  }

  class FileDispatcherThreadPoolExecutor(dispatcher: ThreadPoolFileDispatcher) extends ThreadPoolExecutor(processorNum, processorNum, 30, TimeUnit.SECONDS,
    new ArrayBlockingQueue[Runnable](processorNum * 1024),
    new ThreadPoolExecutor.CallerRunsPolicy()) {
    logger.debug("Created file threadpool with at most %d threads".format(processorNum))
    override def afterExecute(r: Runnable, t: Throwable) {
      if (shouldShutdown()) {
        dispatcher.executorFinished()
      }
    }
  }

  FileProcessor.init(chunker, progressHandler)
  DirectoryProcessor.init(this)
  val fileexecutor = new FileDispatcherThreadPoolExecutor(this)
  val direxecutor = new DirectoryDispatcherThreadPoolExecutor(this)

  def dispatch(f: File, path: String, isDir: Boolean, label: Option[String]) {
    val activeCount = activeAllCount.incrementAndGet()
    if (isDir) {
      activeDirCount.incrementAndGet()
      direxecutor.execute(
        new DirectoryParentRunnable(new DirectoryProcessor(f, label, useDefaultIgnores, followSymlinks)))
    } else {
      activeFileCount.incrementAndGet()
      fileexecutor.execute(new FileParentRunnable(new FileProcessor(f, path, label)))
    }
  }

  def waitUntilFinished() {
    lock.synchronized {
      while (!finished) {
        lock.wait()
      }
    }
  }

  def executorFinished() {
    direxecutor.shutdown()
    fileexecutor.shutdown()

    lock.synchronized {
      finished = true
      lock.notifyAll()
    }
  }

  def report() {
    val secs = ((System.currentTimeMillis() - startTime) / 1000)
    if (secs > 0) {
      val mbs = FileProcessor.totalRead.get() / secs
      logger.info("Files total: %d, data %s (%s/s), active: %d, scheduled: %d, pool %d, directories total: %d, active: %d, scheduled %d, pool %d, skipped %d".format(
        FileProcessor.totalCount.get(),
        StorageUnit(FileProcessor.totalRead.get()),
        StorageUnit(mbs),
        FileProcessor.activeCount.get(),
        activeFileCount.get(),
        fileexecutor.getPoolSize,
        DirectoryProcessor.totalCount.get(),
        DirectoryProcessor.activeCount.get(),
        activeDirCount.get(),
        direxecutor.getPoolSize,
        DirectoryProcessor.skipCount.get()))
    }
  }
}
