package de.pc2.dedup.fschunk.trace

import com.hazelcast.core.Hazelcast
import java.io.File
import scala.actors.Actor
import scala.actors.Actor._
import de.pc2.dedup.chunker._
import scala.actors.Exit
import java.util.concurrent._
import de.pc2.dedup.util._
import java.util.concurrent.atomic.AtomicLong
import de.pc2.dedup.fschunk.handler.FileDataHandler
import com.hazelcast.core.DistributedTask
import com.hazelcast.core.ExecutionCallback
import java.lang.{ Integer => Int }

/**
 * Cluster version of a file dispatcher
 */
class DistributedFileDispatcher(
  processorNum: Int,
  chunker: Seq[(Chunker, List[FileDataHandler])],
  useDefaultIgnores: Boolean,
  followSymlinks: Boolean,
  useRelativePaths: Boolean,
  useJavaDirectoryListing: Boolean,
  progressHandler: (de.pc2.dedup.chunker.File) => Unit) extends FileDispatcher with Log {
  val startTime = System.currentTimeMillis()
  var lock: AnyRef = new Object()
  var finished: Boolean = false

  val activeAllCount = Hazelcast.getAtomicNumber("all count")
  val activeDirCount = Hazelcast.getAtomicNumber("dir count")
  val activeFileCount = Hazelcast.getAtomicNumber("file count")

  /**
   * id of the instance. Is used for determining a initial leader
   */
  val id = Hazelcast.getAtomicNumber("id").incrementAndGet()

  /**
   * true iff the executor service should be shut down.
   */
  def shouldShutdown: Boolean = {
    return activeAllCount.get() == 0
  }

  /**
   * Adapts the Hazelcast configuration
   */
  def adaptExecutorConfiguration() {
    val config = Hazelcast.getConfig()
    config.getExecutorConfig("file").setCorePoolSize(processorNum)
    config.getExecutorConfig("file").setMaxPoolSize(processorNum)
    config.getExecutorConfig("dir").setCorePoolSize(2)
    config.getExecutorConfig("dir").setMaxPoolSize(2)
  }

  adaptExecutorConfiguration()
  val fileexecutor = Hazelcast.getExecutorService("file")
  val direxecutor = Hazelcast.getExecutorService("dir")

  FileProcessor.init(chunker, progressHandler, useRelativePaths)
  DirectoryProcessor.init(this, useJavaDirectoryListing)

  /**
   * Human readable member id aka hostname
   */
  val memberId = Hazelcast.getCluster().getLocalMember().getInetSocketAddress().getHostName()

  override def isLeader(): Boolean = {
    return (id == 1)
  }

  /**
   * Dispatch the given file to the correct executor service
   */
  def dispatch(f: File, path: String, isDir: Boolean, source: Option[String], label: Option[String]) {
    logger.debug("Dispatch %s".format(f))

    activeAllCount.incrementAndGet()
    val runnable = if (isDir) {
      activeDirCount.incrementAndGet()
      new DirectoryProcessor(f, source, label, useDefaultIgnores, followSymlinks)
    } else {
      activeFileCount.incrementAndGet()
      new FileProcessor(f, path, source, label)
    }
    // the task type id is a) necessary for the executor callback and b) is used to determine the 
    // correct operations in the executor callback
    val taskTypeId: Int = if (isDir) {
      1
    } else {
      2
    }

    // create a distributed task instance
    val task = new DistributedTask[Int](runnable, taskTypeId);
    task.setExecutionCallback(new ExecutionCallback[Int]() {
      def done(future: Future[Int]): Unit = {
        val taskTypeId = future.get()
        if (taskTypeId == 1) {
          activeDirCount.decrementAndGet()
        } else {
          activeFileCount.decrementAndGet()
        }
        activeAllCount.decrementAndGet()
        if (shouldShutdown) {
          executorFinished()
        }
      }
    });

    if (isDir) {
      direxecutor.execute(task)
    } else {
      fileexecutor.execute(task)
    }
  }

  /**
   * Shutdown the complete hazelcast system.
   */
  override def quit() {
    Hazelcast.shutdownAll()
  }

  /**
   * Until all files and directories are processed
   */
  def waitUntilFinished() {
    lock.synchronized {
      while (!finished) {
        lock.wait()
      }
    }
    logger.debug("Shutdown")
    direxecutor.shutdown()
    fileexecutor.shutdown()
  }

  def executorFinished() {
    lock.synchronized {
      logger.debug("Finished")

      finished = true
      lock.notifyAll()

      logger.debug("Notification done")
    }
  }

  /**
   * Report the current state of the dispatcher to the user
   */
  def report() {
    val secs = ((System.currentTimeMillis() - startTime) / 1000)
    if (secs > 0) {
      val mbs = FileProcessor.totalRead.get() / secs
      logger.info("%s: Files: local %d, local data %s (%s/s), local active: %d, cluster open %d, directories: local %d, local active: %d, cluster open %d".format(
        memberId,
        FileProcessor.totalCount.get(),
        StorageUnit(FileProcessor.totalRead.get()),
        StorageUnit(mbs),
        FileProcessor.activeCount.get(),
        activeFileCount.get(),
        DirectoryProcessor.totalCount.get(),
        DirectoryProcessor.activeCount.get(),
        activeDirCount.get()))
    }
  }
}