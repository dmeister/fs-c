package de.pc2.dedup.fschunk.trace

import java.io.Closeable
import java.io.{ File => JavaFile }
import java.io.FileInputStream
import java.io.IOException
import java.io.InputStream
import java.io.FileNotFoundException
import java.security.MessageDigest
import java.security.NoSuchAlgorithmException

import de.pc2.dedup.chunker._
import de.pc2.dedup.chunker.Chunker
import de.pc2.dedup.chunker.File
import de.pc2.dedup.util._
import scala.actors.Actor
import scala.collection.mutable.ListBuffer
import scala.collection.mutable.Map
import scala.actors.Actor._
import de.pc2.dedup.util.StorageUnit
import java.util.concurrent.atomic._
import de.pc2.dedup.fschunk.handler.FileDataHandler
import java.nio.ByteBuffer
import java.nio.channels.FileChannel

object FileProcessor extends Log {
  val activeCount = new AtomicLong(0L)
  val totalCount = new AtomicLong(0L)
  val totalRead = new AtomicLong(0L)

  val MAX_CHUNKLIST_SIZE = 10000L
  val defaultBufferSize: Long = 4L * 1024 * 1024
  val directBufferThreshold: Long = 4L * 1024 * 1024 * 1024

  def report() {
    logger.info("IO Stats: read: %s ops, %s bytes".format(totalCount, StorageUnit(totalRead.longValue())))
  }

  var chunker: Seq[(Chunker, List[FileDataHandler])] = null
  var progressHandler: ((File) => Unit) = null

  def init(c: Seq[(Chunker, List[FileDataHandler])], p: ((File) => Unit)) {
    synchronized {
      chunker = c
      progressHandler = p
      this.notifyAll()
    }
  }

}

class FileProcessor(file: JavaFile,
  path: String,
  label: Option[String]) extends Runnable with Log with Serializable {

  def readIntoBuffer(c: FileChannel, buffer: ByteBuffer, offset: Long): Int = {
    if (logger.isDebugEnabled) {
      logger.debug("File: Read %s, offset %s".format(file, StorageUnit(offset)))
    }
    val r = c.read(buffer)
    if (logger.isDebugEnabled) {
      logger.debug("File: Read %s finished, offset %s, data size %s".format(file, StorageUnit(offset), StorageUnit(r)))
    }
    buffer.flip()
    return r
  }

  def allocateBuffer(fileLength: Long): ByteBuffer = {
    val buffer = if (fileLength > FileProcessor.directBufferThreshold) {
      ByteBuffer.allocateDirect(FileProcessor.defaultBufferSize.toInt)
    } else if (fileLength > FileProcessor.defaultBufferSize) {
      ByteBuffer.allocate(FileProcessor.defaultBufferSize.toInt)
    } else {
      ByteBuffer.allocate(fileLength.toInt)
    }
    buffer
  }

  def run() {
    FileProcessor.activeCount.incrementAndGet()
    FileProcessor.totalCount.incrementAndGet()

    FileProcessor.synchronized {
      while (FileProcessor.chunker == null || FileProcessor.progressHandler == null) {
        FileProcessor.wait()
      }
    }

    val startMillis = System.currentTimeMillis()
    var s: FileInputStream = null
    var c: FileChannel = null
    val fileLength = file.length
    if (logger.isDebugEnabled) {
      logger.debug("Started File %s (%s)".format(file, StorageUnit(fileLength)))
    }
    try {
      if (logger.isDebugEnabled) {
        logger.debug("File %s: Allocate buffer".format(file))
      }
      val buffer = allocateBuffer(fileLength)
      val sessionList = for { chunker <- FileProcessor.chunker } yield (chunker._1.createSession(), chunker._2, new ListBuffer[Chunk]())
      val t = FileType.getNormalizedFiletype(file)

      if (logger.isDebugEnabled) {
        logger.debug("File %s: Open".format(file))
      }
      s = new FileInputStream(file)
      c = s.getChannel()
      var r = readIntoBuffer(c, buffer, 0)
      var offset = 0L
      while (r > 0) {
        offset += r
        FileProcessor.totalRead.addAndGet(r)
        
        val startChunkMillis = System.currentTimeMillis()
        for ((session, handlers, chunkList) <- sessionList) {
          val chunkBuffer = buffer.slice()
          session.chunk(chunkBuffer) { chunk =>
            chunkList.append(chunk)
          }
          if (chunkList.size > FileProcessor.MAX_CHUNKLIST_SIZE) {
            val fp = new FilePart(path, chunkList.toList)
            for (handler <- handlers) {
              handler.handle(fp)
            }
            chunkList.clear()
          }
        }
        buffer.clear()
        val endChunkMillis = System.currentTimeMillis()
        if (logger.isDebugEnabled) {
          logger.debug("File %s: Chunk finished, time %sms".format(file, (endChunkMillis - startChunkMillis)))
        }
        if (offset >= fileLength) {
          r = 0
        } else {
          r = readIntoBuffer(c, buffer, offset)
        }
      }
      for ((session, handlers, chunkList) <- sessionList) {
        session.close() { chunk =>
          chunkList.append(chunk)
        }
        val f = new File(path, fileLength, t, chunkList.toList, label)
        for (handler <- handlers) {
          handler.handle(f)
        }
      }
      val fileWithoutChunks = new File(path, fileLength, t, List(), label)
      FileProcessor.progressHandler(fileWithoutChunks)
    } catch {
      case e: FileNotFoundException =>
        for ((chunker, handlers) <- FileProcessor.chunker) {
          for (handler <- handlers) {
            handler.fileError(path, fileLength)
          }
        }
        logger.debug("File %s".format(e.getMessage))
      case e: Exception =>
        logger.error("File %s: %s".format(file, e))
        for ((chunker, handlers) <- FileProcessor.chunker) {
          for (handler <- handlers) {
            handler.fileError(file.getCanonicalPath, fileLength)
          }
        }
    } finally {
      close(s)
    }
    val endMillis = System.currentTimeMillis()
    val diffMillis = endMillis - startMillis
    if (logger.isDebugEnabled) {
      logger.debug("Finished File %s: time %sms, size %s".format(file, diffMillis, StorageUnit(fileLength)))
    }
    FileProcessor.activeCount.decrementAndGet()
  }

  def close(c: Closeable) {
    if (c != null) {
      try {
        c.close()
      } catch {
        case e: IOException =>
      }
    }
  }
}
