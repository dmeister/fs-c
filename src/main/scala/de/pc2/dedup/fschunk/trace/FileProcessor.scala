package de.pc2.dedup.fschunk.trace

import java.io.Closeable
import java.io.{File => JavaFile}
import java.io.FileInputStream
import java.io.FileNotFoundException
import java.io.IOException
import java.nio.channels.FileChannel
import java.nio.ByteBuffer
import java.util.concurrent.atomic.AtomicLong

import scala.collection.mutable.ListBuffer

import de.pc2.dedup.chunker.Chunk
import de.pc2.dedup.chunker.Chunker
import de.pc2.dedup.chunker.File
import de.pc2.dedup.chunker.FilePart
import de.pc2.dedup.fschunk.handler.FileDataHandler
import de.pc2.dedup.util.FileType
import de.pc2.dedup.util.Log
import de.pc2.dedup.util.StorageUnit

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
  var useRelativePaths: (Boolean) = false

  def init(c: Seq[(Chunker, List[FileDataHandler])], p: ((File) => Unit), useRelPath: Boolean) {
    synchronized {
      chunker = c
      progressHandler = p
      useRelativePaths = useRelPath
      this.notifyAll()
    }
  }

}

/**
 * Used to read and process a file on disk
 */
class FileProcessor(file: JavaFile,
  path: String,
  source: Option[String],
  label: Option[String]) extends Runnable with Log with Serializable {

  val sourcePath: Option[String] = source match {
    case None => None
    case Some(s) => Some(new JavaFile(s).getCanonicalPath())
  }

  private def readIntoBuffer(c: FileChannel, buffer: ByteBuffer, offset: Long): Int = {
    logger.debug("File: Read %s, offset %s".format(file, StorageUnit(offset)))
    val r = c.read(buffer)
    logger.debug("File: Read %s finished, offset %s, data size %s".format(file, StorageUnit(offset), StorageUnit(r)))
    buffer.flip()
    return r
  }

  private def allocateBuffer(fileLength: Long): ByteBuffer = {
    val buffer = if (fileLength > FileProcessor.directBufferThreshold) {
      ByteBuffer.allocateDirect(FileProcessor.defaultBufferSize.toInt)
    } else if (fileLength > FileProcessor.defaultBufferSize) {
      ByteBuffer.allocate(FileProcessor.defaultBufferSize.toInt)
    } else {
      ByteBuffer.allocate(fileLength.toInt)
    }
    buffer
  }

  private def filenameToStore(): String = {
    logger.debug("%s %s %s".format(source, sourcePath, path))
    if (FileProcessor.useRelativePaths == false) {
      path
    } else {
      sourcePath match {
        case None => path
        case Some(s) =>

          if (path.startsWith(s)) {
            path.drop(s.size + 1)
          } else {
            path
          }
      }
    }
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
    logger.debug("Started File %s (%s)".format(file, StorageUnit(fileLength)))
    try {
      logger.debug("File %s: Allocate buffer".format(file))
      val buffer = allocateBuffer(fileLength)
      val sessionList = for { chunker <- FileProcessor.chunker } yield (chunker._1.createSession(), chunker._2, new ListBuffer[Chunk]())
      val t = FileType.getNormalizedFiletype(file)

      logger.debug("File %s: Open".format(file))
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
            val fp = new FilePart(filenameToStore(), chunkList.toList)
            for (handler <- handlers) {
              handler.handle(fp)
            }
            chunkList.clear()
          }
        }
        buffer.clear()
        val endChunkMillis = System.currentTimeMillis()
        logger.debug("File %s: Chunk finished, time %sms".format(file, (endChunkMillis - startChunkMillis)))
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
        val f = new File(filenameToStore(), fileLength, t, chunkList.toList, label)
        for (handler <- handlers) {
          handler.handle(f)
        }
      }
      val fileWithoutChunks = new File(filenameToStore(), fileLength, t, List(), label)
      FileProcessor.progressHandler(fileWithoutChunks)
    } catch {
      case e: FileNotFoundException =>
        for ((chunker, handlers) <- FileProcessor.chunker) {
          for (handler <- handlers) {
            handler.fileError(filenameToStore(), fileLength)
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
    logger.debug("Finished File %s: time %sms, size %s".format(file, diffMillis, StorageUnit(fileLength)))
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
