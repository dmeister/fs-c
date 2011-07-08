package de.pc2.dedup.fschunk.trace

import java.io.FileOutputStream
import java.io.OutputStreamWriter
import java.util.concurrent.locks.ReentrantLock
import java.nio.channels.FileChannel
import java.nio.channels.FileLock

/**
 * File progress handler specially developed for BSC to track all processed files
 */
class FileProgressHandler(progressFilename: String) {

  var index: Int = 0
  var fileCount: Int = 0
  var stream: FileOutputStream = null
  var writer: OutputStreamWriter = null
  var channel: FileChannel = null
  var fileLock: FileLock = null
  val processLock = new ReentrantLock()

  openNextFile()

  def close(): Unit = {
    if (fileLock != null) {
      fileLock.release()
      fileLock = null
    }
  }

  def openNextFile(): Unit = {
    if (fileLock != null) {
      fileLock.release()
      fileLock = null
    }
    if (stream != null) {
      writer.flush()
      stream.close()
      writer = null
      channel = null
    }

    stream = new FileOutputStream(progressFilename + "." + index, false)
    writer = new OutputStreamWriter(stream)
    channel = stream.getChannel()
    fileLock = channel.lock()
    index += 1
  }

  def progress(f: de.pc2.dedup.chunker.File): Unit = {
    processLock.lock()
    try {
      writer.write(f.filename)
      writer.write("\n")

      fileCount += 1
      if (fileCount >= 10000) {
        openNextFile()
        fileCount = 0
      }
    } finally {
      processLock.unlock()
    }
  }
}