package de.pc2.dedup.fschunk.handler.hadoop

import scala.collection.mutable._
import de.pc2.dedup.util.FileSizeCategory
import de.pc2.dedup.chunker._
import de.pc2.dedup.fschunk.parse._
import java.io.BufferedWriter
import java.io.FileWriter
import scala.actors.Actor
import de.pc2.dedup.util.StorageUnit
import java.io.Writer
import java.io.BufferedWriter
import java.io.OutputStreamWriter
import java.util.concurrent._
import java.util.concurrent.atomic._
import de.pc2.dedup.util.SystemExitException
import org.apache.hadoop.fs._
import org.apache.hadoop.io._
import org.apache.hadoop.conf.Configuration
import java.net.URI
import org.apache.commons.codec.binary.Base64
import de.pc2.dedup.util.Log
import de.pc2.dedup.fschunk.format.Format
import de.pc2.dedup.fschunk.handler.FileDataHandler
import java.io.OutputStream
import org.apache.hadoop.io.compress.BZip2Codec
import org.clapper.argot._
import de.pc2.dedup.fschunk._
import java.security.MessageDigest
import java.util.concurrent.atomic._
import java.io.FileInputStream

class FileManager(fs: FileSystem, rootPath: Path, suffix: String, compress: Boolean) extends Log {
  val globPath = new Path(rootPath, suffix + "*")
  for (fileStatus <- fs.globStatus(globPath)) {
    if (fs.exists(fileStatus.getPath())) {
      logger.warn("Overwritting " + fileStatus.getPath())
      fs.delete(fileStatus.getPath(), true)
    }
  }

  def files = ListBuffer[Writer]()
  def streams = ListBuffer[OutputStream]()
  val uniqueId = new AtomicInteger(0);
  val threadLocalFile =
    new ThreadLocal[Writer]() {
      override def initialValue(): Writer = {
        val id = uniqueId.getAndIncrement()
        val filePath = if (compress) {
          new Path(rootPath, suffix + id + ".bz2")
        } else {
          new Path(rootPath, suffix + id)
        }
        val stream = if (compress) {
          createCompressedStream(fs, filePath)
        } else {
          fs.create(filePath)
        }
        streams += stream
        val writer = new OutputStreamWriter(stream)
        files += writer
        return writer
      }
    }

  def createCompressedStream(fs: FileSystem, filepath: Path): OutputStream = {
    val codec = new BZip2Codec()
    val rawStream = fs.create(filepath)
    codec.createOutputStream(rawStream)
  }

  def localOutputWriter: Writer = {
    return threadLocalFile.get()
  }

  def quit() {
    for (file <- files) {
      file.flush()
      file.close()
    }
    for (stream <- streams) {
      stream.flush()
      stream.close()
    }
  }
}

/**
 * Handler to import a file into hadoop
 */
class ImportHandler(filesystemName: String,
  filename: String,
  threadCount: Int,
  compress: Boolean,
  withFingerprint: Boolean) extends Reporting with FileDataHandler with Log {
  val conf = new Configuration()
  val fs = FileSystem.get(new URI(filesystemName), conf)
  val rootPath = new Path(filesystemName, filename)
  val fileDataManager = new FileManager(fs, rootPath, "files", compress)

  val fileFingerprintDataManager = if (withFingerprint) {
    new FileManager(fs, rootPath, "file-fingerprint", compress)
  } else {
    null
  }
  val chunkDataManager = new FileManager(fs, rootPath, "chunks", compress)

  var totalFileSize = new AtomicLong()
  var totalChunkSize = new AtomicLong()
  var totalFileCount = new AtomicLong()
  var totalChunkCount = new AtomicLong()
  val startTime = System.currentTimeMillis()

  /* Only used with withFingerprint
  */
  val openFileMap = Map.empty[String, MessageDigest]

  logger.debug("Start")
  logger.info("Write path %s".format(rootPath))

  class ImportThreadPoolExecutor() extends ThreadPoolExecutor(1, threadCount, 30, TimeUnit.SECONDS,
    new ArrayBlockingQueue[Runnable](32),
    new ThreadPoolExecutor.CallerRunsPolicy()) {
  }
  val executor = if (threadCount > 0) {
    new ImportThreadPoolExecutor()
  } else {
    null
  }

  class FilePartRunnable(fp: FilePart) extends Runnable {
    override def run() {
      val base64 = new Base64()

      logger.debug("Write file %s (partial)".format(fp.filename))

      for (chunk <- fp.chunks) {
        if (withFingerprint) {
          openFileMap synchronized {
            getFileDigestBuilder(fp.filename).update(chunk.fp.digest)
          }
        }
        val chunkWriter = chunkDataManager.localOutputWriter
        chunkWriter.write(getChunkLine(fp.filename, chunk))

        totalChunkSize.addAndGet(chunk.size)
      }
      totalChunkCount.addAndGet(fp.chunks.size)
    }
  }

  def getChunkLine(filename: String, chunk: Chunk): String = {
    val base64 = new Base64()
    val fp: String = base64.encodeToString(chunk.fp.digest)
    val sb = new StringBuilder()
    sb.append(filename)
    sb.append('\t')
    sb.append(fp)
    sb.append('\t')
    sb.append(chunk.size)
    sb.append('\n')
    return sb.toString()
  }

  def getFileLine(f: File): String = {
    val l = f.label match {
      case Some(s) => s
      case None => "-"
    }

    val sb = new StringBuilder()
    sb.append(f.filename)
    sb.append('\t')
    sb.append(f.fileSize)
    sb.append('\t')
    sb.append(f.fileType)
    sb.append('\t')
    sb.append(l)
    sb.append('\n')
    return sb.toString()
  }
  class FileRunnable(f: File) extends Runnable {
    override def run() {
      logger.debug("Write file %s, chunks %s".format(f.filename, f.chunks.size))

      val fileWriter = fileDataManager.localOutputWriter
      fileWriter.write(getFileLine(f))

      for (chunk <- f.chunks) {
        if (withFingerprint) {
          openFileMap synchronized {
            getFileDigestBuilder(f.filename).update(chunk.fp.digest)
          }
        }
        val chunkWriter = chunkDataManager.localOutputWriter
        chunkWriter.write(getChunkLine(f.filename, chunk))
        totalChunkSize.addAndGet(chunk.size)
      }

      if (withFingerprint) {
        openFileMap synchronized {
          val base64 = new Base64()
          val fileFingerprint: String = base64.encodeToString(getFileDigestBuilder(f.filename).digest())
          val fileFingerprintLine = "%s\t%s%n".format(f.filename, fileFingerprint)

          val fileFingerpintWriter = fileFingerprintDataManager.localOutputWriter
          fileFingerpintWriter.write(fileFingerprintLine)
          openFileMap -= f.filename
        }
      }
      totalFileSize.addAndGet(f.fileSize)
      totalFileCount.addAndGet(1)
      totalChunkCount.addAndGet(f.chunks.size)
    }
  }

  override def report() {
    val secs = ((System.currentTimeMillis() - startTime) / 1000)
    if (secs > 0) {
      val mbs = totalFileSize.get() / secs
      val fps = totalFileCount.get() / secs
      logger.info("File Count: %d (%d f/s), File Size %s (%s/s), Chunk Size %s, Chunk Count: %d".format(
        totalFileCount.get(),
        fps,
        StorageUnit(totalFileSize.get()),
        StorageUnit(mbs),
        StorageUnit(totalChunkSize.get()),
        totalChunkCount.get()))
    }
  }

  def handle(fp: FilePart) {
    val r = new FilePartRunnable(fp)
    if (executor != null) {
      executor.execute(r)
    } else {
      r.run()
    }
  }

  def getFileDigestBuilder(filename: String): MessageDigest = {
    if (openFileMap.contains(filename)) {
      openFileMap(filename)
    } else {
      val md = MessageDigest.getInstance("MD5")
      openFileMap += (filename -> md)
      md
    }
  }

  def handle(f: File) {
    val r = new FileRunnable(f)
    if (executor != null) {
      executor.execute(r)
    } else {
      r.run()
    }
  }

  override def quit() {
    if (executor != null) {
      executor.shutdown()
      executor.awaitTermination(600L, TimeUnit.SECONDS)
    }

    fileDataManager.quit()
    if (fileFingerprintDataManager != null) {
      fileFingerprintDataManager.quit()
    }
    chunkDataManager.quit()
    report()
    logger.debug("Exit")

  }
}

object Import {
  def main(args: Array[String]): Unit = {
    import ArgotConverters._

    val parser = new ArgotParser("fs-c import", preUsage = Some("Version 0.3.13"))
    val optionFilenames = parser.multiOption[String](List("f", "filename"), "filenames", "Filename to parse")
    val optionReport = parser.option[Int](List("r", "report"), "report", "Interval between progress reports in seconds (Default: 1 minute, 0 = no report)")
    val optionOutput = parser.option[String](List("o", "output"), "output", "HDFS directory for output")
    val optionWithFileFingerprint = parser.flag[Boolean](List("with-file-fingerprint"), "Import with file fingerprint")
    val optionCompress = parser.flag[Boolean](List("c", "compress"), "Compress output")
    val optionThreads = parser.option[Int](List("t", "threads"), "threads", "number of concurrent threads")

    parser.parse(args)

    val reportInterval = optionReport.value
    val threadCount = optionThreads.value match {
      case Some(t) => t
      case None => 0
    }
    val format = "protobuf"
    val output = optionOutput.value match {
      case Some(o) => o
      case None => throw new Exception("--output must be specified")
    }
    val compress = optionCompress.value match {
      case Some(b) => b
      case None => false
    }
    val withFingerprint = optionWithFileFingerprint.value match {
      case Some(b) => b
      case None => false
    }
    for (filename <- optionFilenames.value) {
      val importHandler = new ImportHandler(output, output, threadCount, compress, withFingerprint)
      val stream = if (filename.startsWith("hdfs://")) {
        val conf = new Configuration()
        val fs = FileSystem.get(new URI(filename), conf)
        val path = new Path(filename)
        fs.open(path)
      } else {
        new FileInputStream(filename)
      }
      val reader = Format(format).createReader(stream, importHandler)
      val reporter = new Reporter(importHandler, reportInterval).start()
      reader.parse()

      reporter.quit()
      importHandler.quit()
    }
  }
}
