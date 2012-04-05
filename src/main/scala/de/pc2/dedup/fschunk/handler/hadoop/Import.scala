package de.pc2.dedup.fschunk.handler.hadoop

import scala.collection.mutable._
import de.pc2.dedup.util.FileSizeCategory
import de.pc2.dedup.chunker._
import de.pc2.dedup.fschunk.parse._
import java.io.BufferedWriter
import java.io.FileWriter
import scala.actors.Actor
import de.pc2.dedup.util.StorageUnit
import scalax.io._
import java.util.concurrent._
import java.util.concurrent.atomic._
import scalax.io.CommandLineParser
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

class FileManager(fs: FileSystem, rootPath: Path, suffix :String, compress: Boolean) extends Log {
  val globPath = new Path(rootPath, suffix + "*")
  for (fileStatus <- fs.globStatus(globPath)) {
    if (fs.exists(fileStatus.getPath())) {
      logger.warn("Overwritting " + fileStatus.getPath())
      fs.delete(fileStatus.getPath(), true)
    }
  }

  def files = ListBuffer[OutputStream]()
  val uniqueId = new AtomicInteger(0);
  val threadLocalFile = 
    new ThreadLocal[OutputStream]() {
      override def initialValue() : OutputStream = {
        val id = uniqueId.getAndIncrement()
        val filePath = if (compress) {
          new Path(rootPath, suffix + id + ".bz2")
        } else {
          new Path(rootPath, suffix  + id )
        }
        val writer = if (compress) {
          createCompressedStream(fs, filePath)
        } else {
          fs.create(filePath)
        }
        files += writer
        return writer
      }
    }

  def createCompressedStream(fs: FileSystem, filepath: Path): OutputStream = {
    val codec = new BZip2Codec()
    val rawStream = fs.create(filepath)
    codec.createOutputStream(rawStream)
  }

  def localOutputStream : OutputStream = {
    return threadLocalFile.get()
  }

  def quit() {
    for (file <- files) {
      file.close()
    }
  }
}

/**
 * Handler to import a file into hadoop
 */
class ImportHandler(filesystemName: String, 
    filename: String, 
    compress: Boolean,
    withFingerprint: Boolean) extends Reporting with FileDataHandler with Log {
  val conf = new Configuration()
  val fs = FileSystem.get(new URI(filesystemName), conf)
  val rootPath = new Path(filesystemName, filename)
  val fileDataManager = new FileManager(fs, rootPath, "files", compress)

  val fileFingerprintDataManager =   if (withFingerprint) {
    new FileManager(fs, rootPath, "file-fingerprint", compress)
  } else {
      null
  }
  val chunkDataManager = new FileManager(fs, rootPath, "chunks", compress)

  var totalFileSize = 0L
  var totalFileCount = 0L
  var totalChunkCount = 0L
  val startTime = System.currentTimeMillis()

  /* Only used with withFingerprint
  */
  val openFileMap = Map.empty[String, MessageDigest]

  logger.debug("Start")
  logger.info("Write path %s".format(rootPath))

  class ImportThreadPoolExecutor() extends ThreadPoolExecutor(1, 8, 30, TimeUnit.SECONDS,
    new ArrayBlockingQueue[Runnable](32),
    new ThreadPoolExecutor.CallerRunsPolicy()) {
  }
  val executor = new ImportThreadPoolExecutor()

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

        val fingerprint : String = base64.encodeToString(chunk.fp.digest)
        val chunkSize = chunk.size
        val chunkline = "%s\t%s\t%s%n".format(fp.filename, fingerprint, chunkSize)
        val msg = chunkline.getBytes("UTF-8")

        val chunkWriter = chunkDataManager.localOutputStream
        chunkWriter.write(chunkline.getBytes("UTF-8"))
      }
      totalChunkCount += fp.chunks.size
    }
  }


  class FileRunnable(f: File) extends Runnable {
    override def run() {
      val base64 = new Base64()

      logger.debug("Write file %s, chunks %s".format(f.filename, f.chunks.size))

  
      val l = f.label match {
        case Some(s) => s
        case None => "-"
      }
      val fileline = "%s\t%s\t%s\t%s%n".format(f.filename, f.fileSize, f.fileType, l)
      val fileWriter = fileDataManager.localOutputStream
      fileWriter.write(fileline.getBytes("UTF-8"))
    
      for (chunk <- f.chunks) {
        if (withFingerprint) {
          openFileMap synchronized {
            getFileDigestBuilder(f.filename).update(chunk.fp.digest)
          }
        }
        val fp : String = base64.encodeToString(chunk.fp.digest)
        val chunkSize = chunk.size
        val chunkline = "%s\t%s\t%s%n".format(f.filename, fp, chunkSize)
        val msg = chunkline.getBytes("UTF-8")
        val chunkWriter = chunkDataManager.localOutputStream
        chunkWriter.write(msg)
      }

      if (withFingerprint) {
        openFileMap synchronized {
        val fileFingerprint : String = base64.encodeToString(getFileDigestBuilder(f.filename).digest())
        val fileFingerprintLine = "%s\t%s%n".format(f.filename, fileFingerprint)

        val fileFingerpintWriter = fileFingerprintDataManager.localOutputStream
        fileFingerpintWriter.write(fileFingerprintLine.getBytes("UTF-8"))
        openFileMap -= f.filename
        }
      }
    totalFileSize += f.fileSize
    totalFileCount += 1
    totalChunkCount += f.chunks.size
    }
  }

  override def report() {
    val secs = ((System.currentTimeMillis() - startTime) / 1000)
    if (secs > 0) {
      val mbs = totalFileSize / secs
      val fps = totalFileCount / secs
      logger.info("File Count: %d (%d f/s), File Size %s (%s/s), Chunk Count: %d".format(
        totalFileCount,
        fps,
        StorageUnit(totalFileSize),
        StorageUnit(mbs),
        totalChunkCount))
    }
  }

  def handle(fp: FilePart) {
    val r = new FilePartRunnable(fp)
    executor.execute(r)
  }


  def getFileDigestBuilder(filename: String) : MessageDigest = {
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
    executor.execute(r)
  }

  override def quit() {
    executor.shutdown()

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

    val parser = new ArgotParser("fs-c import", preUsage = Some("Version 0.3.12"))
    val optionFilenames = parser.multiOption[String](List("f", "filename"), "filenames", "Filename to parse")
    val optionReport = parser.option[Int](List("r", "report"), "report", "Interval between progress reports in seconds (Default: 1 minute, 0 = no report)")
    val optionOutput = parser.option[String](List("o", "output"), "output", "HDFS directory for output")
    val optionWithFileFingerprint = parser.flag[Boolean](List("with-file-fingerprint"), "Import file fingerprint only")
    val optionCompress = parser.flag[Boolean](List("c", "compress"), "Compress output")

    parser.parse(args)

    val reportInterval = optionReport.value match {
      case None => 60 * 1000
      case Some(i) => i * 1000
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
      val importHandler = new ImportHandler(output, output, compress, withFingerprint)
      val reader = Format(format).createReader(filename, importHandler)
      val reporter = new Reporter(importHandler, reportInterval).start()
      reader.parse()

      reporter ! Quit
      importHandler.quit()
    }
  }
}
