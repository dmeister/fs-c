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

/**
 * Handler to import a file into hadoop
 */
class ImportHandler(filesystemName: String, 
    filename: String, 
    compress: Boolean,
    fileFingerprintOnly: Boolean) extends Reporting with FileDataHandler with Log {
  def createCompressedStream(fs: FileSystem, filepath: Path): OutputStream = {
    val codec = new BZip2Codec()
    val rawStream = fs.create(filepath)
    codec.createOutputStream(rawStream)
  }
  val conf = new Configuration()
  val fs = FileSystem.get(new URI(filesystemName), conf)

  val rootPath = new Path(filesystemName, filename)
  val chunkPath = if (compress) {
    new Path(rootPath, "chunks.bz2")
  } else {
    new Path(rootPath, "chunks")
  }
  val filePath = if (compress) {
    new Path(rootPath, "files.bz2")
  } else {
    new Path(rootPath, "files")
  }
  val fileFingerprintPath = if (compress) {
    new Path(rootPath, "file-fingerprint.bz2")
  } else {
    new Path(rootPath, "file-fingerprint")
  }

  val base64 = new Base64()
  var totalFileSize = 0L
  var totalFileCount = 0L
  var totalChunkCount = 0L
  val startTime = System.currentTimeMillis()

  val openFileMap = Map.empty[String, MessageDigest]

  logger.debug("Start")
  logger.info("Write path %s".format(rootPath))

  if (!fileFingerprintOnly) {
    if (fs.exists(filePath)) {
      logger.warn("Overwritting " + filePath)
      fs.delete(filePath, true)
    }
    if (fs.exists(chunkPath)) {
      logger.warn("Overwritting " + chunkPath)
      fs.delete(chunkPath, true)
    }
  }
  if (fs.exists(fileFingerprintPath)) {
    logger.warn("Overwritting " + fileFingerprintPath)
    fs.delete(fileFingerprintPath, true)
  }


  val fileWriter = if (fileFingerprintOnly) {
    null
  } else if (compress) {
    createCompressedStream(fs, filePath)
  } else {
    fs.create(filePath)
  }
  val fileFingerpintWriter = if (compress) {
    createCompressedStream(fs, fileFingerprintPath)
  } else {
    fs.create(fileFingerprintPath)
  }
  val chunkWriter = if (fileFingerprintOnly) {
    null
  } else if (compress) {
    createCompressedStream(fs, chunkPath)
  } else {
    fs.create(chunkPath)
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
    logger.debug("Write file %s (partial)".format(fp.filename))

    val fileDigestBuilder = getFileDigestBuilder(fp.filename)

    for (chunk <- fp.chunks) {
      fileDigestBuilder.update(chunk.fp.digest)

      if (!fileFingerprintOnly) {
        val fingerprint : String = base64.encodeToString(chunk.fp.digest)
        val chunkSize = chunk.size
        val chunkline = "%s\t%s\t%s%n".format(fp.filename, fingerprint, chunkSize)
        chunkWriter.write(chunkline.getBytes("UTF-8"))
      }
    }
    totalChunkCount += fp.chunks.size
  }


  def getFileDigestBuilder(filename: String) : MessageDigest = {
    if (openFileMap.contains(filename)) {
      openFileMap(filename)
    } else {
      val md = MessageDigest.getInstance("MD5");
      openFileMap += (filename -> md)
      md
    }
  }

  def handle(f: File) {
    logger.debug("Write file %s, chunks %s".format(f.filename, f.chunks.size))

    val fileDigestBuilder = getFileDigestBuilder(f.filename)

    if (!fileFingerprintOnly) {
      val l = f.label match {
        case Some(s) => s
        case None => "-"
      }
      val fileline = "%s\t%s\t%s\t%s%n".format(f.filename, f.fileSize, f.fileType, l)
      fileWriter.write(fileline.getBytes("UTF-8"))
    }
    for (chunk <- f.chunks) {
      fileDigestBuilder.update(chunk.fp.digest)

      if (!fileFingerprintOnly) {
        val fp : String = base64.encodeToString(chunk.fp.digest)
        val chunkSize = chunk.size
        val chunkline = "%s\t%s\t%s%n".format(f.filename, fp, chunkSize)
        chunkWriter.write(chunkline.getBytes("UTF-8"))
      }
    }

    val fileFingerprint : String = base64.encodeToString(fileDigestBuilder.digest)
    val fileFingerprintLine = "%s\t%s%n".format(f.filename, fileFingerprint)
    fileFingerpintWriter.write(fileFingerprintLine.getBytes("UTF-8"))
    openFileMap -= f.filename

    totalFileSize += f.fileSize
    totalFileCount += 1
    totalChunkCount += f.chunks.size
  }

  override def quit() {
    if (fileWriter != null) {
      fileWriter.close()
    }
    fileFingerpintWriter.close()
    if (chunkWriter != null) {
      chunkWriter.close()
    }
    report()
    logger.debug("Exit")

  }
}

object Import {
  def main(args: Array[String]): Unit = {
    import ArgotConverters._

    val parser = new ArgotParser("fs-c import", preUsage = Some("Version 0.3.10"))
    val optionFilenames = parser.multiOption[String](List("f", "filename"), "filenames", "Filename to parse")
    val optionReport = parser.option[Int](List("r", "report"), "report", "Interval between progress reports in seconds (Default: 1 minute, 0 = no report)")
    val optionOutput = parser.option[String](List("o", "output"), "output", "HDFS directory for output")
    val optionFileFingerprintOnly = parser.flag[Boolean](List("file-fingerprint-only"), "Import file fingerprint only")
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
    val fingerprintOnly = optionFileFingerprintOnly.value match {
      case Some(b) => b
      case None => false
    }
    for (filename <- optionFilenames.value) {
      val importHandler = new ImportHandler(output, output, compress, fingerprintOnly)
      val reader = Format(format).createReader(filename, importHandler)
      val reporter = new Reporter(importHandler, reportInterval).start()
      reader.parse()

      reporter ! Quit
      importHandler.quit()
    }
  }
}
