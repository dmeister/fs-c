package de.pc2.dedup.fschunk.format

import java.io.BufferedInputStream
import java.io.BufferedOutputStream
import java.io.IOException
import java.io.InputStream
import java.io.OutputStream

import com.google.protobuf.ByteString
import com.google.protobuf.InvalidProtocolBufferException

import de.pc2.dedup.chunker.Chunk
import de.pc2.dedup.chunker.Digest
import de.pc2.dedup.chunker.File
import de.pc2.dedup.chunker.FilePart
import de.pc2.dedup.fschunk.handler.FileDataHandler
import de.pc2.dedup.util.Log
import de.pc2.dedup.util.StorageUnit

/**
 * Reader of the protobuf format files
 */
class ProtobufFormatReader(file: InputStream, receiver: FileDataHandler) extends Reader with Log {
  private var faultyTestPartFoundCount: Long = 0

  private def convertChunkData(chunkData: de.pc2.dedup.fschunk.Protocol.Chunk): Chunk = {
    if (chunkData.hasChunkHash) {
      Chunk(chunkData.getSize, Digest(chunkData.getFp.toByteArray), Some(chunkData.getChunkHash))
    } else {
      Chunk(chunkData.getSize, Digest(chunkData.getFp.toByteArray), None)
    }
  }

  private def parseChunkEntry(stream: InputStream): Chunk = {
    val chunkData = de.pc2.dedup.fschunk.Protocol.Chunk.parseDelimitedFrom(stream)
    convertChunkData(chunkData)
  }

  private def parseFileEntry(stream: InputStream) {
    stream.mark(1024)
    var fileData = de.pc2.dedup.fschunk.Protocol.File.parseDelimitedFrom(stream)
    if (fileData == null) {
      return
    }
    val chunkList: Seq[Chunk] = if (fileData.getFilename().size == 0) {
      // Pseudo fallback mode
      stream.reset()

      if (faultyTestPartFoundCount == 0) {
        logger.warn("Found faulty test part data. Falling back mode active")
      }
      val chunk = parseChunkEntry(stream) // try again as chunk entry
      // use dummy file data
      fileData = de.pc2.dedup.fschunk.Protocol.File.newBuilder().
        setFilename("").
        setSize(chunk.size).
        setType("FALLBACK").
        setFilename("FALLBACK %s".format(faultyTestPartFoundCount)).
        setChunkCount(1).build()
      faultyTestPartFoundCount += 1
      List(chunk)
    } else {
      // normal mode
      val chunkList = for (i <- 0 until fileData.getChunkCount())
        yield (parseChunkEntry(stream))
      chunkList
    }

    if (fileData.getPartial()) {
      receiver.handle(FilePart(fileData.getFilename(), chunkList))
    } else {
      val l: Option[String] = if (fileData.hasLabel()) {
        Some(fileData.getLabel())
      } else {
        None
      }
      receiver.handle(File(fileData.getFilename, fileData.getSize, fileData.getType, chunkList, l))
    }
    parseFileEntry(stream)
  }

  /**
   * Parses the given file
   */
  def parse() {
    try {
      val stream = new BufferedInputStream(file);
      try {
        parseFileEntry(stream)
      } catch {
        case e: InvalidProtocolBufferException =>
          logger.debug("No more files", e)
        case e: Exception =>
          logger.error("Parsing error", e)
      } finally {
        stream.close()
      }
    } catch {
      case e: IOException =>
        logger.fatal("Cannot read trace file", e)
    }
    logger.debug("Exit")
  }
}

/**
 * Handler that saved the file and chunk data of the current trace (using this class in parse mode
 * is non-sense) in a file (named filename) using the protocol buffers format specified in the
 * file fs-c.proto
 */
class ProtobufFormatWriter(file: OutputStream, privacy: Boolean) extends FileDataHandler with Log {
  val filestream: OutputStream = new BufferedOutputStream(file, 4 * 1024 * 1024)
  var fileCount = 0L
  var chunkCount = 0L
  var totalFileSize = 0L
  val startTime = System.currentTimeMillis()
  var lastTime = System.currentTimeMillis()
  var errorCount = 0L
  var lock: AnyRef = new Object()

  override def report() {
    val secs = ((System.currentTimeMillis() - startTime) / 1000)
    if (secs > 0) {
      val mbs = totalFileSize / secs
      val fps = fileCount / secs
      logger.info("File count: %d (%d f/s), file size: %s (%s/s), chunk count: %d, error count: %d".format(
        fileCount,
        fps,
        StorageUnit(totalFileSize),
        StorageUnit(mbs),
        chunkCount,
        errorCount))
    }
  }

  def finalFilename(filename: String): String = {
    if (privacy) {
      "" + filename.hashCode
    } else {
      filename
    }
  }

  override def quit() {
    // Sentinal value
    filestream.close()
    report()
  }

  def handle(fp: FilePart) {
    try {
      val filePartData = createFilePartData(finalFilename(fp.filename), fp.chunks)
      lock.synchronized {
        filePartData.writeDelimitedTo(filestream)
        for (c <- fp.chunks) {
          val chunkData = createChunkData(c)
          chunkData.writeDelimitedTo(filestream)
        }
        chunkCount += fp.chunks.size
      }
    } catch {
      case e => logger.error(e)
    }
  }

  def handle(f: File) {
    try {
      val fileData = createFileData(finalFilename(f.filename), f.fileSize, f.fileType, f.chunks, f.label)
      lock.synchronized {

        fileData.writeDelimitedTo(filestream)
        for (c <- f.chunks) {
          val chunkData = createChunkData(c)
          chunkData.writeDelimitedTo(filestream)
        }
        fileCount += 1
        totalFileSize += f.fileSize
        chunkCount += f.chunks.size
      }
    } catch {
      case e => logger.error(e)
    }
  }

  override def fileError(filename: String, fileSize: Long) {
    errorCount += 1
  }

  def createFilePartData(filename: String, chunks: Seq[Chunk]): de.pc2.dedup.fschunk.Protocol.File = {
    val filePartBuilder = de.pc2.dedup.fschunk.Protocol.File.newBuilder()
    filePartBuilder.setFilename(filename).setChunkCount(chunks.size).setPartial(true)
    filePartBuilder.build()
  }

  /**
   * Creates a protocol buffer "File" instance from the file data including the chunks as repeated field
   */
  def createFileData(filename: String, fileSize: Long, fileType: String, chunks: Seq[Chunk], label: Option[String]): de.pc2.dedup.fschunk.Protocol.File = {
    val fileBuilder = de.pc2.dedup.fschunk.Protocol.File.newBuilder()
    fileBuilder.setFilename(filename).setSize(fileSize).setType(fileType).setChunkCount(chunks.size).setPartial(false)
    label match {
      case Some(s) => fileBuilder.setLabel(s)
      case None =>
    }
    fileBuilder.build()
  }

  def createChunkData(c: Chunk): de.pc2.dedup.fschunk.Protocol.Chunk = {
    val chunkBuilder = de.pc2.dedup.fschunk.Protocol.Chunk.newBuilder()
    chunkBuilder.setFp(ByteString.copyFrom(c.fp.digest)).setSize(c.size)
    c.chunkHash match {
      case None =>
      case Some(chunkHash) => chunkBuilder.setChunkHash(chunkHash)
    }
    chunkBuilder.build()
  }
}

object ProtobufFormat extends Format {
  def createReader(file: InputStream, receiver: FileDataHandler) = new ProtobufFormatReader(file, receiver)
  def createWriter(file: OutputStream, privacyMode: Boolean) = new ProtobufFormatWriter(file, privacyMode)
}
