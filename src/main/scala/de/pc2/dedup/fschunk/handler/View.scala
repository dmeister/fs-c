package de.pc2.dedup.fschunk.handler

import scala.collection.mutable.ListBuffer
import scala.collection.mutable.Map

import org.clapper.argot.ArgotParser
import org.clapper.argot.ArgotConverters
import de.pc2.dedup.chunker.Chunk
import de.pc2.dedup.chunker.File
import de.pc2.dedup.chunker.FilePart
import de.pc2.dedup.fschunk.format.Format
import de.pc2.dedup.fschunk.Reporting
import de.pc2.dedup.util.Log

/**
 * Handler to view a trace file
 */
class ViewHandler(outputOnlyFingerprint: Boolean) extends Reporting with FileDataHandler with Log {
  val filePartialMap = Map.empty[String, ListBuffer[Chunk]]
  val startTime = System.currentTimeMillis()

  logger.debug("Start")

  override def report() {
  }

  def handle(fp: FilePart) {
    logger.debug("View file %s (partial)".format(fp.filename))
    if (!filePartialMap.contains(fp.filename)) {
      filePartialMap += (fp.filename -> new ListBuffer[Chunk]())
    }
    for (chunk <- fp.chunks) {
      filePartialMap(fp.filename).append(chunk)
    }
  }

  def handle(f: File) {
    logger.debug("View file %s, chunks %s".format(f.filename, f.chunks.size))

    val allFileChunks = if (filePartialMap.contains(f.filename)) {
      val partialChunks = filePartialMap(f.filename)
      filePartialMap -= f.filename
      List.concat(partialChunks.toList, f.chunks)
    } else {
      f.chunks
    }

    if (!outputOnlyFingerprint) {
      val msg = f.label match {
        case Some(l) => "File %s, size %s, type %s, label %s".format(f.filename, f.fileSize, f.fileType, l)
        case None => "File %s, size %s, type %s".format(f.filename, f.fileSize, f.fileType)
      }
      println(msg)
    }

    var offset = 0L
    for (chunk <- allFileChunks) {
      val msg = if (outputOnlyFingerprint) {
        "%s".format(chunk.fp)
      } else {
        chunk.chunkHash match {
          case Some(ch) =>
            "Chunk %s, offset %s, size %s, chunk hash %s".format(chunk.fp, offset, chunk.size, ch.toHexString)
          case None =>
            "Chunk %s, offset %s, size %s".format(chunk.fp, offset, chunk.size)
        }
      }
      offset += chunk.size
      println(msg)
    }
  }

  override def quit() {
    logger.debug("Exit")
  }
}

object View {
  def main(args: Array[String]): Unit = {
    import ArgotConverters._

    val parser = new ArgotParser("fs-c view", preUsage = Some("Version 0.3.14"))
    val optionFilenames = parser.multiOption[String](List("f", "filename"), "filenames", "Filename to parse (deprecated)")
    val optionFormat = parser.option[String](List("format"), "trace file format", "Trace file format (expert)")
    val optionOnlyFingerprint = parser.flag[Boolean]("only-fingerprint", false, "Outputs only chunk fingerprints (expert)")
    val parameterFilenames = parser.multiParameter[String]("input filenames",
      "Input trace files files to parse",
      true) {
        (s, opt) =>
          val file = new java.io.File(s)
          if (!file.exists) {
            parser.usage("Input file \"" + s + "\" does not exist.")
          }
          s
      }
    parser.parse(args)

    val format = optionFormat.value match {
      case Some(s) =>
        if (!Format.isFormat(s)) {
          parser.usage("Invalid fs-c file format")
        }
        s
      case None => "protobuf"
    }
    val outputOnlyFingerprint = optionOnlyFingerprint.value match {
      case Some(b) => b
      case None => false
    }

    val filenames = if (optionFilenames.value.isEmpty && parameterFilenames.value.isEmpty) {
      parser.usage("Provide at least one trace file")
    } else if (!optionFilenames.value.isEmpty && !parameterFilenames.value.isEmpty) {
      parser.usage("Provide files by -f (deprecated) or by positional parameter, but not both")
    } else if (!optionFilenames.value.isEmpty) {
      optionFilenames.value.toList
    } else {
      parameterFilenames.value.toList
    }

    for (filename <- filenames) {
      val viewHandler = new ViewHandler(outputOnlyFingerprint)
      val reader = Format(format).createReader(filename, viewHandler)
      reader.parse()
      viewHandler.quit()
    }
  }
}

