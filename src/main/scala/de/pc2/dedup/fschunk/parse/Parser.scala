package de.pc2.dedup.fschunk.parse

import java.io.FileInputStream
import java.io.IOException
import java.io.InputStream
import java.net.URI
import java.net.URISyntaxException
import java.nio.charset.Charset
import java.util.ArrayList

import de.pc2.dedup.chunker._
import de.pc2.dedup.util.FileType
import com.google.protobuf.CodedInputStream
import scala.actors.Actor
import scala.collection.mutable.ListBuffer
import scala.actors.Actor
import scala.actors.Actor._
import scala.actors._
import de.pc2.dedup.util._
import com.google.protobuf.InvalidProtocolBufferException
import de.pc2.dedup.fschunk.format.Format
import de.pc2.dedup.fschunk.handler.FileDataHandler

/**
 * Parses a file and calls all handler for each file and file part
 */
class Parser(filename: String, format: String, handlers: Seq[FileDataHandler]) extends FileDataHandler with Log {
  override def quit() {
    for (handler <- handlers) {
      handler.quit()
    }
  }

  def parse() {
    logger.debug("Started parser with %d handlers".format(handlers.size))
    val reader = Format(format).createReader(filename, this)
    reader.parse()
  }

  override def report() {
    for (handler <- handlers) {
      handler.report()
    }
  }

  def handle(fp: FilePart) {
    for (handler <- handlers) {
      handler.handle(fp)
    }
  }

  def handle(f: File) {
    for (handler <- handlers) {
      handler.handle(f)
    }
  }
}
