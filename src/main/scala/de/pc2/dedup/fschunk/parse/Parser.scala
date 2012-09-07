package de.pc2.dedup.fschunk.parse

import de.pc2.dedup.chunker.File
import de.pc2.dedup.chunker.FilePart
import de.pc2.dedup.fschunk.format.Format
import de.pc2.dedup.fschunk.handler.FileDataHandler
import de.pc2.dedup.util.Log

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
