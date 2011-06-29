package de.pc2.dedup.fschunk.handler

import de.pc2.dedup.chunker.File
import de.pc2.dedup.chunker.FilePart
import scala.actors._
import de.pc2.dedup.fschunk.Reporting

trait FileDataHandler extends Reporting {
  def handle(f: File)

  def handle(fp: FilePart)

  /**
   * Empty default implemention
   */
  def quit() {
  }

  def fileError(filename: String, fileSize: Long) {
  }

  def report() {
  }
}

