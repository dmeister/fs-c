package de.pc2.dedup.fschunk.format

import scala.actors._
import scala.collection.mutable.Map
import de.pc2.dedup.fschunk.handler.FileDataHandler
import java.io.InputStream
import java.io.OutputStream
import java.io.FileInputStream
import java.io.FileOutputStream

object Format {
  val formats: Map[String, Format] = Map("protobuf" -> ProtobufFormat, "legacy" -> LegacyFormat)

  def registerFormat(name: String, format: Format) {
    formats += (name -> format)
  }

  def isFormat(name: String) = formats.contains(name)

  def apply(name: String) = formats(name)
}

trait Reader {
  def parse()
}

trait Format {
  def createReader(filename: String, receiver: FileDataHandler): Reader = {
    return createReader(new FileInputStream(filename), receiver)
  }
  def createWriter(filename: String, privacyMode: Boolean): FileDataHandler = {
    return createWriter(new FileOutputStream(filename), privacyMode)
  }

  def createReader(file: InputStream, receiver: FileDataHandler): Reader
  def createWriter(file: OutputStream, privacyMode: Boolean): FileDataHandler
}
