package de.pc2.dedup.fschunk.format

import java.io.FileInputStream
import java.io.FileOutputStream
import java.io.InputStream
import java.io.OutputStream
import scala.collection.mutable.Map
import de.pc2.dedup.fschunk.handler.FileDataHandler
import de.pc2.dedup.fschunk.trace.PrivacyMode

/**
 * Format objected. Used to manage the different format instances.
 * Currently the newer protobuf format an the binary/textual mixed legacy format
 */
object Format {
  /**
   * Default formats
   */
  val formats: Map[String, Format] = Map("protobuf" -> ProtobufFormat, "legacy" -> LegacyFormat)

  /**
   * Register a new format
   */
  def registerFormat(name: String, format: Format) {
    formats += (name -> format)
  }

  /**
   * Check if a format with the given name is registered
   */
  def isFormat(name: String): Boolean = formats.contains(name)

  /**
   * Create a new format instance
   */
  def apply(name: String): Format = formats(name)
}

/**
 * Simple reader trait
 */
trait Reader {
  def parse()
}

/**
 * Trait for a format handler.
 * It should be able to create a new reader.
 * The create writer method is optional. It may return a writer instance that throw exceptions if writing in that format is not supported.
 */
trait Format {
  def createReader(filename: String, receiver: FileDataHandler): Reader = {
    return createReader(new FileInputStream(filename), receiver)
  }
  def createWriter(filename: String, privacyMode: PrivacyMode) : FileDataHandler=
    createWriter(new FileOutputStream(filename), privacyMode)
  
  def createWriter(file: OutputStream) : FileDataHandler= 
    createWriter(file, PrivacyMode.NoPrivacy)
    
  def createReader(file: InputStream, receiver: FileDataHandler): Reader

  /**
   * May return null if writing is not supported by that format.
   * TODO (dmeister) Rewrite to use Option instead of of throwning exceptions if writing is not supported
   */
  def createWriter(file: OutputStream, privacyMode: PrivacyMode): FileDataHandler
}
