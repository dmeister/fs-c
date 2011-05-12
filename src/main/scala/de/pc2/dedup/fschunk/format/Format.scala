package de.pc2.dedup.fschunk.format

import scala.actors._
import scala.collection.mutable.Map
import de.pc2.dedup.fschunk.handler.FileDataHandler

object Format {
    val formats : Map[String,Format] = Map("protobuf" -> ProtobufFormat)
  
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
    def createReader(filename: String, receiver: FileDataHandler) : Reader
    def createWriter(filename: String, privacyMode: Boolean) : FileDataHandler
}
