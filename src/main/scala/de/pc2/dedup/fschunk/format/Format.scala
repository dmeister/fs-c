package de.pc2.dedup.fschunk.format

import scala.actors._
import scala.collection.mutable.Map

object Format {
  val formats : Map[String,Format] = Map("protobuf" -> ProtobufFormat, "legacy" -> LegacyFormat)
  
  def registerFormat(name: String, format: Format) {
	  formats += (name -> format)
  }
  
  def isFormat(name: String) = formats.contains(name)
  
  def apply(name: String) = formats(name)
}

trait Format {
    def createReader(filename: String, receiver: Actor) : Actor
    def createWriter(filename: String, privacyMode: Boolean) : Actor
}
