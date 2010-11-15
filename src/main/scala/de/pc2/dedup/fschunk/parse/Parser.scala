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
import scala.collection.jcl.Conversions._ 
import com.google.protobuf.CodedInputStream
import scala.actors.Actor
import scala.collection.mutable.ListBuffer
import scala.actors.Actor
import scala.actors.Actor._
import scala.actors._
import de.pc2.dedup.util._
import com.google.protobuf.InvalidProtocolBufferException
import de.pc2.dedup.fschunk.format.Format


class Parser(filename: String, format: String, handlers: List[Actor]) extends Actor with Log with ActorUtil {
	trapExit = true
	val MAILBOX_THRESHOLD = 1000

	def quit() {
		notifyHandlers(Quit)
		logger.debug("Exit")
		exit()
	}

	def act() { 
		logger.debug("Start")
		logger.info("Started parser with %d handlers".format(handlers.size))
		val reader = Format(format).createReader(filename, this)
		link(reader)
		handlers.foreach(h => link(h))
		loop {
			react {
			case Exit(actor,reason) =>
			logger.info("%s exited with reason %s".format(actor,reason))
			notifyHandlers(Quit)
			logger.debug("Exit")
			exit()
			case Quit =>
			quit()
			case f: File =>
			if(logger.isDebugEnabled) {
				logger.debug("Dispatch file " + f.filename)
			}
			notifyHandlers(f)
			case Report =>
			reader ! Report
			notifyHandlers(Report)
			case msg: Any =>
			logger.warn("Unknown message %s".format(msg))
			}
		}
	}

	def notifyHandlers(msg: Any) {
		for(handler <- handlers) {
		  stepDownCheck(handler)
			handler ! msg

		}
	}
}