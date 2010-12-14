package de.pc2.dedup.fschunk.format

import java.io.FileInputStream
import java.io.InputStream
import java.io.OutputStream
import java.io.BufferedOutputStream
import java.io.FileOutputStream
import java.net.URI
import java.net.URISyntaxException
import java.nio.charset.Charset
import java.util.ArrayList  
import java.io.IOException
import de.pc2.dedup.chunker._
import de.pc2.dedup.util.FileType
import scala.collection.jcl.Conversions._ 
import scala.actors.Actor
import scala.collection.mutable.ListBuffer
import scala.actors.Actor
import scala.actors.Actor._
import de.pc2.dedup.util._
import com.google.protobuf._

class ProtobufFormatReader(filename: String, receiver: Actor) extends Actor with Log with ActorUtil {
	private def parseChunkEntry(stream: InputStream) : Chunk = {
	  val chunkData = de.pc2.dedup.fschunk.Protocol.Chunk.parseDelimitedFrom(stream)
	  Chunk(chunkData.getSize, Digest(chunkData.getFp.toByteArray))
	}
	private def parseFileEntry(stream: InputStream) {
		val fileData = de.pc2.dedup.fschunk.Protocol.File.parseDelimitedFrom(stream)
		
		val chunkList = for(i <- 0 until fileData.getChunkCount())
			yield(parseChunkEntry(stream))

                        val l : Option[String] = if (fileData.hasLabel()) {
                            Some(fileData.getLabel())
                        } else {
                            None
                        }

			val file = File(fileData.getFilename, fileData.getSize, fileData.getType, chunkList.toList, l)

		stepDownCheck(receiver)
		receiver ! file 

		clearMailbox()
		parseFileEntry(stream) 
	}

	def act() { 
		logger.debug("Start")
		try {
			val stream = new FileInputStream(filename);
			try {
				parseFileEntry(stream) 
			} catch {
			case e: InvalidProtocolBufferException =>
			logger.debug("No more files")

			receiver ! Quit
			case e: Exception =>
			logger.error("Parsing error",e)
			} finally {
				stream.close()
			}
		} catch {
		case e: IOException =>
		logger.fatal("Cannot read trace file " + filename, e)
		}
		logger.debug("Exit")
		exit()
	}  
}

/**
 * Handler that saved the file and chunk data of the current trace (using this class in parse mode
 * is non-sense) in a file (named filename) using the protocol buffers format specified in the
		* file fs-c.proto
		*/
    class ProtobufFormatWriter(filename: String, privacy: Boolean) extends Actor with Log {
	trapExit = true
	val filestream : OutputStream = new BufferedOutputStream(new FileOutputStream(filename), 512 * 1024)
	var fileCount = 0L
	var chunkCount = 0L
	var totalFileSize = 0L
	val startTime = System.currentTimeMillis()
	var lastTime = System.currentTimeMillis()
	var errorCount = 0L

	def report() {
		val secs = ((System.currentTimeMillis() - startTime) / 1000)
		if(secs > 0) {
			val mbs = totalFileSize / secs
			val fps = fileCount / secs
			logger.info("%s: file count: %d (%d f/s), file size: %s (%s/s), chunk count: %d, error count: %d, queue: %d".format(
                                        filename,
					fileCount, 
					fps,
					StorageUnit(totalFileSize), 
					StorageUnit(mbs),
					chunkCount,
					errorCount,
					mailboxSize))
		}
	}

	def act() { 
	    logger.debug("Start")
	    while(true) {
	        receive {
		    case Quit =>
			// Sentinal value
			filestream.close()
			report()
			logger.debug("Exit")
			exit()
		    case File(filename, fileSize, fileType,chunks, label) =>
                        val currentFilename = if(privacy) {  
				"" + filename.hashCode
			} else {
				filename
			} 
			val fileData = createFileData(currentFilename, fileSize, fileType, chunks, label)
                            logger.debug(fileData)

			fileData.writeDelimitedTo(filestream)
			for(c <- chunks) {
			  val chunkData = createChunkData(c)
			  chunkData.writeDelimitedTo(filestream)
			}
			fileCount += 1
			totalFileSize += fileSize
			chunkCount += chunks.size
			case FileError(filename, fileSize) =>
			errorCount += 1
			case Report =>
				report()
			case msg: Any =>
				logger.error("Unknown Message " + msg)
			}  
		}
	}

	/**
	* Creates a protocol buffer "File" instance from the file data including the chunks as repeated field
	*/
	def createFileData(filename: String, fileSize: Long, fileType: String, chunks: List[Chunk], label: Option[String]) : de.pc2.dedup.fschunk.Protocol.File = {
			val fileBuilder = de.pc2.dedup.fschunk.Protocol.File.newBuilder() 
			fileBuilder.setFilename(filename).setSize(fileSize).setType(fileType).setChunkCount(chunks.size)
                        label match {
                            case Some(s) => fileBuilder.setLabel(s)
                            case None =>
                        }
			fileBuilder.build()
	}
	def createChunkData(c: Chunk) : de.pc2.dedup.fschunk.Protocol.Chunk = {
		val chunkBuilder = de.pc2.dedup.fschunk.Protocol.Chunk.newBuilder()
		chunkBuilder.setFp(ByteString.copyFrom(c.fp.digest)).setSize(c.size)
		chunkBuilder.build()
	}
}  

object ProtobufFormat extends Format {
	def createReader(filename: String, receiver: Actor) = new ProtobufFormatReader(filename,receiver).start()
	def createWriter(filename: String, privacyMode: Boolean) = new ProtobufFormatWriter(filename, privacyMode).start()
}
