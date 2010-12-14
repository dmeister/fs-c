package de.pc2.dedup.fschunk.format

import java.io.FileInputStream
import java.io.InputStream
import java.io.OutputStream
import java.io.BufferedOutputStream
import java.io.FileOutputStream
import java.io.BufferedReader
import java.io.InputStreamReader
import java.net.URI
import java.net.URISyntaxException
import java.nio.charset.Charset
import java.util.ArrayList  
import java.io.IOException
import de.pc2.dedup.chunker._
import de.pc2.dedup.util.FileType
import scala.collection.jcl.Conversions._ 
import scala.collection.mutable.ListBuffer
import scala.actors.Actor
import scala.actors.Actor._
import scala.actors._
import de.pc2.dedup.util._
import java.nio._
import de.pc2.dedup.fschunk._

object IntStreamConverstion {
  	def apply(buffer : Array[Char], offset: Int) : Int = {
	  var result : Int = 0
	  var i = buffer(offset + 3).toInt
	  result = 256 * result + i
      i = buffer(offset + 2).toInt
	  result = 256 * result + i
	  i = buffer(offset + 1).toInt
	  result = 256 * result + i
      i = buffer(offset + 0).toInt
	  result = 256 * result + i
	  return result;
	}
   
	def apply(i: Int) : Array[Byte] = {
	  val dword = new Array[Byte](4)
		dword(3) = ((i >> 24) & 0x000000FF).toByte
		dword(2) = ((i >> 16) & 0x000000FF).toByte
		dword(1) = ((i >> 8) & 0x000000FF).toByte
		dword(0) = (i & 0x00FF).toByte
		return dword;
	}
}

class ParseException(msg: String) extends Exception(msg)

class LegacyFormatReader(filename: String, receiver: Actor) extends Actor with Log with ActorUtil {
	var fileCount = 0L
	var chunkCount = 0L
 
	private def getType(filetype: String) : String = {
			if(filetype.length() == 0) {
				return "---";
			}
			filetype
	}

	private def parseChunks(reader: BufferedReader)(h: (Chunk => Unit)) {
		val recordSize = reader.read()
		if(recordSize == 0 || recordSize == -1) {
			return
		}
		if(recordSize != 24) {
			throw new ParseException("Illegal record size" + recordSize)
		}
		val buffer = new Array[Char](24);
		reader.read(buffer);
		val chunkSize : Int = IntStreamConverstion(buffer, 0);
		if(chunkSize >= 64 * 1024 || chunkSize < 0) {
			throw new ParseException("Illegal chunk size " + chunkSize)
		} else {
			val fp = new Array[Byte](20);
			for(i <- 0 to 19) {
				fp(i) = buffer(4 + i).toByte
			}
			h(Chunk(chunkSize, Digest(fp)))
		}
		chunkCount += 1
		parseChunks(reader)(h)
	}

	private def parseFileEntry(reader: BufferedReader) {
		val line = reader.readLine()
		if (line == null) {
			return
		}
		val chunks = new ListBuffer[Chunk]()
		val elements = line.split("\t")
		if(elements.size != 2 && elements.size != 3) {
		  throw new ParseException("Invalid file line " + line)
		}
		val filename = elements(0)
		val fileSize = java.lang.Long.parseLong(elements(1))
		val fileType : String = if(elements.length >= 3) {
			getType(elements(2))
		} else {
			""
		}
		parseChunks(reader) { c =>
			chunks.append(c)
		}

		if(logger.isDebugEnabled) {
			logger.debug("Fetched file %s from trace".format(filename))
		}

		val file = File(filename, fileSize, fileType, chunks.toList, None)
		stepDownCheck(receiver)
		receiver ! file
		reader.read()
  
		fileCount += 1
  
		if(fileCount >= 200000) {
		  return
		}
  
		clearMailbox()
		parseFileEntry(reader)
	}
 
	def act() { 
		logger.debug("Start")
		try {
			val reader = new BufferedReader(new InputStreamReader(new FileInputStream(filename), Charset.forName("ISO-8859-1")));
			try {
				parseFileEntry(reader)
				receiver ! Quit
    
			} catch {
			case e: Exception =>
				logger.error("Parsing error",e)
			} finally {
				reader.close()
			}
		} catch {
		case e: IOException =>
			logger.fatal("Cannot read trace file " + filename, e)
		}
		logger.debug("Exit")
		exit()
	}  
}

class LegacyFormatWriter(filename: String, privacy: Boolean) extends Actor with Log {
	trapExit = true
	val filestream : OutputStream = new BufferedOutputStream(new FileOutputStream(filename), 512 * 1024)
	var fileCount = 0L
	var chunkCount = 0L
	var totalFileSize = 0L
	val startTime = System.currentTimeMillis()
	var errorCount = 0L


	private def djb2_hash(data : Array[Byte]) : Long = {
		var hash = 5381L
		var i = 0
		while(i < data.size) {
			val c = data(i);
			hash = ((hash << 5) + hash) + c; /* hash * 33 + c */
			i = i+1
		}
		return hash;
	}

	def report() {
		val secs = ((System.currentTimeMillis() - startTime) / 1000)
		val mbs = totalFileSize / secs
		val fps = fileCount / secs
		logger.info("File Count: %d (%d f/s), File Size %s (%s/s), Chunk Count: %d, ErrorCount: %d, Queue: %d".format(
				fileCount, 
				fps,
				StorageUnit(totalFileSize), 
				StorageUnit(mbs),
				chunkCount,
				errorCount,
				mailboxSize))
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
			if(logger.isDebugEnabled()) {
				logger.info("Write %s".format(filename))
			}

			val currentFilename = if(privacy) {  
				"" + djb2_hash(filename.getBytes())
			} else {
				filename.trim()
			} 
			val fileLine = "%s\t%s\t%s\n".format(currentFilename,fileSize ,fileType)
			filestream.write(fileLine.getBytes("US-ASCII"))

			for(chunk <- chunks) {
				if(chunk.fp.digest.size != 20) {
					throw new Exception("Legacy Writer allows only 20 byte fingerprints")
				}
				filestream.write(24) // Record Size
				filestream.write(IntStreamConverstion(chunk.size)) // Chunk Size
				filestream.write(chunk.fp.digest) // Chunk Digest
			}
			filestream.write(0)
			filestream.write("\n".getBytes("US-ASCII"))

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
}

object LegacyFormat extends Format {
	def createReader(filename: String, receiver: Actor) = new LegacyFormatReader(filename,receiver).start()
	def createWriter(filename: String, privacyMode: Boolean) = new LegacyFormatWriter(filename, privacyMode).start()
}
