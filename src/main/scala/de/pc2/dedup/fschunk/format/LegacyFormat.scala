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
import scala.collection.mutable.ListBuffer
import scala.actors.Actor
import scala.actors.Actor._
import scala.actors._
import de.pc2.dedup.util._
import java.nio._
import de.pc2.dedup.fschunk._
import de.pc2.dedup.fschunk.handler.FileDataHandler

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

class LegacyFormatReader(filename: String, receiver: FileDataHandler) extends Reader with Log {
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
			h(Chunk(chunkSize, Digest(fp), None))
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

		val file = File(filename, fileSize, fileType, chunks.toList, None)
		receiver.handle(file)
		reader.read()
  
		fileCount += 1  
		parseFileEntry(reader)
	}
 
	  /**
   * Parses the given file
   */
  def parse() {
    try {
      val reader = new BufferedReader(new InputStreamReader(new FileInputStream(filename), Charset.forName("ISO-8859-1")));
      try {
        parseFileEntry(reader)
      } catch {
        case e: Exception =>
          logger.error("Parsing error", e)
      } finally {
        reader.close()
      }
    } catch {
      case e: IOException =>
        logger.fatal("Cannot read trace file " + filename, e)
    }
    logger.debug("Exit")
  }
}

class LegacyFormatWriter extends FileDataHandler with Log {
	def handle(f: File) {
	    throw new Exception("Cannot use legacy format for writing")
	  }

  def handle(fp: FilePart) {
    throw new Exception("Cannot use legacy format for writing")
  }
}

object LegacyFormat extends Format {
	def createReader(filename: String, receiver: FileDataHandler) = new LegacyFormatReader(filename, receiver)
	def createWriter(filename: String, privacyMode: Boolean) = new LegacyFormatWriter()
}