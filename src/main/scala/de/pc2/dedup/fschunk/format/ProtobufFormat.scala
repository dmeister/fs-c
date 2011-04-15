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
import de.pc2.dedup.util._
import de.pc2.dedup.fschunk.handler.FileDataHandler
import com.google.protobuf._

class ProtobufFormatReader(filename: String, receiver: FileDataHandler) extends Reader with Log {
    private def parseChunkEntry(stream: InputStream) : Chunk = {
        val chunkData = de.pc2.dedup.fschunk.Protocol.Chunk.parseDelimitedFrom(stream)
	Chunk(chunkData.getSize, Digest(chunkData.getFp.toByteArray))
    }
	
    private def parseFileEntry(stream: InputStream) {
        val fileData = de.pc2.dedup.fschunk.Protocol.File.parseDelimitedFrom(stream)
        if (fileData == null) {
	    return
        }
	val chunkList = for(i <- 0 until fileData.getChunkCount())
	    yield(parseChunkEntry(stream))

        if (fileData.getPartial()) {
            receiver.handle(FilePart(fileData.getFilename(), chunkList.toList))
        } else {
            val l : Option[String] = if (fileData.hasLabel()) {
                Some(fileData.getLabel())
            } else {
                None
            }
	    receiver.handle(File(fileData.getFilename, fileData.getSize, fileData.getType, chunkList.toList, l))
        } 
	parseFileEntry(stream) 
	}
	
	def parse() {
		logger.debug("Start")
		try {
			val stream = new FileInputStream(filename);
			try {
				parseFileEntry(stream) 
			} catch {
			    case e: InvalidProtocolBufferException =>
			        logger.debug("No more files")
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
	}
}

/**
 * Handler that saved the file and chunk data of the current trace (using this class in parse mode
 * is non-sense) in a file (named filename) using the protocol buffers format specified in the
 * file fs-c.proto
 */
class ProtobufFormatWriter(filename: String, privacy: Boolean) extends FileDataHandler with Log {
    val filestream : OutputStream = new BufferedOutputStream(new FileOutputStream(filename), 512 * 1024)
    var fileCount = 0L
    var chunkCount = 0L
    var totalFileSize = 0L
    val startTime = System.currentTimeMillis()
    var lastTime = System.currentTimeMillis()
    var errorCount = 0L
    var lock : AnyRef = new Object()

    override def report() {
        val secs = ((System.currentTimeMillis() - startTime) / 1000)
	if(secs > 0) {
	    val mbs = totalFileSize / secs
	    val fps = fileCount / secs
	    logger.info("%s: file count: %d (%d f/s), file size: %s (%s/s), chunk count: %d, error count: %d".format(
                filename,
		fileCount, 
		fps,
		StorageUnit(totalFileSize), 
		StorageUnit(mbs),
		chunkCount,
		errorCount))
	}
    }

    def finalFilename(filename : String) : String = {
        if(privacy) {  
            "" + filename.hashCode
        } else {
            filename
        }   
    }

    override def quit() {
        // Sentinal value
	filestream.close()
	report()
    }

    def handle(fp: FilePart) {
        lock.synchronized {
            try {
                val filePartData = createFilePartData(finalFilename(fp.filename), fp.chunks)
                logger.debug(filePartData)
                
                for(c <- fp.chunks) {
    	            val chunkData = createChunkData(c)
    	            chunkData.writeDelimitedTo(filestream)
    	        }
    	        chunkCount += fp.chunks.size
    	    } catch {
    	        case e => logger.error(e)
            }
        }
    }

    def handle(f: File) {
        lock.synchronized {
            try {
	        val fileData = createFileData(finalFilename(f.filename), f.fileSize, f.fileType, f.chunks, f.label)
                logger.debug(fileData)

	        fileData.writeDelimitedTo(filestream)
	        for(c <- f.chunks) {
	            val chunkData = createChunkData(c)
		    chunkData.writeDelimitedTo(filestream)
                }
	        fileCount += 1
	        totalFileSize += f.fileSize
	        chunkCount += f.chunks.size
	    } catch {
    	        case e => logger.error(e)
    	    }
        }
    }
 
    override def fileError(filename: String, fileSize: Long) {
        errorCount += 1
    }

    def createFilePartData(filename: String, chunks: List[Chunk]) : de.pc2.dedup.fschunk.Protocol.File = {
        val filePartBuilder = de.pc2.dedup.fschunk.Protocol.File.newBuilder() 
	filePartBuilder.setFilename(filename).setChunkCount(chunks.size).setPartial(true)
	filePartBuilder.build()
    }
	
    /**
     * Creates a protocol buffer "File" instance from the file data including the chunks as repeated field
     */
    def createFileData(filename: String, fileSize: Long, fileType: String, chunks: List[Chunk], label: Option[String]) : de.pc2.dedup.fschunk.Protocol.File = {
        val fileBuilder = de.pc2.dedup.fschunk.Protocol.File.newBuilder() 
	fileBuilder.setFilename(filename).setSize(fileSize).setType(fileType).setChunkCount(chunks.size).setPartial(false)
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
	def createReader(filename: String, receiver: FileDataHandler) = new ProtobufFormatReader(filename, receiver)
	def createWriter(filename: String, privacyMode: Boolean) = new ProtobufFormatWriter(filename, privacyMode)
}
