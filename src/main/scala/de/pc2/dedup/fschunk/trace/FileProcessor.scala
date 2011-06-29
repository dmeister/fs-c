package de.pc2.dedup.fschunk.trace

import java.io.Closeable
import java.io.{File => JavaFile}
import java.io.FileInputStream
import java.io.IOException
import java.io.InputStream
import java.io.FileNotFoundException
import java.security.MessageDigest
import java.security.NoSuchAlgorithmException

import de.pc2.dedup.chunker._
import de.pc2.dedup.chunker.Chunker 
import de.pc2.dedup.chunker.File
import de.pc2.dedup.util._
import scala.actors.Actor 
import scala.collection.mutable.ListBuffer 
import scala.actors.Actor._
import de.pc2.dedup.util.StorageUnit
import java.util.concurrent.atomic._
import de.pc2.dedup.fschunk.handler.FileDataHandler

object FileProcessor extends Log {
    val activeCount = new AtomicLong(0L)
    val totalCount = new AtomicLong(0L)  
    val totalRead = new AtomicLong(0L)
	
    val MAX_CHUNKLIST_SIZE = 10000L
    
    def report() {
        logger.info("IO Stats: read: %s ops, %s bytes".format(totalCount, StorageUnit(totalRead.longValue())))
    }
}

class FileProcessor(file: JavaFile, 
                    path: String,
                    label: Option[String], 
                    chunkerList: List[(Chunker, List[FileDataHandler])],
                    defaultBufferSize: Int, 
                    progressHandler: (File) => Unit) extends Runnable with Log {

    def readIntoBuffer(s: InputStream, buffer: Array[Byte], offset: Long) : Int = {
        if(logger.isDebugEnabled) {
            logger.debug("File: Read %s, offset %s".format(file, StorageUnit(offset)))
        }
        var r = s.read(buffer)
        if(logger.isDebugEnabled) {
            logger.debug("File: Read %s finished, offset %s, data size %s".format(file, StorageUnit(offset), StorageUnit(r)))
        }
        return r
    }

    def run() {
	FileProcessor.activeCount.incrementAndGet()
	FileProcessor.totalCount.incrementAndGet()
  
	var s : InputStream = null
        val fileLength = file.length
        if(logger.isDebugEnabled) {
	    logger.debug("Started File %s (%s)".format(file, StorageUnit(fileLength)))
	}
	try {
	    val bufferSize : Int = if(fileLength >= defaultBufferSize) {
	        defaultBufferSize
	    } else {
		fileLength.toInt
	    }
	    val buffer = new Array[Byte](bufferSize)
	    val sessionList = for {chunker <- chunkerList} yield (chunker._1.createSession(), chunker._2, new ListBuffer[Chunk]())

	    s = new FileInputStream(file)
	    val t = FileType.getNormalizedFiletype(file)
	    var r = readIntoBuffer(s, buffer, 0)
            var offset = 0L
	    while(r > 0) {
                offset += r
		FileProcessor.totalRead.addAndGet(r)
                for ((session, handlers, chunkList) <- sessionList) {
	    	    session.chunk(buffer, r) { 
		        chunk => chunkList.append(chunk)
		    }
		    if (chunkList.size > FileProcessor.MAX_CHUNKLIST_SIZE) {
		        val fp = new FilePart(path, chunkList.toList)
                        for (handler <- handlers) {
                            handler.handle(fp)
                        }
                        chunkList.clear()
		    }
                }
	        r = readIntoBuffer(s, buffer, offset)
	    }
            for ((session, handlers, chunkList) <- sessionList) {
	        session.close() { 
		    chunk => chunkList.append(chunk)
	        }
	        val f = new File(path, fileLength, t, chunkList.toList, label)
	        for(handler <- handlers) {
	            handler.handle(f)
                }  
            }
            val fileWithoutChunks = new File(path, fileLength, t, List(), label)
            progressHandler(fileWithoutChunks) 
	} catch {
	    case e: FileNotFoundException =>
                for (val (chunker, handlers) <- chunkerList) {
	            for(handler <- handlers) {
	                handler.fileError(path, fileLength)
		    }  
                }
		logger.warn("File %s".format(e.getMessage))
	    case  e: Exception => 
		logger.error("Processing Error %s (%s)".format(file,StorageUnit(fileLength)),e)
                for (val (chunker, handlers) <- chunkerList) {
		    for(handler <- handlers) {
		        handler.fileError(file.getCanonicalPath, fileLength)
		    }  
                }
	} finally {
	    close(s) 
	}
  	if(logger.isDebugEnabled) {
            logger.debug("Finished File " + file)
	}
	FileProcessor.activeCount.decrementAndGet()
    }

    def close(c: Closeable) {
        if(c != null) {
	    try {
		c.close()
	    } catch {
	        case e:IOException =>
	    }
	}
    }
}
