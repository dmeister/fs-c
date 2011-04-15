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

object FileProcessor {
    val activeCount = new AtomicLong(0L)
    val totalCount = new AtomicLong(0L)  
    val totalRead = new AtomicLong(0L)
	
    val MAX_CHUNKLIST_SIZE = 10000L
}

class FileProcessor(file: JavaFile, label: Option[String], chunkerList: List[(Chunker, List[FileDataHandler])], defaultBufferSize: Int, progressHandler: (File) => Unit) extends Runnable with Log {

    def run() {
        if(logger.isDebugEnabled) {
	    logger.debug("Started File %s (%s)".format(file, StorageUnit(file.length)))
	}
	FileProcessor.activeCount.incrementAndGet()
	FileProcessor.totalCount.incrementAndGet()
  
	var s : InputStream = null
	try {
	    val bufferSize : Int = if(file.length >= defaultBufferSize) {
	        defaultBufferSize
	    } else {
		file.length.toInt
	    }
	    val buffer = new Array[Byte](bufferSize)
	    val sessionList = for {chunker <- chunkerList} yield (chunker._1.createSession(), chunker._2, new ListBuffer[Chunk]())

	    s = new FileInputStream(file)
	    val t = FileType.getNormalizedFiletype(file)
	    var r = s.read(buffer)
	    while(r > 0) {
		FileProcessor.totalRead.addAndGet(r)
                for ((session, handlers, chunkList) <- sessionList) {
	    	    session.chunk(buffer, r) { 
		        chunk => chunkList.append(chunk)
		    }
		    if (chunkList.size > FileProcessor.MAX_CHUNKLIST_SIZE) {
		        val fp = new FilePart(file.getCanonicalPath, chunkList.toList)
                        for (handler <- handlers) {
                            handler.handle(fp)
                        }
                        chunkList.clear()
		    }
                }
	        r = s.read(buffer)
	    }
            for ((session, handlers, chunkList) <- sessionList) {
	        session.close() { 
		    chunk => chunkList.append(chunk)
	        }
	        val f = new File(file.getCanonicalPath, file.length, t, chunkList.toList, label)
	        logger.debug(f)
	        for(handler <- handlers) {
	            handler.handle(f)
                }  
            }
            val fileWithoutChunks = new File(file.getCanonicalPath, file.length, t, List(), label)
            progressHandler(fileWithoutChunks) 
	} catch {
	    case e: FileNotFoundException =>
                for (val (chunker, handlers) <- chunkerList) {
	            for(handler <- handlers) {
	                handler.fileError(file.getCanonicalPath, file.length)
		    }  
                }
		logger.warn("File %s".format(e.getMessage))
	    case  e: Exception => 
		logger.error("Processing Error %s (%s)".format(file,StorageUnit(file.length)),e)
                for (val (chunker, handlers) <- chunkerList) {
		    for(handler <- handlers) {
		        handler.fileError(file.getCanonicalPath, file.length)
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
