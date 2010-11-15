package de.pc2.dedup.fschunk.handler.direct

import de.pc2.dedup.chunker._
import java.util.concurrent.atomic.AtomicLong
import java.util.concurrent.ConcurrentHashMap
import scala.collection.mutable.ListBuffer
import de.pc2.dedup.util.StorageUnit
import scala.actors.Actor
import scala.actors.Actor._
import de.pc2.dedup.util.Log

class InMemoryChunkHandler(silent:Boolean, d:ChunkIndex) extends Actor with Log {
	var totalFileSize = 0L 
	var totalChunkSize = 0L
	var totalChunkCount = 0L

	val startTime = System.currentTimeMillis()

	def report() {
		val stop = System.currentTimeMillis() 
		val seconds = (stop - startTime) / 1000 

		val totalRedundancy = totalFileSize - totalChunkSize
		val msg = new StringBuffer()
		msg.append("\n")
		msg.append("Total Size: " + StorageUnit(totalFileSize) + "\n")
		msg.append("Chunk Size: " + StorageUnit(totalChunkSize) + "\n")
		msg.append("Redundancy: " + StorageUnit(totalRedundancy))
		if(totalFileSize > 0) {
			msg.append(" (%.2f%%)".format(100.0 * totalRedundancy / totalFileSize))
		}
		msg.append("%nChunks: %d".format(totalChunkCount))
		if(totalChunkCount > 0) {
			msg.append(" (%s/Chunk)".format(totalChunkCount, 
					StorageUnit(totalFileSize / totalChunkCount)))
		}
		msg.append("%nTime: %ds%n".format(seconds))
		if(seconds > 0) {
			msg.append("Throughput: %s/s".format(StorageUnit(totalFileSize / seconds)))
		} else {
			msg.append("Throughput: N/A")
		}
		logger.info(msg)	  
	}

	def act() { 
	  logger.debug("Start")
		loop {  
			react {          
			case Quit =>
			report()
			logger.debug("Exit")
			exit()
			case Report =>
			report()
			case File(filename, size, fileType, chunks) =>
			var chunkFileSize = 0L
			var chunkSize = 0L 
			var chunkCount = 0L
			for(chunk <- chunks) {
				chunkCount += 1
				chunkFileSize += chunk.size
				if(!d.check(chunk.fp)) {
					// New Chunk
					d.update(chunk.fp)
					chunkSize += chunk.size
				}  
			} 
			totalChunkCount += chunkCount
			totalFileSize += chunkFileSize
			totalChunkSize += chunkSize

			val msg = new StringBuffer(filename)
			if(!silent) {
				val redundancy = size - chunkSize    
				msg.append("\nSize: %s (%d Byte)%n".format(
						StorageUnit(chunkFileSize),chunkFileSize))
						msg.append("Redundancy: %s".format(StorageUnit(redundancy)))
						if(chunkFileSize > 0) {
							msg.append(" (%.2f%%)".format(100.0 * redundancy / chunkFileSize))
						}
				msg.append("%nPatch Size: %s (%d Byte)".format(
						StorageUnit(chunkFileSize - redundancy),chunkFileSize - redundancy))
			}
			logger.info(msg)
			} 
		}
	} 
}
