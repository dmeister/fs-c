package de.pc2.dedup.chunker.rabin

import java.math.BigInteger

import de.pc2.dedup.chunker.Chunker
import scala.collection.mutable._ 
import de.pc2.dedup.util._
 
/**
 * Rabin Chunking
 */
class RabinChunker(minimalSize: Int, 
                   averageSize: Int, 
                   maximalSize: Int, 
                   digestFactory: DigestFactory, 
                   val chunkerName: String) extends Chunker with Log {
    private val rabinWindow = new RabinWindow(Rabin.createDefaultRabin(), 48)
    private val breakmark : Long = (Math.pow(2.0, BigInteger.valueOf(averageSize).bitLength()-1)-1).toLong
    val positionWindowBeforeMin : Int = minimalSize - 48
	
    /**
     * Creates a new rabin chunking session
     */
    def createSession() : ChunkerSession = new RabinChunkerSession(chunkerName)
  
    private class RabinChunkerSession(val chunkerName: String) extends ChunkerSession {
        val rabinSession = rabinWindow.createSession()
        val overflowChunk = new Array[Byte](maximalSize)
        var overflowChunkPos: Int = 0
        val digestBuilder = digestFactory.builder()
		  
        def acceptChunk(h: (Chunk => Unit), data: Array[Byte], dataPos: Int, dataLen: Int) {
            val digest = digestBuilder.append(overflowChunk, 0, overflowChunkPos)
            .append(data, dataPos, dataLen).build()
            
            val c = Chunk(this.overflowChunkPos + dataLen, digest)
            h(c)
            rabinSession.clear()
            overflowChunkPos = 0
        }  
		 
        def chunk(data: Array[Byte], size: Int)(h: (Chunk => Unit))  {
            var current = 0
            var nonChunkedData = 0
            var break = false;
            while(current < size && !break) {
                var todo = size - current
                var countToMax = maximalSize - overflowChunkPos
                if (todo > countToMax) {
                    todo = countToMax
                }
                var countToMin = positionWindowBeforeMin - overflowChunkPos
        
                if (countToMin > todo) {
                    // break (but Scala has no break
                    break = true
                } else {
                    if (countToMin < 0) {
                        countToMin = 0
                    }
                    val end = current + todo
                    current += countToMin
                    var innerBreak = false
                    while(current < end && !innerBreak) {
                        val value : Int = if(data(current) < 0) {
                            256 + data(current)
                        } else {
                            data(current)
                        }
                        current += 1
                        this.rabinSession.append(value)
                        val isBreakmark = (this.rabinSession.fingerprint & breakmark) == breakmark;
            
                        if (isBreakmark) {
                            acceptChunk(h, data, nonChunkedData,current - nonChunkedData)
                            nonChunkedData = current
                            innerBreak = true
                        }
                    }
          
                    // end of inner loop
                    if (current == end && todo == countToMax) {
                        acceptChunk(h, data, nonChunkedData, current - nonChunkedData)
                        nonChunkedData = current
                    }
                }
            }
            // end of outer loop
            if (size - nonChunkedData > 0) {
                System.arraycopy(data, nonChunkedData, overflowChunk, overflowChunkPos, size - nonChunkedData)
                overflowChunkPos += (size - nonChunkedData)
            }
        }
        /**
         * Closes the rabin chunker
         */
        def close()(h: (Chunk => Unit)) {
            if(overflowChunkPos > 0) {
                acceptChunk(h, null, 0, 0)
            }
        }
    }
}
