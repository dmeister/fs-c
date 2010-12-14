package de.pc2.dedup.chunker.rabin

import java.math.BigInteger

import de.pc2.dedup.chunker.Chunker
import scala.collection.mutable._ 
 
class RabinChunker(minimalSize: Int, averageSize: Int, maximalSize: Int, digestFactory: DigestFactory, val chunkerName: String) extends Chunker {
	private val rabinWindow = new RabinWindow(Rabin.createDefaultRabin(), 48);
	private val breakmark : Long = (Math.pow(2.0, BigInteger.valueOf(averageSize).bitLength()-1)-1).toLong
	 
	def createSession() : ChunkerSession = new RabinChunkerSession(chunkerName)
  
	private class RabinChunkerSession(val chunkerName: String) extends ChunkerSession {
		val rabinSession = rabinWindow.createSession()
		val currentChunk = new Array[Byte](maximalSize)
		var currentChunkPos: Int = 0
		  
		def acceptChunk(h: (Chunk => Unit)) {
			val c = Chunk(this.currentChunkPos, digestFactory.createFromData(currentChunk, currentChunkPos))
			h(c)
			rabinSession.clear()
			currentChunkPos = 0
		}  
		 
		def chunk(data: Array[Byte],size: Int)(h: (Chunk => Unit))  {
			var i = 0
			while(i < size) {
				this.currentChunk(this.currentChunkPos) = data(i);
				this.currentChunkPos += 1
				val value : Int = if(data(i) < 0) {
					256 + data(i)
				} else {
				  data(i)
				}
				this.rabinSession.append(value)
				
				val isBreakmark = (this.rabinSession.getFingerprint() & breakmark) == breakmark;
				if(isBreakmark && this.currentChunkPos > minimalSize) {
					rabinSession.clear()
					acceptChunk(h)
				} else if(this.currentChunkPos >= maximalSize) {
					rabinSession.clear()
					acceptChunk(h)
				}
				i += 1
			}
		}
 
		def close()(h: (Chunk => Unit)) {
			if(this.currentChunkPos > 0) {
				acceptChunk(h)
			}
		}
	}
}
