package de.pc2.dedup.chunker.fixed

import de.pc2.dedup.chunker.Chunker
import de.pc2.dedup.chunker.Chunk
import scala.collection.mutable._
import de.pc2.dedup.chunker.DigestFactory
import de.pc2.dedup.chunker.ChunkerSession
import java.nio.ByteBuffer

/**
 * Chunker for Static (or Fixed)-size chunking
 *
 * @chunkSize: Static size of all chunks generated by this chunker
 */
class FixedChunker(chunkSize: Int, digestFactory: DigestFactory, val chunkerName: String) extends Chunker {
  /**
   * Creates a new fixed chunker session
   */
  def createSession(): ChunkerSession = new FixedChunkerSession(chunkerName)

  /**
   * Fixed Chunker Session
   */
  class FixedChunkerSession(val chunkerName: String) extends ChunkerSession {
    val currentChunk = new Array[Byte](chunkSize)
    var currentChunkPos = 0

    /**
     * Creates a new chunk from all open chunk data
     */
    def acceptChunk(h: (Chunk => Unit)) {
      val digest = digestFactory.builder().append(currentChunk, 0, currentChunkPos).build()
      val c = Chunk(this.currentChunkPos, digest, None)
      h(c)
      currentChunkPos = 0
    }

    /**
     * Chunks the data
     */
    def chunk(data: ByteBuffer)(h: (Chunk => Unit)) {
      for (i <- 0 until data.limit) {
        currentChunk(this.currentChunkPos) = data.get()
        currentChunkPos += 1
        if (currentChunkPos >= chunkSize) {
          acceptChunk(h)
        }
      }
    }

    /**
     * Closes the chunker session
     */
    def close()(h: (Chunk => Unit)) {
      if (currentChunkPos > 0) {
        acceptChunk(h)
      }
    }
  }
}
