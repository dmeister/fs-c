package de.pc2.dedup.chunker

/**
 * Trait of all chunker sessions.
 */
trait ChunkerSession {  
    /**
     * chunks "size" bytes of the data array.
     * The session may contain data from that array in an additinal open chunk.
     */
    def chunk(data: Array[Byte], size: Int)(h: (Chunk => Unit))
  
    /**
     * Return additional chunks that stayed open after chunk calls. The
     * chunker session should not contain any state after calling close. However,
     * it should be ready to process new chunk calls.
     */
    def close()(h: (Chunk => Unit))

    def chunkerName() : String
}

/**
 * Trait for all chunker implementations.
 * All thread bounded operations should be performed in a chunker session instance.
 */
trait Chunker {
    /**
     * Creates a new chunker session
     */
    def createSession() : ChunkerSession

    def chunkerName() : String
}       
