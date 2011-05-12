package de.pc2.dedup.chunker

/**
 * Represents a single chunk
 * 
 * @size size of the chunk
 * @fp fingerprint of the chunk data
 */
case class Chunk(val size: Int, val fp : Digest)  {
    if(size < 0) {
        throw new IllegalArgumentException("Illegal size" + size)
    }
}