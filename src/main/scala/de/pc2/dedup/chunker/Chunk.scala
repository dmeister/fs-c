package de.pc2.dedup.chunker

/**
 * Represents a single chunk
 *
 * @size size of the chunk
 * @fp fingerprint of the chunk data
 * @chunkHash optional hash of the chunk at the end of the chunk. Used only by the rabin chunker. It then
 * contains the rabin fingerprint.
 */
case class Chunk(val size: Int, val fp: Digest, val chunkHash: Option[Long]) {
  if (size < 0) {
    throw new IllegalArgumentException("Illegal size %s".format(size))
  }
}