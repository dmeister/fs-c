package de.pc2.dedup.fschunk.handler.direct

import de.pc2.dedup.chunker.Digest
import scala.collection.mutable.Set

/**
 * Simulation of a chunk index used for data deduplication
 */
class ChunkIndex() {
  val m = Set.empty[Digest]
  
  /**
   * Adds the digest to the index
   */
  def update(d: Digest) = m += d
  
  /**
   * Checks if the digest is known
   */
  def check(d: Digest) : Boolean = (m contains d)
  
  /**
   * Clears the chunk index 
   */
  def clear() = m.clear
}  
      