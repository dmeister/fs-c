package de.pc2.dedup.chunker

import java.io._
import java.security.MessageDigest

/**
 * Creates a digest of all chunk hashes of a file. Used to emulate
 * a hash over the whole file contents
 */
object FileDigest {
  private val md = MessageDigest.getInstance("SHA-1")

  /**
   * Creates a overall digest from a list of digests.
   * May be used for a full file deduplication analysis
   */
  def createFromChunkHashes(hashes: List[Digest]): Digest = {
    for (d <- hashes) {
      md.update(d.digest)
    }
    new Digest(md.digest())
  }
}