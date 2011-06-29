package de.pc2.dedup.chunker

import java.io._
import java.security.MessageDigest

/**
 * Creates a digest of all chunk hashes of a file. Used to emulate
 * a hash over the whole file contents
 */
object FileDigest {
  private val md = MessageDigest.getInstance("SHA-1")
  def createFromChunkHashes(hashes: List[Digest]): Digest = {
    for (d <- hashes) {
      md.update(d.digest)
    }
    new Digest(md.digest())
  }
}