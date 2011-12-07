package de.pc2.dedup.chunker;

import java.util.Arrays
import java.security.MessageDigest
import java.security.NoSuchAlgorithmException
import java.nio.ByteBuffer
import java.math.BigInteger

/**
 * Compation object to create fingerprints of a chunk or a file
 */
class DigestFactory(val digestType: String, val digestLength: Int) {

  /**
   * Tests if the digest type is valid
   */
  def testDigestType() {
    try {
      val md = MessageDigest.getInstance(digestType)
      if (digestLength > md.getDigestLength) {
        throw new IllegalArgumentException("Digest length larger than 20 not allowed")
      }
    } catch {
      case e: NoSuchAlgorithmException =>
        throw new IllegalArgumentException("Digest type not known");
    }
  }
  testDigestType()

  /**
   * builder class for digests
   */
  class DigestBuilder {
    val md = MessageDigest.getInstance(digestType)

    /**
     * Append new bytes to the current digest builder
     */
    def append(buf: Array[Byte], pos: Int, len: Int): DigestBuilder = {
      if (len > 0) {
        md.update(buf, pos, len)
      }
      return this
    }
    
    def append(buf: ByteBuffer): DigestBuilder = {
    	md.update(buf)
      return this
    }

    /**
     * create a new digest from the current data. Rests the digest builder
     */
    def build(): Digest = {
      val fullDigest = md.digest()
      val digest = if (digestLength == md.getDigestLength) {
        fullDigest
      } else {
        val d = new Array[Byte](digestLength)
        System.arraycopy(fullDigest, 0, d, 0, digestLength)
        d
      }
      md.reset()
      return new Digest(digest)

    }
  }

  /**
   * Creates a new digest builder
   */
  def builder(): DigestBuilder = {
    return new DigestBuilder()
  }
}
/**
 * Fingerprint of a chunk or a file.
 * The reason not to use a byte array directly is that hashCode and equals has
 * not the expected behavior on a byte array
 */
case class Digest(digest: Array[Byte]) {
  /**
   * Hashcode of the digest. Calls Arrays.hashCode()
   */
  override def hashCode: Int = return Arrays.hashCode(digest)

  /**
   * Checks if two digests are equal. Calls Array.equal
   */
  override def equals(o: Any): Boolean = {
    o match {
      case Digest(fp) => Arrays.equals(this.digest, fp)
      case _ => false
    }
  }
  
  override def toString() : String = {
      val bi = new BigInteger(1, digest)
      val result = bi.toString(16)
      if (result.length() % 2 != 0) {
          "0" + result
      } else {
          result
      }
  }
}
