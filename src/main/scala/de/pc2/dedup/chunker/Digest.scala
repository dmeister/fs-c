package de.pc2.dedup.chunker;


import java.util.Arrays
import java.security.MessageDigest
import java.security.NoSuchAlgorithmException

/**
 * Compation object to create fingerprints of a chunk or a file
 */
class DigestFactory(val digestType: String, val digestLength: Int) {
	def testDigestType() {
		try {
			val md = MessageDigest.getInstance(digestType)
			if(digestLength > md.getDigestLength) {
				throw new IllegalArgumentException("Digest length larger than 20 not allowed")
			}
		} catch {
			case e: NoSuchAlgorithmException =>
				throw new IllegalArgumentException("Digest type not known");
		}
	}
	testDigestType()

  /**
   * Creates a SHA-1 digest of the first len bytes of the buf array
   */
  def createFromData(buf: Array[Byte], len: Int) : Digest = {
    val md = MessageDigest.getInstance(digestType)
    md.update(buf, 0, len)
    val fullDigest = md.digest()
    val digest = if(digestLength == md.getDigestLength) {
    	fullDigest
    } else { 
    	val d = new Array[Byte](digestLength)
    	System.arraycopy(fullDigest, 0, d, 0, digestLength)
    	d
    }    
    return new Digest(digest)
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
  override def hashCode : Int = return Arrays.hashCode(digest)

    /**
     * Checks if two digests are equal. Calls Array.equal
     */
    override def equals(o: Any) : Boolean = {
        o match {
            case Digest(fp) => Arrays.equals(this.digest, fp)
            case _ => false
        }
    }
}
     