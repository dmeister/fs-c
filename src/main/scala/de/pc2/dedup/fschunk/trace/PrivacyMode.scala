package de.pc2.dedup.fschunk.trace
import org.apache.commons.codec.digest.DigestUtils

trait PrivacyMode {
  /* encodes a filename to the output privacy-preserving filename (or outputs the original filename directory for
   * no privacy)
  */
  def encodeFilename(filename: String): String
}

/**
 * Enumeration for the trace privacy mode
 * - NoPrivacy: Emits full path
 * - FlatDefault: Revertable full path hashing
 * - FlatSHA1: SHA-1 full path hashing
 * - DirectorySHA11: Directory-level based SHA1-
 */
object PrivacyMode extends Enumeration {
  object FlatDefault extends PrivacyMode {
    def encodeFilename(filename: String): String = {
      "" + filename.hashCode
    }
  }

  object FlatSHA1 extends PrivacyMode {
    def encodeFilename(filename: String): String = {
      DigestUtils.shaHex(filename)
    }
  }

  object NoPrivacy extends PrivacyMode {
    def encodeFilename(filename: String): String = filename
  }

  object DirectorySHA1 extends PrivacyMode {
    def encodeFilename(filename: String): String = {
      filename.split("/").map(DigestUtils.shaHex).mkString("/")
    }
  }
}
