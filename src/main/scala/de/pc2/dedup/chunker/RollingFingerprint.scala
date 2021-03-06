package de.pc2.dedup.chunker

/**
 * Trait for a rolling fingerprint function
 */
trait RollingFingerprint {

  /**
   * Creates a new fingerprint session
   */
  def createSession(): Session

  /**
   * Rolling fingerprint session.
   * A session contains the current state of a fingerprint and should only
   * be called from a single thread.
   */
  trait Session {

    /**
     * Returns the current fingerprint of the session
     */
    def fingerprint: Long

    /**
     * Clears the fingerprint state
     */
    def clear()

    /**
     * Appends a new data element.
     * Throws IllegalArgumentException if data is not in the range [0:255].
     */
    def append(data: Int)
  }
}
