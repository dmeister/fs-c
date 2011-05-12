package de.pc2.dedup.chunker

/**
 * Exception throws if an error ocurred during the chunking process.
 */
class ChunkerException(message: String, cause: Throwable) extends Exception {
    def this(message: String) = this(message, null)
}
