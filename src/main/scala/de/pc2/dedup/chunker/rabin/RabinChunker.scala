package de.pc2.dedup.chunker.rabin

import java.math.BigInteger
import java.nio.ByteBuffer

import scala.math.pow

import de.pc2.dedup.chunker.Chunk
import de.pc2.dedup.chunker.Chunker
import de.pc2.dedup.chunker.ChunkerSession
import de.pc2.dedup.chunker.DigestFactory
import de.pc2.dedup.util.Log

/**
 * Rabin Chunking
 */
class RabinChunker(minimalSize: Int,
  averageSize: Int,
  maximalSize: Int,
  logChunkHashes: Boolean,
  digestFactory: DigestFactory,
  val chunkerName: String) extends Chunker with Log {

  /**
   * windows rabin instance
   */
  private val rabinWindow = new RabinWindow(Rabin.createDefaultRabin(), 48)

  /**
   * breakmark bit pattern
   */
  private val breakmark: Long = (pow(2.0, BigInteger.valueOf(averageSize).bitLength() - 1) - 1).toLong
  val positionWindowBeforeMin: Int = minimalSize - 48

  /**
   * Creates a new rabin chunking session
   */
  def createSession(): ChunkerSession = new RabinChunkerSession(chunkerName)

  /**
   * session class
   */
  private class RabinChunkerSession(val chunkerName: String) extends ChunkerSession {
    val rabinSession = rabinWindow.createSession()
    val overflowChunk = new Array[Byte](maximalSize)
    var overflowChunkPos: Int = 0
    val digestBuilder = digestFactory.builder()

    /**
     * Also, adds the rabin fingerprint (hash) to the chunk. Usually the rabin hash has
     * the hash breakmark as a suffix, but this is not the case when the chunk has been
     * closed due to size limitations
     */
    def acceptChunk(h: (Chunk => Unit), nonChunkedData: ByteBuffer) {
      def getChunkHash(): Option[Long] = {
        if (logChunkHashes) {
          Some(this.rabinSession.fingerprint)
        } else {
          None
        }
      }
      val c = if (nonChunkedData != null) {
        val remaining = nonChunkedData.remaining
        val digest = digestBuilder.append(overflowChunk, 0, overflowChunkPos).append(nonChunkedData).build()
        Chunk(this.overflowChunkPos + remaining, digest, getChunkHash())
      } else {
        val digest = digestBuilder.append(overflowChunk, 0, overflowChunkPos).build()
        Chunk(this.overflowChunkPos, digest, getChunkHash())
      }
      h(c)
      rabinSession.clear()
      overflowChunkPos = 0
    }

    def debugString(b: ByteBuffer): String = {
      return "position %s, limit %s, capacity %s".format(b.position, b.limit, b.capacity)
    }

    /**
     * Chunks the data
     */
    def chunk(data: ByteBuffer)(h: (Chunk => Unit)) {
      try {
        var current = 0
        var nonChunkedData: ByteBuffer = data.slice()
        var break = false;

        while (current < data.limit && !break) {
          var todo = data.limit - current
          var countToMax = maximalSize - overflowChunkPos
          if (todo > countToMax) {
            todo = countToMax
          }
          var countToMin = positionWindowBeforeMin - overflowChunkPos

          if (countToMin > todo) {
            // break (but Scala has no break)
            break = true
          } else {
            if (countToMin < 0) {
              countToMin = 0
            }
            val end = current + todo
            current += countToMin
            var innerBreak = false
            data.position(current)
            while (current < end && !innerBreak) {
              val b = data.get()
              val value: Int = if (b < 0) {
                256 + b
              } else {
                b
              }
              current += 1
              this.rabinSession.append(value)
              val isBreakmark = (this.rabinSession.fingerprint & breakmark) == breakmark;
              if (isBreakmark) {
                nonChunkedData.limit(data.position)
                acceptChunk(h, nonChunkedData)
                innerBreak = true
              }
            }

            // end of inner loop
            if (current == end && todo == countToMax) {
              nonChunkedData.limit(data.position)
              acceptChunk(h, nonChunkedData)
            }
          }
        }
        // end of outer loop

        if (nonChunkedData.position < nonChunkedData.capacity) {
          nonChunkedData.limit(nonChunkedData.capacity)

          if (nonChunkedData.remaining > 0) {
            val remaining = nonChunkedData.remaining
            nonChunkedData.get(overflowChunk, overflowChunkPos, nonChunkedData.remaining)
            overflowChunkPos += remaining
          }
        }
      } catch {
        case e: Exception =>
          logger.error("Chunking error".format(e))
          throw e
      }
    }
    /**
     * Closes the rabin chunker
     */
    def close()(h: (Chunk => Unit)) {
      if (overflowChunkPos > 0) {
        acceptChunk(h, null)
      }
    }
  }
}
