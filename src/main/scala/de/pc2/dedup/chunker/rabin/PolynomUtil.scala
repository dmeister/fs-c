package de.pc2.dedup.chunker.rabin

/**
 * Utility class for Polynom operations on a 64-bit integer.
 * In the C++ version, I used the polynom operations provided by cryptopp
 *
 * References: Donald Knuth, The Art of Computer Programming, Volume 2
 */
object PolynomUtil {

  private def mult(x: Long, y: Long): (Long, Long) = {
    var hi = 0L

    var lo = if (testBit(x, 0)) {
      y
    } else {
      0L
    }
    
    for (i <- 1 until 64) {
      if (testBit(x, i)) {
        lo ^= y << i
        hi ^= y >> (64 - i)
      }
    }
    (hi, lo)
  }

  def mod(hi: Long, lo: Long, p: Long): Long = {
    var lo_ = lo
    var hi_ = if (testBit(hi, 63)) {
      hi ^ p;
    } else {
      hi
    }
    for (i <- 62 to 0 by -1) {
      if (testBit(hi_, i)) {
        hi_ ^= p >> (63 - i)
        lo_ ^= p << (i + 1)
        0
      }
    }
    if (testBit(lo_, 63)) {
      lo_ ^= p;
    }
    lo_
  }

  def modmult(x: Long, y: Long, p: Long): Long = {
    val (hi, lo) = mult(x, y)
    mod(hi, lo, p)
  }

  private def testBit(x: Long, b: Int): Boolean = (x & (1L << b)) != 0
}
