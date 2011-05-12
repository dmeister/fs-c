package de.pc2.dedup.chunker.rabin

import java.math.BigInteger
/**
 * Rabin Fingerprint implementation using a lookup table.
 * After construction the object is immutable and can savely used in multiple threads.
 * 
 * References: 
 * - C. Chan and H. Lu, "Fingerprinting using poloynomial (rabin's method)," 
 *   CMPUT690 Term Projekt, University of Alberta, December 2001.
 * - A. Broder, Some applications of Rabin's fingerprinting method.
 *   Springer Verlag, 1993, pp. 143-152. 
 * - M. O. Rabin, "Fingerprinting by random polynomials," TR-15-81, 
 *   Center for Research in Computing Technology, Tech. Rep., 1981.
 */
object Rabin {
    val MSD_SET : Long = 1L << 63
  
    /**
     * Creates a new rabin object using the polynom represented by the long
     * 13827942727904890243.
     * @return
     */
    def createDefaultRabin() = new Rabin(new BigInteger("13827942727904890243").longValue())
}  
class Rabin(val polynom: Long) {
    private val T = createModTable(polynom)

    def createModTable(polynom: Long) : Array[Long] = {
        val T = new Array[Long](256)
        val T1 = PolynomUtil.mod(0, Rabin.MSD_SET, polynom)
		
        for(i <- 0 until 256) {
            val v = PolynomUtil.modmult(i, T1, polynom)
            val w = (i.toLong) << 63
            val t = v | w
            T(i.toInt) = t
        }
        return T
    } 
	
    /**
     * Appends the byte in data to the fingerprint.
     * @param fingerprint old fingerprint
     * @param data data that should be in the range 0 <= data <= 255. It represents a single byte.
     * The "byte" type isn't used because byte is signed in Java (see Java Puzzlers Book).
     * 
     * @return
     */
    def append(fingerprint: Long, data: Int) : Long = {
        if(data < 0 || data > 255) {
            throw new IllegalArgumentException("data")
        }
        val shifted = (fingerprint >> 55).toInt
        return ((fingerprint << 8) | data) ^ T(shifted)
    }
}
