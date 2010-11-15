package de.pc2.dedup.chunker.rabin

import de.pc2.dedup.chunker.RollingFingerprint;

/**
 * A rolling fingerprint variant of Rabin's Fingerprinting Method.
 * It used a "invert table" to speed up the processing.
 * 
 * A proof for the correctness of the invert table can be found in "Dirk Meister, Data Deduplication
 * using Flash-based indexes, Master thesis, University of Paderborn, 2009"   
 * 
 */  
class RabinWindow(rabin: Rabin, windowSize: Int) extends RollingFingerprint {
	val invertTable = createInvertTable(rabin, windowSize)
	
	def createInvertTable(rabin: Rabin, windowSize: Int) : Array[Long] = {
	  /**
       * To extract and old byte i from the rabin fingerprint 
       * i * 1 << (63 * 8 + 1) (correct? Double check in thesis) must be "and"-ed to the
       * current fingerprint.
       */
	  def calculateWindowSizeShift(i: Int, s: Long) : Long = {
	    if(i < windowSize) calculateWindowSizeShift(i + 1,rabin.append(s, 0.toByte))
        else s
	  }
      val shift = calculateWindowSizeShift(1,1L)
	  val U = new Array[Long](256)
		 
      val it = for(i <- 0 until 256) yield PolynomUtil.modmult(i, shift, rabin.polynom)
      it.toArray
	}  
	
 /**
  * Creates a new windowed rabin fingerprint session
  */
 def createSession() : Session = new RabinWindowSession()
	
 class RabinWindowSession extends Session  {
   val window = new Array[Int](windowSize)
   var windowPos = -1
   var fingerprint = 0L
		
   def clear() = {	}
		
   def append(data: Int) {
     if(data < 0 || data > 255) {
       throw new IllegalArgumentException("data")
     }
     windowPos = (windowPos + 1) % window.length
     val oldestByte = this.window(this.windowPos)
	 window(this.windowPos) = data
			
    /**
     * Removed oldest byte from fingerprint and adds new byte (data) to it
     */
	fingerprint = rabin.append(fingerprint ^ invertTable(oldestByte), data)
   }
		
	def  getFingerprint() : Long  = this.fingerprint
  }
}
