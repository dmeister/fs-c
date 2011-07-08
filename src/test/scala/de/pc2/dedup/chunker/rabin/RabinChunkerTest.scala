package de.pc2.dedup.chunker.rabin

import org.scalatest._
import org.scalatest.matchers._
import org.scalatest.junit._ 
import scala.math.pow
import java.io.FileInputStream
import java.io.IOException
import java.io.InputStream
import java.math.BigInteger
import java.security.MessageDigest
import java.security.NoSuchAlgorithmException
import java.util.Arrays
import java.util.LinkedList
import java.util.List

import de.pc2.dedup.chunker.Chunk
import de.pc2.dedup.chunker.Chunker
import de.pc2.dedup.chunker.Digest

class RabinChunkerTest extends FunSuite with ShouldMatchers {

	test("breakmark calculation") {
		val averageSize = 8 * 1024
		val breakmark = (pow(2.0, BigInteger.valueOf(averageSize).bitLength()-1)-1).toInt
		
		(breakmark + 1) should be (averageSize)
	}
	
	test("default polynom") {
		val p = -4618801345804661373L
		val rabin = Rabin.createDefaultRabin()
		rabin.polynom should be (p)
	}
	
	test("table calcuation") {
		val rabin = Rabin.createDefaultRabin()
		val T = rabin.createModTable(rabin.polynom)
		
		T(0) should be (0L)
		T(1) should be (-4618801345804661373L)
		T(2) should be (9209141382100228870L)
		
		T(10) should be (4671215262765932955L)
		T(11) should be (-56922619018187752L)
	}
	
	test("invert table calculation") {
		val window = new RabinWindow(Rabin.createDefaultRabin(), 48)		
		val invertTable = window.invertTable
		
		invertTable(1) should be (3516854836665139219L)
		invertTable(131) should be (846774551824259960L)		
	}
	
	test("two appends") {
	  		val window = new RabinWindow(Rabin.createDefaultRabin(), 48)	
		val sess = window.createSession()

		for(i <- 0 until 128) {
			  sess.append(2)
		  }
		val oldFingerprint = sess.fingerprint
		for(i <- 128 until 256) {
			  sess.append(2)
			  val fingerprint : Long = sess.fingerprint 
			  	  
			  fingerprint should be (oldFingerprint)
		  }
	}
	
	test("rabin hash calculation") {
		val i = 0
		
		val rabin = Rabin.createDefaultRabin()
		val file = this.getClass().getClassLoader().getResourceAsStream("rabin-test")
			  
		val buffer = new Array[Byte](65536)
		file.read(buffer)
		file.close()
		
		var fingerprint = 0L
		for(i <- 0 until 256) {
			val value = if(buffer(i) >= 0) {
				buffer(i).toInt
			} else {
				256 + buffer(i).toInt
			}
			fingerprint = rabin.append(fingerprint, value)
		}
		val fp1 = fingerprint
		
		fingerprint = 0
		for(i <- 0 until 256) {
			val value = if(buffer(i) >= 0) {
				buffer(i).toInt
			} else {
				256 + buffer(i).toInt
			}
			fingerprint = rabin.append(fingerprint, value)
		}
		val fp2 = fingerprint
		fp2 should be (fp1)
	}
	
	test("rabin window calculation") {
		val window = new RabinWindow(Rabin.createDefaultRabin(), 48)
		val file = this.getClass().getClassLoader().getResourceAsStream("rabin-test")
		
		val buffer = new Array[Byte](65536)
		file.read(buffer)
		file.close()
		  
		val sess1 = window.createSession()
		for(i <- 0 until 128) {
			val value = if(buffer(i) >= 0) {
				buffer(i).toInt
			} else {
				256 + buffer(i).toInt
			}
			sess1.append(value)
		}
		val fp1 = sess1.fingerprint
		
		sess1.clear()
		for(i <- 12 until 128) {
			val value = if(buffer(i) >= 0) {
				buffer(i).toInt
			} else {
				256 + buffer(i).toInt
			}
			sess1.append(value)
		}
		
		val fp2 = sess1.fingerprint
		fp2 should be (fp1)
	}
}
