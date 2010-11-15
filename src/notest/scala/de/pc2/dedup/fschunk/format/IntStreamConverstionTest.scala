package de.pc2.dedup.fschunk.format

import org.scalatest._
import org.scalatest.matchers._
import org.scalatest.junit._ 
import java.io._
import scala.collection.mutable.ListBuffer 
import java.nio.charset.Charset

class IntStreamConverstionTest extends FunSuite with ShouldMatchers {
	val ARRAY = createFourArray()

	def createFourArray() : Array[Char] = {
			val b = new Array[Char](4)
			b(0) = 139
			b(1) = 10
			b(2) = 0
			b(3) = 0
			return b
	}
 
	test("array to number") {
	  val value = IntStreamConverstion(ARRAY, 0)
	  value should be (2699)
	}
 
	test("number to array") {
	  val value = 2699
	  val s = IntStreamConverstion(value)
	  s(0) should be (-256+139)
	  s(1) should be (10)
	  s(2) should be (0)
	  s(3) should be (0)
	}
 
  test("roundtrip") {
    val ba = IntStreamConverstion(10244)
    val s = new ByteArrayInputStream(ba)
    val reader = new BufferedReader(new InputStreamReader(s, Charset.forName("ISO-8859-1")));
    val buffer = new Array[Char](4);
	reader.read(buffer);
	val value = IntStreamConverstion(buffer, 0)
	value should be (10244)
   }
}
