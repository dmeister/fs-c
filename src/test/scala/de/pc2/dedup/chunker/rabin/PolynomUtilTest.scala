package de.pc2.dedup.chunker.rabin

import org.scalatest._
import org.scalatest.matchers._
import org.scalatest.junit._

class PolynomUtilTest extends FunSuite with ShouldMatchers {

  test("multi with one") {
    val a = 1L
    val b = 4604570691050114435L
    val p = -4618801345804661373L

    val value = PolynomUtil.modmult(a, b, p)
    value should be(b)
  }
}
