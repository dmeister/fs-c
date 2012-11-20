package de.pc2.dedup.fschunk.trace
import org.scalatest.FunSuite
import org.scalatest.matchers.ShouldMatchers

class PrivacyModeTest extends FunSuite {

  test("no privacy") {
    assert(PrivacyMode.NoPrivacy.encodeFilename("a/a.txt") === "a/a.txt")
  }

  test("flat default") {
    assert(PrivacyMode.FlatDefault.encodeFilename("a/a.txt") != "a/a.txt")
  }

  test("flat sha1") {
    assert(PrivacyMode.FlatSHA1.encodeFilename("a/a.txt") != "a/a.txt")
  }

  test("directory sha1") {
    println(PrivacyMode.DirectorySHA1.encodeFilename("a/a.txt"))
    assert(PrivacyMode.DirectorySHA1.encodeFilename("a/a.txt").matches(".*/.*"))
  }
}