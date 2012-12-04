package de.pc2.dedup.util

import org.scalatest._
import org.scalatest.matchers._
import org.scalatest.junit._
import java.io.File

class FileTypeTest extends FunSuite with ShouldMatchers {

  test("file type normalization") {
    val list = Map(
      ".asadmintruststore" -> "---",
      "src/timer.d" -> "d",
      "resume.DOC" -> "doc",
      "resume.DOC~" -> "doc")

    for ((name, fileType) <- list) {

      val nft = FileType.getNormalizedFiletype(new File(name))
      nft should be(fileType)
    }
  }
}
