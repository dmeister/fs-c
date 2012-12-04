package de.pc2.dedup.fschunk

import org.scalatest._
import de.pc2.dedup.fschunk.format._
import de.pc2.dedup.fschunk.trace._
import de.pc2.dedup.chunker.rabin._
import de.pc2.dedup.util._

object Test {
  def main(args: Array[String]): Unit = {
    (new DirectoryProcessorTest).execute()
    (new RabinChunkerTest).execute()
    (new FileTypeTest).execute()
    (new PolynomUtilTest).execute()
  }
}
