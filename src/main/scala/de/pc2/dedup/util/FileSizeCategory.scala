package de.pc2.dedup.util

object FileSizeCategory {
  private val log_2 = scala.math.log(2);

  def getCategory(fileSize: Long): Long = {
    val l = log2(fileSize);
    return scala.math.pow(2, scala.math.floor(l)).toLong;
  }

  private def log2(value: Double): Double = {
    return scala.math.log(value) / log_2;
  }
}
