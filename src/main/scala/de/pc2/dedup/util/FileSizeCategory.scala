package de.pc2.dedup.util

object FileSizeCategory {
  private val log_2 = Math.log(2);

  def getCategory(fileSize: Long): Long = {
    val l = log2(fileSize);
    return Math.pow(2, Math.floor(l)).toLong;
  }

  private def log2(value: Double): Double = {
    return Math.log(value) / log_2;
  }
}
