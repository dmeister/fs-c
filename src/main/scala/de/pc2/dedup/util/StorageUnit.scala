package de.pc2.dedup.util

object StorageUnit {
  val KB = 1024.0
  val MB = 1024.0 * 1024.0
  var GB = 1024.0 * 1024.0 * 1024.0

  def apply(bytes: Long): String = {
    if (bytes > GB) {
      return "%.2fGB".format(bytes / GB)
    }
    if (bytes > MB) {
      return "%.2fMB".format(bytes / MB)
    }
    if (bytes > KB) {
      return "%.2fKB".format(bytes / KB)
    }
    return bytes.toString
  }
}