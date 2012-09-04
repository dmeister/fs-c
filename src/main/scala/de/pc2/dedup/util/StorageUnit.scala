package de.pc2.dedup.util

object StorageUnit {
  val KB = 1024.0
  val MB = 1024.0 * 1024.0
  var GB = 1024.0 * 1024.0 * 1024.0

  def apply(bytes: Long): String = {
    if (bytes > GB) {
      return "%.2fG".format(bytes / GB)
    }
    if (bytes > MB) {
      return "%.2fM".format(bytes / MB)
    }
    if (bytes > KB) {
      return "%.2fK".format(bytes / KB)
    }
    return bytes.toString
  }

  def fromString(input: String): Long = {
    val multi = if (input.last == 'K' || input.last == 'k') {
      1024L
    } else if (input.last == 'M' || input.last == 'm') {
      1024 * 1024L
    } else if (input.last == 'G' || input.last == 'g') {
      1024L * 1024 * 1024L
    } else if (input.last == 'T' || input.last == 't') {
      1024L * 1024L * 1024 * 1024L
    } else {
      1
    }

    if (multi == 1) {
      return input.toLong
    } else {
      return input.slice(0, input.length - 1).toLong * multi
    }
  }
}