package de.pc2.dedup.chunker

/**
 * Represents a file.
 *
 * @param filename filename (or hash of a filename in privacy mode)
 * @param fileSize logical file size in bytes
 * @param fileType type of the file or "" if the file has no type
 * @param chunks list of chunks that form the file contents
 * @param  label an optional label for the file
 */
case class File(filename: String, fileSize: Long, fileType: String, chunks: List[Chunk], label: Option[String]) {
  if (fileSize < 0) {
    throw new IllegalArgumentException("Illegal file size " + fileSize)
  }
}
