package de.pc2.dedup.chunker

/*
 * Represents a file
 * @filename filename (or hash of a filename in privacy mode)
 * @fileSize logical file size in bytes
 * @fileType type of the file or "" if the file has no type
 * @chunks list of chunks that form the file contents
 */
case class File(filename: String, fileSize: Long, fileType: String, chunks: List[Chunk], label: Option[String]) {
  if(fileSize < 0) {
    throw new IllegalArgumentException("Illegal file size " + fileSize)
  }
}
