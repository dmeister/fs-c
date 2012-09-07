package de.pc2.dedup.fschunk.handler.direct

import de.pc2.dedup.chunker.File
import de.pc2.dedup.chunker.FilePart
import de.pc2.dedup.fschunk.handler.FileDataHandler

/**
 * Handler that used and updates the given chunk index.
 * Usually used in combination with other handlers.
 */
class DeduplicationHandler(val d: ChunkIndex) extends FileDataHandler {
  var lock: AnyRef = new Object()

  def handle(fp: FilePart) {
    lock.synchronized {
      for (c <- fp.chunks) {
        if (!d.check(c.fp)) {
          d.update(c.fp)
        }
      }
    }
  }

  def handle(f: File) {
    lock.synchronized {
      for (c <- f.chunks) {
        if (!d.check(c.fp)) {
          d.update(c.fp)
        }
      }
    }
  }
}

