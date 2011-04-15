package de.pc2.dedup.fschunk.handler.direct

import de.pc2.dedup.chunker.Chunk
import de.pc2.dedup.chunker.Digest
import scala.actors.Actor
import scala.actors.Actor._
import de.pc2.dedup.chunker.File
import de.pc2.dedup.chunker.FilePart
import de.pc2.dedup.fschunk.handler.FileDataHandler

class DeduplicationHandler(val d: ChunkIndex, val chunkerName: String) extends FileDataHandler {
    var lock : AnyRef = new Object()

    def handle(fp: FilePart) {
        lock.synchronized {
            for(c <- fp.chunks) {
                if (!d.check(c.fp)) {
                    d.update(c.fp)
                }
            }
        }
    }

    def handle(f: File) {
        lock.synchronized {
            for(c <- f.chunks) {
                if (!d.check(c.fp)) {
                    d.update(c.fp)
                }
            }
        }
    }
}
       
