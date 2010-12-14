package de.pc2.dedup.fschunk.handler.direct

import de.pc2.dedup.chunker.Chunk
import de.pc2.dedup.chunker.Digest
import scala.actors.Actor
import scala.actors.Actor._
import de.pc2.dedup.chunker.File

class DeduplicationHandler(val d: ChunkIndex, val chunkerName: String) extends Actor {
  def act() {
    loop {
      react {
        case File(_,_,_,chunks, _) =>
          for(c <- chunks) {
            if(!d.check(c.fp)) {
              d.update(c.fp)
            }
        }
      }
    }
  }
}
       
