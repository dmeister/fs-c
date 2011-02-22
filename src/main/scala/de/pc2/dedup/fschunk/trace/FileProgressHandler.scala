package de.pc2.dedup.fschunk.trace

import java.io.FileOutputStream
import java.io.OutputStreamWriter
import java.util.concurrent.locks.ReentrantLock

class FileProgressHandler(progressFilename : String) {
    val stream = new FileOutputStream(progressFilename, true)
    val writer = new OutputStreamWriter(stream)
    val channel = stream.getChannel()
    val processLock = new ReentrantLock()
    
    def progress(f: de.pc2.dedup.chunker.File) : Unit = {
        processLock.lock()
        try {
            val fileLock = channel.lock()
        
            writer.write(f.filename)
            writer.write("\n")
            writer.flush()
            fileLock.release()
        } finally {
            processLock.unlock()
        }
    }
}