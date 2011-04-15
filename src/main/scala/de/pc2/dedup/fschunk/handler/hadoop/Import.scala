package de.pc2.dedup.fschunk.handler.hadoop

import scala.collection.mutable._
import de.pc2.dedup.util.FileSizeCategory 
import de.pc2.dedup.chunker._
import de.pc2.dedup.fschunk.parse._
import java.io.BufferedWriter
import java.io.FileWriter
import scala.actors.Actor
import de.pc2.dedup.util.StorageUnit
import scalax.io._
import scalax.io.CommandLineParser
import pc2.dedup.util.SystemExitException
import org.apache.hadoop.fs._
import org.apache.hadoop.io._
import org.apache.hadoop.conf.Configuration
import java.net.URI
import org.apache.commons.codec.binary.Base64
import de.pc2.dedup.util.Log
import de.pc2.dedup.fschunk.Reporting
import de.pc2.dedup.fschunk.format.Format
import de.pc2.dedup.fschunk.handler.FileDataHandler
import java.io.OutputStream
import org.apache.hadoop.io.compress.BZip2Codec

class ImportHandler(filesystemName: String, filename : String, compress: Boolean) extends Reporting with FileDataHandler with Log {
	def createCompressedStream(fs: FileSystem, filepath: Path) : OutputStream = {
            val codec = new BZip2Codec()
            val rawStream = fs.create(filepath)
            codec.createOutputStream(rawStream)
        }
        val conf = new Configuration()
	val fs = FileSystem.get(new URI(filesystemName), conf)

	val rootPath = new Path(filesystemName, filename)
	val chunkPath = if (compress) {
            new Path(rootPath, "chunks.bz2")
        } else {
            new Path(rootPath, "chunks")
        }
        val filePath = if(compress) {
            new Path(rootPath, "files.bz2")
        } else {
            new Path(rootPath, "files")
        }

	val base64 = new Base64()
	var totalFileSize = 0L
	var totalFileCount = 0L
	var totalChunkCount = 0L
	val startTime = System.currentTimeMillis()


        logger.debug("Start")
    	logger.info("Write path %s".format(rootPath))
    	if(fs.exists(filePath)) {
	    logger.warn("Overwritting " + filePath)
	    fs.delete(filePath)
	}
  	if(fs.exists(chunkPath)) {
	    logger.warn("Overwritting " + chunkPath)
	    fs.delete(chunkPath)
	}

	val fileWriter = if (compress) {
            createCompressedStream(fs, filePath)
        } else {
            fs.create(filePath)  
        }
    	val chunkWriter = if(compress) {
            createCompressedStream(fs, chunkPath)
        } else {
            fs.create(chunkPath)
        }

	override def report() {
            val secs = ((System.currentTimeMillis() - startTime) / 1000)
	    if(secs > 0) {
	        val mbs = totalFileSize / secs
		val fps = totalFileCount / secs
		logger.info("File Count: %d (%d f/s), File Size %s (%s/s), Chunk Count: %d".format(
		    totalFileCount, 
		    fps,
		    StorageUnit(totalFileSize), 
		    StorageUnit(mbs),
		    totalChunkCount))
	    }
	}

        def handle(fp: FilePart) {
	    if(logger.isDebugEnabled) {
	        logger.debug("Write file %s (partial)".format(fp.filename))
	    }
	    for(chunk <- fp.chunks) {
	        val fingerprint = base64.encode(chunk.fp.digest)		      	
		val chunkSize = chunk.size
		val chunkline = "%s\t%s\t%s%n".format(fp.filename, new String(fingerprint), chunkSize)
		chunkWriter.write(chunkline.getBytes("UTF-8"))
            }
	    totalChunkCount += fp.chunks.size		
        }

        def handle(f: File) {
    	    if(logger.isDebugEnabled) {
	        logger.debug("Write file %s".format(f.filename))
	    }
	    val l = f.label match {
                case Some(s) => s
                case None => ""
            }
	    val fileline = "%s\t%s\t%s\t%s%n".format(f.filename, f.fileSize, f.fileType, l)
	    fileWriter.write(fileline.getBytes("UTF-8"))

	    for(chunk <- f.chunks) {
	        val fp = base64.encode(chunk.fp.digest)		      	
		val chunkSize = chunk.size
		val chunkline = "%s\t%s\t%s%n".format(filename, new String(fp), chunkSize)
		chunkWriter.write(chunkline.getBytes("UTF-8"))
	    }
	    totalFileSize += f.fileSize
	    totalFileCount += 1
	    totalChunkCount += f.chunks.size
        }

        override def quit() {
            fileWriter.close()
	    chunkWriter.close()
	    report()
	    logger.debug("Exit")

        }
}

object Import {
	def main(args : Array[String]) : Unit = {
			object Options extends CommandLineParser {
				val output =  new StringOption('o',"output", "HDFS directory for output") with AllowAll
				val filename =  new StringOption('f',"filename", "Trace filename") with AllowAll
				val report = new IntOption(None, "report", "Interval between progess reports in seconds (Default: 1 minute, 0 = no report)") with AllowAll
				val format = new StringOption(None, "format", "Input/Output Format (protobuf, legacy, default: protobuf)") with AllowAll
			} 
			try {
				Options.parseOrHelp(args) { cmd =>
				try {
					val filenames = cmd.all(Options.filename)
					val output = cmd(Options.output) match {
					case None => 
					Options.showHelp(System.out)
					throw new SystemExitException()
					case Some(s) => s
					}
					val reportInterval = cmd(Options.report) match {
					    case None => 60 * 1000
					    case Some(i) => i * 1000
					}
					val format = cmd(Options.format) match {
						case None => "protobuf"
						case Some(s) => if(Format.isFormat(s)) {
							s
						} else {
							throw new MatchError("Unsupported format")
						}
					}
					if(filenames.size == 0) {
						throw new SystemExitException()
					}
					for(filename <- filenames) {
					    val importHandler = new ImportHandler(output, output, true)
					    val reader = Format(format).createReader(filename, importHandler)
					    val reporter = new ObjectReporter(importHandler, reportInterval).start() 
					    reader.parse()

                                            reporter ! Quit
                                            importHandler.quit()
					}
				} catch {
				case e: MatchError => 
				Options.showHelp(System.out)
				throw new SystemExitException()
				}
				}
			} catch {
			case e: SystemExitException => System.exit(1)
			}
	}
}
