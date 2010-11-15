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
import de.pc2.dedup.fschunk.format.Format

class ImportHandler(filesystemName: String, filename : String) extends Actor with Log {
	trapExit = true
	val conf = new Configuration()
	val fs = FileSystem.get(new URI(filesystemName), conf)

	val rootPath = new Path(filesystemName, filename)
	val chunkPath = new Path(rootPath, "chunks")
	val filePath = new Path(rootPath, "files")

	val base64 = new Base64()
	var totalFileSize = 0L
	var totalFileCount = 0L
	var totalChunkCount = 0L
	val startTime = System.currentTimeMillis()

	def report() {
		val secs = ((System.currentTimeMillis() - startTime) / 1000)
		if(secs > 0) {
			val mbs = totalFileSize / secs
			val fps = totalFileCount / secs
			logger.info("File Count: %d (%d f/s), File Size %s (%s/s), Chunk Count: %d, Queue: %d".format(
					totalFileCount, 
					fps,
					StorageUnit(totalFileSize), 
					StorageUnit(mbs),
					totalChunkCount,
					mailboxSize))
		}
	}


	def act() {
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
		val fileWriter = fs.create(filePath)  
		val chunkWriter = fs.create(chunkPath)
		
		while(true) {
			receive { 
			case Report =>
			report()
			case File(filename, fileSize, fileType, chunks) =>
			if(logger.isDebugEnabled) {
				logger.debug("Write file %s".format(filename))
			}
			val fileline = "%s\t%s\t%s%n".format(filename, fileSize, fileType)

			fileWriter.write(fileline.getBytes("UTF-8"))

			for(chunk <- chunks) {
				val fp = base64.encode(chunk.fp.digest)		      	
				val chunkSize = chunk.size
				val chunkline = "%s\t%s\t%s%n".format(filename, new String(fp), chunkSize)

				chunkWriter.write(chunkline.getBytes("UTF-8"))
			}
			totalFileSize += fileSize
			totalFileCount += 1
			totalChunkCount += chunks.size
			case Quit =>
			fileWriter.close()
			chunkWriter.close()
			report()
			logger.debug("Exit")
			exit()
			case msg : Any =>
			logger.warn("Unknown message " + msg)
			} 
		}
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
						val p = new Parser(filename,format,  
								new ImportHandler(output, output).start() :: Nil).start()
								val reporter = new Reporter(p, reportInterval).start() 
								p
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
