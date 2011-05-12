package de.pc2.dedup.fschunk.trace

import java.io._
import scala.collection.mutable.ListBuffer

case class FilenameLabel(val filename: String, val label: Option[String])

object FileListingProvider {
    // we always try to follow initial symlinks
    def g(filename: String, label: Option[String], f: (FilenameLabel) => Unit) : Unit = {
        try {
            val fl = FilenameLabel(new File(filename).getCanonicalPath(), label)
            f(fl)
        } catch {
            case _ =>
                val fl = FilenameLabel(filename, label)
                f(fl)
        }
    }
	
    def fromDirectFile(filenames: List[String], label: Option[String]) : FileListingProvider = {
        class DirectFileProvider extends  FileListingProvider {
            def foreach (f : (FilenameLabel) => Unit) : Unit = {
                filenames.foreach(filename => g(filename, label, f)) 
            }
        } 
        new DirectFileProvider() 
    } 
    def fromListingFile(filenames: List[String], defaultLabel: Option[String]) : FileListingProvider = {
        class ListingFileProvider extends  FileListingProvider {
            var reader: BufferedReader = null;
            var queue = new ListBuffer[FilenameLabel]()  
            filenames.foreach(f => appendFile(f))
            def appendFile(f: String) {
                try {
                    reader = new BufferedReader(new FileReader(f))
                    var line = reader.readLine();
                    while(line != null) {  
                        val lio = line.lastIndexOf("=")
                        val fl = if (lio >= 0 && lio < line.size) {
                            // a label is provided
                            FilenameLabel(line.substring(0, line.lastIndexOf("=")), Some(line.substring(line.lastIndexOf("=")+1)))
                        } else {
                            FilenameLabel(line, defaultLabel)
                        }
                        queue.append(fl)
                        line = reader.readLine();
                    }
		} catch {
                    case e: IOException =>
			e.printStackTrace();
		} finally {
                    if(reader != null) {
                        try {
                            reader.close();
                        } catch {
                            case e: IOException =>
                        }											
                    }
		}
            }
            def foreach (f : (FilenameLabel) => Unit) : Unit = {
                for(fl <- queue) {
                    g(fl.filename, fl.label, f)
                }
            }
        }
        new ListingFileProvider() 
    }
}

trait FileListingProvider {
    def foreach (f : (FilenameLabel) => Unit) : Unit
}
