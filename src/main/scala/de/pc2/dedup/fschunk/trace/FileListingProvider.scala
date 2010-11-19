package de.pc2.dedup.fschunk.trace

import java.io._
import scala.collection.mutable.ListBuffer
 
object FileListingProvider {
	// we always try to follow initial symlinks
	def g(filename: String, f: (String) => Unit) : Unit = {
		try {
			f(new File(filename).getCanonicalPath())
		} catch {
                    case _ => 
			f(filename)
		}
	}
	
  def fromDirectFile(filenames: List[String]) : FileListingProvider = {
    class DirectFileProvider extends  FileListingProvider {
      def foreach (f : (String) => Unit) : Unit = {
        filenames.foreach(filename => g(filename, f)) 
      }
    } 
    new DirectFileProvider() 
  } 
  def fromListingFile(filenames: List[String]) : FileListingProvider = {
    class ListingFileProvider extends  FileListingProvider {
      var reader: BufferedReader = null;
      var queue = new ListBuffer[String]()  
      filenames.foreach(f => appendFile(f))
      def appendFile(f: String) {
        try {
          reader = new BufferedReader(new FileReader(f))
          var line = reader.readLine();
          while(line != null) {  
            queue.append(line)
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
      def foreach (f : (String) => Unit) : Unit = {
        for(line <- queue) {
          g(line, f)
        }
      }
    }
    new ListingFileProvider() 
  }
}

trait FileListingProvider {
  def foreach (f : (String) => Unit) : Unit
}
