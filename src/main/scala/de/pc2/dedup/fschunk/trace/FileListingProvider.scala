package de.pc2.dedup.fschunk.trace

import java.io._
import scala.collection.mutable.ListBuffer
 
object FileListingProvider {
  def fromDirectFile(filenames: List[String]) : FileListingProvider = {
    class DirectFileProvider extends  FileListingProvider {
      def foreach (f : (String) => Unit) : Unit = {
        filenames.foreach(filename => f(filename)) 
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
          f(line)
        }
      }
    }
    new ListingFileProvider() 
  }
}

trait FileListingProvider {
  def foreach (f : (String) => Unit) : Unit
}
