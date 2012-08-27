package de.pc2.dedup.fschunk.trace

import java.io._
import scala.collection.mutable.ListBuffer

case class FilenameLabel(val filename: String, val source: Option[String], val label: Option[String])

/**
 * object for the file listing provided
 */
object FileListingProvider {
  // we always try to follow initial symlinks
  def g(filename: String, source: Option[String], label: Option[String], f: (FilenameLabel) => Unit): Unit = {
    try {
      val fl = FilenameLabel(new File(filename).getCanonicalPath(), source, label)
      f(fl)
    } catch {
      case _ =>
        val fl = FilenameLabel(filename, source, label)
        f(fl)
    }
  }

  /**
   * Create a listing provied from a direct file.
   */
  def fromDirectFile(filenames: Seq[String], label: Option[String]): FileListingProvider = {
    class DirectFileProvider extends FileListingProvider {
      def foreach(f: (FilenameLabel) => Unit): Unit = {
        filenames.foreach(filename => g(filename, Some(filename), label, f))
      }
    }
    new DirectFileProvider()
  }

  /**
   * Create a listing provider for a listing file
   */
  def fromListingFile(filenames: Seq[String], defaultLabel: Option[String]): FileListingProvider = {
    class ListingFileProvider extends FileListingProvider {
      var reader: BufferedReader = null;
      var queue = new ListBuffer[FilenameLabel]()
      filenames.foreach(f => appendFile(f))
      def appendFile(f: String) {
        try {
          reader = new BufferedReader(new FileReader(f))
          var line = reader.readLine();
          while (line != null) {
            val lio = line.lastIndexOf("=")
            val fl = if (lio >= 0 && lio < line.size) {
              // a label is provided
              val baseFilename = line.substring(0, line.lastIndexOf("="))
              val label = Some(line.substring(line.lastIndexOf("=") + 1))
              FilenameLabel(baseFilename, Some(baseFilename), label)
            } else {
              val baseFilename = line
              FilenameLabel(baseFilename, Some(baseFilename), defaultLabel)
            }
            queue.append(fl)
            line = reader.readLine();
          }
        } catch {
          case e: IOException =>
            e.printStackTrace();
        } finally {
          if (reader != null) {
            try {
              reader.close();
            } catch {
              case e: IOException =>
            }
          }
        }
      }
      def foreach(f: (FilenameLabel) => Unit): Unit = {
        for (fl <- queue) {
          g(fl.filename, fl.source, fl.label, f)
        }
      }
    }
    new ListingFileProvider()
  }
}

trait FileListingProvider {
  def foreach(f: (FilenameLabel) => Unit): Unit
}
