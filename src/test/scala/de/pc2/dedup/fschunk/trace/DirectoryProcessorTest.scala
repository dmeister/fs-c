package de.pc2.dedup.fschunk.trace

import org.scalatest._
import org.scalatest.matchers._
import org.scalatest.junit._ 
import java.io.File
import scala.collection.mutable.ListBuffer 

class DirectoryProcessorTest extends FunSuite with ShouldMatchers {
	class FileDispatcherMock extends FileDispatcher {
		val files = new ListBuffer[File]()
		
		def dispatch(f: File) {
			files.append(f)
		}
		
		def act() {
			
		}
	}
	def exec(cmd: String) : Boolean = {
		val p = Runtime.getRuntime().exec(cmd)
		p.waitFor()
		return p.exitValue == 0
	}
	
	def getTempDirectoryName() : String = {
		val tmpFile = File.createTempFile("fs-c-test","")
		tmpFile.delete()
		return tmpFile.getCanonicalPath()
	}
	
	def isUNIX() : Boolean = {
		val ls = System.getProperty("file.separator")
		return ls == "/"
	}
	
	implicit def str2file(s: String) : File = {
			return new File(s)
	}
	
	test("symlink handling") {
		if(isUNIX()) {
			val tmp = getTempDirectoryName()
			println(tmp)
			exec("mkdir %s".format(tmp))
			exec("mkdir %s/a".format(tmp))
			exec("ln -s a %s/b".format(tmp))
			exec("touch %s/a/c".format(tmp))
			
			val mock = new FileDispatcherMock()
			
			val d = new DirectoryProcessor("%s".format(tmp),true,mock)
			d.run()
			mock.files should have length (1)
			
			tmp.delete()
		}
		
		
	}
}
