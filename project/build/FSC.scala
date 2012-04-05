import sbt._
import java.io.File

class FSCProject(info: ProjectInfo) extends DefaultProject(info)
{  
  def getHadoopJars() : PathFinder = {
    val hadoopPath = System.getenv().get("HADOOP_ROOT")
    if (hadoopPath == null) {
      throw new Exception("HADOOP_ROOT enviromment is missing")
    }
    val hadoopJars = descendents(Path.fromFile(new File(hadoopPath)), "*.jar")
    return hadoopJars
  }
  def getPigJars() : PathFinder = {
    val path = System.getenv().get("PIG_ROOT")
    if (path == null) {
      throw new Exception("PIG_ROOT enviromment is missing")
    }
    val jars = descendents(Path.fromFile(new File(path)), "*.jar")
    return jars
  }
  override def javaCompileOptions = super.javaCompileOptions ++ javaCompileOptions("-source", "1.6") ++ javaCompileOptions("-target", "1.6")
  override def dependencyPath = "lib"
  override def unmanagedClasspath = super.unmanagedClasspath +++ getHadoopJars() +++ getPigJars()
  override def mainSourceRoots = super.mainSourceRoots +++ ("src" / "main" / "gen")
  override def mainClass = Some("de.pc2.dedup.fschunk.trace.Main")
    
  val argot = "org.clapper" %% "argot" % "0.3.1"
  val scalatest = "org.scalatest" % "scalatest_2.9.0" % "1.6.1"
}
