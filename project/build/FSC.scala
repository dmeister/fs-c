import sbt._
import java.io.File

class FSCProject(info: ProjectInfo) extends DefaultProject(info)
{  
  def getHadoopJars() : PathFinder = {
    val hadoopPath = System.getenv().get("HADOOP_PATH")
    if (hadoopPath == null) {
      throw new Exception("HADOOP_PATH enviromment is missing")
    }
    val hadoopJars = descendents(Path.fromFile(new File(hadoopPath)), "*.jar")
    return hadoopJars
  }
  def getPigJars() : PathFinder = {
    val path = System.getenv().get("PIG_PATH")
    if (path == null) {
      throw new Exception("PIG_PATH enviromment is missing")
    }
    val jars = descendents(Path.fromFile(new File(path)), "*.jar")
    return jars
  }
  override def dependencyPath = "lib"
  override def unmanagedClasspath = super.unmanagedClasspath +++ getHadoopJars() +++ getPigJars()
  override def mainSourceRoots = super.mainSourceRoots +++ ("src" / "main" / "gen")
  override def mainClass = Some("de.pc2.dedup.fschunk.trace.Main")
    
  val argot = "org.clapper" %% "argot" % "0.3.1"
}