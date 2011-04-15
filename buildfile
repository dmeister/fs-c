
# Version number for this release
VERSION_NUMBER = "0.3.3"
# Group identifier for your projects
GROUP = "fs-c"
COPYRIGHT = "(c) Paderborn Center for Parallel Computing, 2009. Open Source under New BSD license"

require 'buildr/scala'
require 'buildr/java'

repositories.remote << "http://www.ibiblio.org/maven2/" << 
					   "http://repo1.maven.org/maven2" << 
					   "http://scala-tools.org/repo-releases"
HADOOP_ROOT = ENV["HADOOP_ROOT"]
if (!HADOOP_ROOT) then
	print "HADOOP_ROOT NOT SET\n"
	exit
end
PIG_ROOT = ENV["PIG_ROOT"]
if (!PIG_ROOT) then
	print "PIG_ROOT NOT SET\n"
	exit
end

desc "The Fs-c project"
define "fs-c" do

  project.version = VERSION_NUMBER
  project.group = GROUP

  manifest["Implementation-Vendor"] = COPYRIGHT
  
  define "protobuf" do
    task("generate") do
       	system("protoc --java_out src/main/gen/ src/main/other/fs-c.proto")
    end
    compile.options.target = '1.5'
    compile.from('src/main/gen').
    	into('target/gen-classes').
    	with("lib/*",
    		task("generate")
    	)
  end  
  
  compile.dependencies.include "#{HADOOP_ROOT}/hadoop-*.jar"
  compile.dependencies.include "lib/*.jar"
  compile.dependencies.include "#{PIG_ROOT}/pig-*.jar"
  compile.dependencies.include "target/gen-classes"
      
  define "java-parts" do
      compile.options.target = '1.5'
      compile.dependencies.include "#{HADOOP_ROOT}/hadoop-*.jar"
      compile.dependencies.include "lib/*.jar"
      compile.dependencies.include "#{PIG_ROOT}/pig-*.jar"
      compile.with("target/gen-classes")
      compile.from('src/main/java15').using(:javac).into('target/j-classes')
  end
  define "scala-parts" do
    compile.options.target = '1.5'
    compile.dependencies.include "#{HADOOP_ROOT}/hadoop-*.jar"
    compile.dependencies.include "lib/*.jar"
    compile.dependencies.include "target/gen-classes"
    compile.dependencies.include "#{PIG_ROOT}/pig-*.jar"
    compile.with("protobuf").with("java-parts")
  end

  package_with_sources
  package(:jar).with :manifest=>{ 'Copyright'=>'Paderborn Center for Parallel Computing (C) 2009-2010' }
  package(:jar).include "target/gen-classes/*"
  package(:jar).include "target/j-classes/*"
  
  package(:zip).include file("target/fs-c-#{VERSION_NUMBER}.jar")
  package(:zip).include file("src/main/other/fs-c"), :path => "bin"
  package(:zip).include file("src/main/other/log4j.xml"), :path => "conf"
  package(:zip).include "contrib/*", :path => "contrib"
  package(:zip).include file("README.txt")
  package(:zip).include file("CHANGES.txt")
  package(:zip).include "lib/*.jar", :path => "lib"
  
  package(:tgz).include file("target/fs-c-#{VERSION_NUMBER}.jar")
  package(:tgz).include file("src/main/other/fs-c"), :path => "bin"
  package(:tgz).include file("src/main/other/log4j.xml"), :path => "conf"
  package(:tgz).include "contrib/*", :path => "contrib"
  package(:tgz).include file("README.txt")
  package(:tgz).include file("CHANGES.txt")
  package(:tgz).include "lib/*", :path => "lib"
end
