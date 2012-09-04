import os
import sys
import tarfile
import optparse
import re

def get_configured_fsc_version():
    pattern = re.compile("project.version=(.+)") 
    properties_file = open("project/build.properties")
    for l in properties_file:
        m = pattern.match(l)
        if m:
            return m.groups()[0]
    raise Exception("Failed to find project version")

if __name__ == "__main__":
    parser = optparse.OptionParser()
    (options, args) = parser.parse_args()
        
    version = get_configured_fsc_version()
    
    filename = "fs-c-%s.tar.gz" % version
    release_tar = tarfile.open(filename, mode="w:gz")
    release_tar.add("README.txt", "fs-c-%s/README.txt" % version)
    release_tar.add("CHANGES.txt", "fs-c-%s/CHANGES.txt" % version)
    release_tar.add("lib", "fs-c-%s/lib" % version)
    release_tar.add("contrib", "fs-c-%s/contrib" % version)
    release_tar.add("src/main/other/log4j.xml", "fs-c-%s/conf/log4j.xml" % version)
    release_tar.add("src/main/other/log4j_debug.xml", "fs-c-%s/conf/log4j_debug.xml" % version)
    release_tar.add("src/main/other/hazelcast.xml", "fs-c-%s/conf/hazelcast.xml" % version)
    release_tar.add("src/main/other/hosts", "fs-c-%s/conf/hosts" % version)
    release_tar.add("src/main/other/fs-c", "fs-c-%s/bin/fs-c" % version)
    release_tar.add("project/boot/scala-2.9.0/lib/scala-library.jar", "fs-c-%s/lib/scala-library.jar" % version)
    release_tar.add("lib_managed/scala_2.9.0/compile/argot_2.9.0-0.3.1.jar", "fs-c-%s/lib/argot-0.3.1.jar" % version)
    release_tar.add("lib_managed/scala_2.9.0/compile/grizzled-scala_2.9.0-1.0.6.jar", "fs-c-%s/lib/grizzled-1.0.6.jar" % version)
    release_tar.add("lib_managed/scala_2.9.0/compile/guava-13.0.1.jar", "fs-c-%s/lib/guava-13.0.1.jar" % version)
    release_tar.add("target/scala_2.9.0/fs-c_2.9.0-%s.jar" % version, "fs-c-%s/fs-c-%s.jar" % (version, version))
    release_tar.close()

    print "Packaged", filename