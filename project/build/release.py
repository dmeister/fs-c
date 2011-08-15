import os
import sys
import tarfile
import optparse

parser = optparse.OptionParser()
parser.add_option("-v","--new_version", dest="version", help="version of FS-C")
(options, args) = parser.parse_args()

if not options.version:
    print "Specify version with -v"
    sys.exit(1)

release_tar = tarfile.TarFile("fs-c-%s.tgz" % options.version, mode="w")
release_tar.add("README.txt", "fs-c/README.txt")
release_tar.add("CHANGES.txt", "fs-c/CHANGES.txt")
release_tar.add("lib", "fs-c/lib")
release_tar.add("contrib", "fs-c/contrib")
release_tar.add("src/main/other/log4j.xml", "fs-c/conf/log4j.xml")
release_tar.add("src/main/other/log4j_debug.xml", "fs-c/conf/log4j_debug.xml")
release_tar.add("src/main/other/hazelcast.xml", "fs-c/conf/hazelcast.xml")
release_tar.add("src/main/other/hosts", "fs-c/conf/hosts")
release_tar.add("src/main/other/fs-c", "fs-c/bin/fs-c")
release_tar.add("project/boot/scala-2.9.0/lib/scala-library.jar", "fs-c/lib/scala-library.jar")
release_tar.add("lib_managed/scala_2.9.0/compile/argot_2.9.0-0.3.1.jar", "fs-c/lib/argot-0.3.1.jar")
release_tar.add("lib_managed/scala_2.9.0/compile/grizzled-scala_2.9.0-1.0.6.jar", "fs-c/lib/grizzled-1.0.6.jar")
release_tar.add("target/scala_2.9.0/fs-c_2.9.0-%s.jar" % options.version, "fs-c/fs-c-%s.jar" % options.version)