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

release_tar = tarfile.open("fs-c-%s.tar" % options.version, mode="w")
release_tar.add("README.txt", "fs-c-%s/README.txt" % options.version)
release_tar.add("CHANGES.txt", "fs-c-%s/CHANGES.txt" % options.version)
release_tar.add("lib", "fs-c-%s/lib" % options.version)
release_tar.add("contrib", "fs-c-%s/contrib" % options.version)
release_tar.add("src/main/other/log4j.xml", "fs-c-%s/conf/log4j.xml" % options.version)
release_tar.add("src/main/other/log4j_debug.xml", "fs-c-%s/conf/log4j_debug.xml" % options.version)
release_tar.add("src/main/other/hazelcast.xml", "fs-c-%s/conf/hazelcast.xml" % options.version)
release_tar.add("src/main/other/hosts", "fs-c-%s/conf/hosts" % options.version)
release_tar.add("src/main/other/fs-c", "fs-c-%s/bin/fs-c" % options.version)
release_tar.add("project/boot/scala-2.9.0/lib/scala-library.jar", "fs-c-%s/lib/scala-library.jar" % options.version)
release_tar.add("lib_managed/scala_2.9.0/compile/argot_2.9.0-0.3.1.jar", "fs-c-%s/lib/argot-0.3.1.jar" % options.version)
release_tar.add("lib_managed/scala_2.9.0/compile/grizzled-scala_2.9.0-1.0.6.jar", "fs-c-%s/lib/grizzled-1.0.6.jar" % options.version)
release_tar.add("target/scala_2.9.0/fs-c_2.9.0-%s.jar" % options.version, "fs-c-%s/fs-c-%s.jar" % (options.version, options.version))
