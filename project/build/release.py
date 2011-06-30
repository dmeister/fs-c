import os
import sys
import tarfile

version = "0.3.5"

release_tar = tarfile.TarFile("fs-c-%s.tgz" % version, mode="w")
release_tar.add("README.txt")
release_tar.add("CHANGES.txt")
release_tar.add("lib")
release_tar.add("contrib")
release_tar.add("src/main/other/log4j.xml", "conf/log4j.xml")
release_tar.add("src/main/other/log4j_debug.xml", "/conf/log4j_debug.xml")
release_tar.add("src/main/other/fs-c", "bin/fs-c")
release_tar.add("project/boot/scala-2.9.0/lib/scala-library.jar", "lib/scala-library.jar")
release_tar.add("lib_managed/scala_2.9.0/compile/argot_2.9.0-0.3.1.jar", "lib/argot-0.3.1.jar")
release_tar.add("target/scala_2.9.0/fs-c_2.9.0-%s.jar" % version, "fs-c-%s.jar" % version)