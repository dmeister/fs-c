import os
import sys
import re
import optparse
from release import get_configured_fsc_version

def replace_version_in_file(filename, old_version, new_version):
    file_data = open(filename).read()
    if re.search(old_version, open(filename).read()):
        print "Found version identifier in", filename
        
        new_contents = re.sub(old_version, new_version, file_data)
        f = open(filename + ".bak", "w").write(file_data)
        f = open(filename, "w").write(new_contents)
        
        os.unlink(filename + ".bak")
        
if __name__ == "__main__":
    parser = optparse.OptionParser()

    parser.add_option("-v","--new_version", dest="version", help="new version of FS-C")
    (options, args) = parser.parse_args()
    
    old_version = get_configured_fsc_version()

    if not options.version:
        print "Specify version with -v"
        sys.exit(1)

    for dir in ["src", "contrib"]:
        for root, dirs, files in os.walk(dir):
            for filename in files:
                full_filename = os.path.join(root, filename)
                if filename.endswith(".bak"):
                    continue
                
                replace_version_in_file(full_filename, old_version, options.version)
    replace_version_in_file("project/build.properties", old_version, options.version)
                    
                    
        