import os
import sys
import re
import optparse

def replace_version_in_file(filename, options):
    file_data = open(filename).read()
    if re.search(options.old_version, open(filename).read()):
        print "Found version identifier in", filename
        
        new_contents = re.sub(options.old_version, options.version, file_data)
        f = open(filename + ".bak", "w").write(file_data)
        f = open(filename, "w").write(new_contents)
        
        os.unlink(filename + ".bak")
        
if __name__ == "__main__":
    parser = optparse.OptionParser()

    parser.add_option("-v","--new_version", dest="version", help="new version of FS-C")
    parser.add_option("-o","--old_version", dest="old_version", help="old version of FS-C")
    (options, args) = parser.parse_args()

    if not options.version or not options.old_version:
        print "Specify version with -v/-o"
        sys.exit(1)

    for dir in ["src", "contrib"]:
        for root, dirs, files in os.walk(dir):
            for filename in files:
                full_filename = os.path.join(root, filename)
                if (filename.endswith(".bak")):
                    continue
                
                replace_version_in_file(full_filename, options)
    replace_version_in_file("project/build.properties", options)
                    
                    
        