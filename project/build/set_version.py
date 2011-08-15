import os
import sys
import re
import optparse

if __name__ == "__main__":
    parser = optparse.OptionParser()

    parser.add_option("-v","--new_version", dest="version", help="new version of FS-C")
    parser.add_option("-o","--old_version", dest="old_version", help="old version of FS-C")
    (options, args) = parser.parse_args()

    if not options.version or not options.old_version:
        print "Specify version with -v"
        sys.exit(1)

    for dir in ["src", "contrib"]:
        for root, dirs, files in os.walk(dir):
            for filename in files:
                full_filename = os.path.join(root, filename)
                if (filename.endswith(".bak")):
                    continue
                
                file_data = open(full_filename).read()
                if re.search(options.old_version, open(full_filename).read()):
                    print "Found version identifier in ", full_filename
                    
                    new_contents = re.sub(options.old_version, options.version, file_data)
                    f = open(full_filename + ".bak", "w").write(file_data)
                    f = open(full_filename, "w").write(new_contents)
                    
                    os.unlink(full_filename + ".bak")
                    
                    
                    
        