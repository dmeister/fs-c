#!/bin/python
#
# Example for fs_c.py usage
#
import fs_c
import sys
import logging
import optparse

if __name__ == "__main__":
    logging.basicConfig()

    parser = optparse.OptionParser()
    parser.add_option("-f", "--format", dest="format",
                  help="format type of the file to parse")
    parser.add_option("-o", "--output", dest="output",
                  help="output file name")
    (options, args) = parser.parse_args()

    if len(args) == 0:
        parser.error("Need to provide at least one trace file to parse")
    if options.output is None:
        parser.error("Need to specify the output filename")

    writer = fs_c.NewFileWriter(options.output)

    for filename in args:
        parser = fs_c.create_parser(filename, writer.write, options.format)
        parser.parse_file()




