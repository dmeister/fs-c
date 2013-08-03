#!/bin/python
#
# Example for fs_c.py usage
#
import fs_c
import sys
import logging
import optparse

def handle_file(file_data):
  print file_data.filename
  for chunk in file_data.chunks:
    print chunk
  print

if __name__ == "__main__":
  logging.basicConfig()

  parser = optparse.OptionParser()
  parser.add_option("-f", "--format", dest="format",
                  help="format type of the file to parse")
  (options, args) = parser.parse_args()

  for filename in args:
    parser = fs_c.create_parser(filename, handle_file, options.format)
    parser.parse_file()



