#!/bin/python
#
# Example for fs_c.py usage
#
import fs_c
import sys
import logging

def handle_file(file_data):
  print file_data.filename
  for chunk in file_data.chunks:
    print chunk
  print

if __name__ == "__main__":
  logging.basicConfig()

  for filename in sys.argv[1:]:
    parser = fs_c.create_parser(filename, handle_file)
    parser.parse_file()



