import os
import sys
from protobuf_util import read_sized_message
import mmap
import logging
import logging.config
import optparse
import json
from google.protobuf.internal import decoder
from google.protobuf.internal import encoder
import fs_c_pb2
import re

logger = logging.getLogger("fsc parser")

class Chunk(object):
    """ Class that represents a chunk
    """
    def __init__(self, chunk_size, digest):
        """ Constructor """
        self.chunk_size = chunk_size
        self.digest = digest

    def __str__(self):
        """ Formatted output """
        return "%s %s" % (self.digest.encode("hex"), self.chunk_size)

class File(object):
    """ Class that represents a file """
    def __init__(self, filename, file_size, file_type):
        """ Constructor """
        self.filename = filename
        self.file_size = file_size
        self.file_type = file_type
        self.chunks = []

    def __str__(self):
        """ Formatted output """
        return "%s, %s, %s: chunks %s" % (self.filename,
            self.file_size, self.file_type,
            len(self.chunks))

def create_parser(filename, callback, parser_type = None):
  """
    Creates a new parser object.

    legacy should only be used if the user is sure. There
    is only a single legacy data set.
  """
  if (parser_type == "legacy"):
    return LegacyFileParser(filename, callback)
  elif parser_type == "ubc":
    return UBCFileParser(filename, callback)
  elif (parser_type == "protobuf" or parser_type is None):
    return NewFileParser(filename, callback)

class NewFileWriter(object):
    """ Writer class for protobuf-based trace files
    """

    def __init__(self, filename):
      self.filename = filename
      self.f = open(self.filename, "w")
      self.encoder = encoder._VarintEncoder()

    def write(self, fo):
        file_pb = fs_c_pb2.File()
        file_pb.filename = fo.filename
        file_pb.size = fo.file_size
        file_pb.type = fo.file_type
        file_pb.chunkCount = len(fo.chunks)
        self._write_message(file_pb)

        for co in fo.chunks:
          chunk_pb = fs_c_pb2.Chunk()
          chunk_pb.fp = co.digest
          chunk_pb.size = co.chunk_size
          self._write_message(chunk_pb)

    def _write_message(self, m):
        self.encoder(self.f.write, m.ByteSize())
        s = m.SerializeToString()
        self.f.write(s)

class NewFileParser(object):
    """ Parser class for protobuf based trace files
    """

    def __init__(self, filename, callback):
        self.filename = filename
        self.callback = callback
        logger.debug("Parse legacy file %s", filename)

        self.f = open(self.filename, "r")
        self.buffer_size = 1024 * 1024
        self.buffer = self.f.read(self.buffer_size)
        self.offset = 0

    def _remaining_buffer(self):
        return len(self.buffer) - self.offset

    def _extend_buffer(self, n):
        buffer_size = n
        if buffer_size < self.buffer_size:
            buffer_size = self.buffer_size

        if self._remaining_buffer() == 0:
            self.buffer = self.f.read(buffer_size)
            self.offset = 0
        elif self._remaining_buffer() < n:
            self.buffer = "%s%s" % (self.buffer[self.offset:], 
                self.f.read(buffer_size))
            self.offset = 0
        else:
            pass

    def _clear_from_buffer(self, n):
        self.offset += n

    def _read_message(self, m):
        self._extend_buffer(8)
        (data_size, data_size_len) = decoder._DecodeVarint32(buffer(self.buffer, 
          self.offset), 0)
        self._clear_from_buffer(data_size_len)
        self._extend_buffer(data_size)
        m.ParseFromString(buffer(self.buffer, self.offset, data_size))
        self._clear_from_buffer(data_size)

    def parse_file(self):
      """ Runs through the complete file.
          Calls the callback for each finished file
      """
      while self._parse_next_file_entry() is not None:
        pass

    def _parse_next_file_entry(self):
        """ Parses a file
            TODO (dmeister) Check if FilePart entries are handled correctly
        """
        file_data = None
        try:
            # test for end
            self._extend_buffer(8)
            if self._remaining_buffer() == 0:
                return None
            file_pb = fs_c_pb2.File()
            self._read_message(file_pb)
            file_data = File(file_pb.filename, file_pb.size, file_pb.type)
            for i in xrange(file_pb.chunkCount):
                chunk_pb = fs_c_pb2.Chunk()
                self._read_message(chunk_pb)
                chunk_data = Chunk(chunk_pb.size, chunk_pb.fp)
                file_data.chunks.append(chunk_data)
        except KeyboardInterrupt:
            raise
        except:
            logger.exception("Read message error: %s, remaining buffer %s, file offset %s", 
                self.filename, self._remaining_buffer(), self.f.tell())
            return None

        if self.callback:
            self.callback(file_data)
        return len(file_data.chunks)

class UBCFileParser(object):
    """
      Parser class for the file format for UBC (University of British Columbia)
      trace files.
    """
    def __init__(self, filename, callback):
        """ 
          Constructor
        """
        self.filename = filename
        self.callback = callback
        logger.debug("Parse ubc file %s", filename)

        self.f = open(self.filename, "r")
        self.chunk_pattern = re.compile(r"^([0-9a-fz]+):(\d+)$")

    def parse_file(self):
      """
        Parses the trace file
      """
      self._parse_header()
      while self._parse_next_file_entry() is not None:
         pass

    def _parse_header(self):
      """
        Reads the file header.
        There is no interesting information for us there, so we
        just skip until the first empty line
      """
      line = self._readline()
      while line != "":
        line = self._readline()

    def _skip_lines(self, n):
        """
          Skips n lines from the trace file
        """
        for i in xrange(n):
            self._readline()

    def _skip_file_frag_info(self):
        """
          Skips as long as the file fragmentation information are stored in 
          the line.
          Returns the first chunk data line
        """
        while True:
            line = self._readline()
            if not (line.startswith("SV:") or line.startswith("V:") or line.startswith("A:")):
                return line

    def _process_chunk_line(self, line):
        """
          Parses a line with chunk data
        """
        line = line.strip()
        m = self.chunk_pattern.match(line)
        if not m:
            raise Exception("Illegal chunk line: %s" % line)
        s = int(m.groups()[1])
        digest = m.groups()[0]
        if digest.startswith("z"):
            pass
        else:
            digest = digest.decode("hex")
        chunk_data = Chunk(s, digest)
        return chunk_data

    def _readline(self):
        """
          Reads a line from the trace file
        """
        return self.f.readline().strip()

    def _parse_next_file_entry(self):
        # filename = directory + filename hash + extension hash
        first_line = self._readline()
        if first_line == "LOGCOMPLETE":
          return None
        dir_hash = first_line.partition(":")[0]
        filename_hash =self._readline().partition(":")[0]
        ext_hash = self._readline().partition(":")[0]
        self._skip_lines(1)
        file_size = int(self._readline())

        file_data = File(dir_hash + filename_hash + ext_hash, 
            file_size, ext_hash)

        self._skip_lines(7)
        line = self._skip_file_frag_info()
        file_size = 0
        while line != "":
            chunk_data = self._process_chunk_line(line)
            file_size += chunk_data.chunk_size
            file_data.chunks.append(chunk_data)
            line = self._readline()

        file_data.file_size = file_size

        if self.callback:
            self.callback(file_data)
        return len(file_data.chunks)

class LegacyFileParser(object):
    def __init__(self, filename, callback):
        self.filename = filename
        self.callback = callback
        logger.debug("Parse legacy file %s", filename)
        self.f = open(filename)

        if "Struct" in dir(struct):
            self.chunk_struct = struct.Struct("=iQ12x")
        else:
            self.chunk_struct = None

    def _get_file_type(self, elements):
        if len(elements) >= 3:
            if elements[2]:
                return elements[2]
            else:
                return "---"
        else:
            return ""

    def _parse_chunks(self, callback):
        record_size = ord(self.f.read(1))
        if record_size == 0 or record_size == -1:
            return False
        if record_size != 24:
            raise Exception("Illegal record size")
        buf = self.f.read(24)
        if self.chunk_struct:
            (chunk_size, digest) = self.chunk_struct.unpack(buf)
        else:
            (chunk_size, digest) = struct.unpack("=iQ12x", buf)

        chunk = Chunk(chunk_size, digest)
        callback(chunk)
        return True

    def parse_file(self):
      """ Reads the trace file and calls the callback for each file
      """
      while True:
        # TODO (dmeister) Stopping condition not clear, should be fixed
        self._parse_next_file_entry()

    def _parse_next_file_entry(self):
        line = self.f.readline()
        elements = line.strip().split("\t")
        filename = elements[0]
        file_size = long(elements[1])
        file_type = self._get_file_type(elements)
        file_data = File(filename, file_size, file_type)

        chunks = 0
        while self.parse_chunks(lambda c: file_data.chunks.append(c)):
            chunks += 1
        self.f.read(1)
        self.callback(file_data)
        return chunks
