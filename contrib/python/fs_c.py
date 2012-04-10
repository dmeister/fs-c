import os
import sys
from protobuf_util import read_sized_message
import mmap
import logging
import logging.config
import optparse
import struct
try:
    import json
except ImportError:
    import simplejson as json
from google.protobuf.internal import decoder
import fs_c_pb2

logger = logging.getLogger("fsc parser")
    
def digest_to_hex(digest):
    return "%X" % (digest)

class Chunk:
    def __init__(self, chunk_size, digest):
        self.chunk_size = chunk_size
        self.digest = digest
        
    def __str__(self):
        return digest_to_hex(self.digest)
        
class File:
    def __init__(self, filename, file_size, file_type):
        self.filename = filename
        self.file_size = file_size
        self.file_type = file_type
        self.chunks = []
        
    def __str__(self):
        return "%s, %s, %s: chunks %s" % (self.filename,
            self.file_size, self.file_type,
            len(self.chunks))

class NewFileParser:
    def __init__(self, filename, callback):
        self.filename = filename
        self.callback = callback
        logger.debug("Parse legacy file %s", filename)        
        
        self.f = open(self.filename, "r")
        self.buffer_size = 1024 * 1024
        self.buffer = self.f.read(self.buffer_size)
        self.offset = 0
        if "Struct" in dir(struct):
            self.chunk_struct = struct.Struct("=Q12x")
        else:
            # Python 2.4 comp mode
            self.chunk_struct = None
    
    def remaining_buffer(self):
        return len(self.buffer) - self.offset
        
    def extend_buffer(self, n):
        buffer_size = n
        if buffer_size < self.buffer_size:
            buffer_size = self.buffer_size
        
        if self.remaining_buffer() == 0:
            self.buffer = self.f.read(buffer_size)
            self.offset = 0
        elif self.remaining_buffer() < n:
            self.buffer = "%s%s" % (self.buffer[self.offset:], self.f.read(buffer_size))
            self.offset = 0
        else:
            pass

    def clear_from_buffer(self, n):
        self.offset += n

    def read_message(self, m):
        self.extend_buffer(8)
        (data_size, data_size_len) = decoder._DecodeVarint32(buffer(self.buffer, self.offset), 0)
        self.clear_from_buffer(data_size_len)
        self.extend_buffer(data_size)
        m.ParseFromString(buffer(self.buffer, self.offset, data_size))
        self.clear_from_buffer(data_size)
       
    def parse_next_file_entry(self):
        file_data = None
        chunk_count = 0
        try:
            # test for end
            self.extend_buffer(8)
            if self.remaining_buffer() == 0:
                return None
            file_pb = fs_c_pb2.File()
            self.read_message(file_pb)
            file_data = File(file_pb.filename, file_pb.size, file_pb.type)
        
            for i in xrange(file_pb.chunkCount):
                chunk_pb = fs_c_pb2.Chunk()
                self.read_message(chunk_pb)
                if self.chunk_struct:
                    digest = self.chunk_struct.unpack(chunk_pb.fp)
                else:
                    digest = struct.unpack("=Q12x", chunk_pb.fp)
                chunk_data = Chunk(chunk_pb.size, digest)
                file_data.chunks.append(chunk_data)
            chunk_count = len(file_data.chunks)
        except KeyboardInterrupt:
            raise
        except:
            logger.exception("Read message error: %s, remaining buffer %s, file offset %s", self.filename, self.remaining_buffer(), self.f.tell())
            return None
                    
        if self.callback:
            self.callback(file_data)
        return len(file_data.chunks)
       
class LegacyFileParser:
    def __init__(self, filename, callback):
        self.filename = filename
        self.callback = callback
        logger.debug("Parse legacy file %s", filename)
        self.f = open(filename)
        
        if "Struct" in dir(struct):
            self.chunk_struct = struct.Struct("=iQ12x")
        else:
            self.chunk_struct = None
    
    def get_file_type(self, elements):
        if len(elements) >= 3:
            if elements[2]:
                return elements[2]
            else:
                return "---"
        else:
            return ""
            
    def parse_chunks(self, callback):
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
        
    def parse_next_file_entry(self):
        line = self.f.readline()
        elements = line.strip().split("\t")
        filename = elements[0]
        file_size = long(elements[1])
        file_type = self.get_file_type(elements)
        file_data = File(filename, file_size, file_type)
        
        chunks = 0
        while self.parse_chunks(lambda c: file_data.chunks.append(c)):
            chunks += 1
        self.f.read(1)
        self.callback(file_data)
        return chunks