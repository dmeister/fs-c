from google.protobuf.internal import decoder
from google.protobuf.internal import encoder
import binascii

def read_sized_message(message, raw_data):
    (message_contents, end) = get_message_contents(raw_data)
    message.ParseFromString(message_contents)
    return end

def get_message_contents(raw_data):
    (data_size, data_size_len) = decoder._DecodeVarint32(raw_data, 0)
    return (raw_data[data_size_len:data_size_len+data_size], data_size_len + data_size)