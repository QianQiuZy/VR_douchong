# -*- coding: utf-8 -*-
"""
简易protobuf字段解析工具，兼容B站INTERACT_WORD_V2等pb字段。
"""
import base64

def read_varint(buf, pos):
    result = 0
    shift = 0
    while True:
        b = buf[pos]
        pos += 1
        result |= (b & 0x7F) << shift
        if not (b & 0x80):
            break
        shift += 7
    return result, pos

def parse_pb(pb_base64):
    buf = base64.b64decode(pb_base64)
    pos = 0
    fields = {}
    while pos < len(buf):
        key, pos = read_varint(buf, pos)
        field_num = key >> 3
        wire_type = key & 0x7
        value = None
        if wire_type == 0:  # varint
            value, pos = read_varint(buf, pos)
        elif wire_type == 2:  # length-delimited (string, bytes, embedded message)
            length, pos = read_varint(buf, pos)
            value_bytes = buf[pos:pos+length]
            try:
                value_str = value_bytes.decode('utf-8')
                value = value_str
            except Exception:
                value = value_bytes
            pos += length
        else:
            # 其他类型暂不处理
            break
        fields[field_num] = value
    return fields
