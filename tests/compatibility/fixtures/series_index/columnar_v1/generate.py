# Copyright 2023 Greptime Team
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""Independent wire fixture: python3 generate.py.

Do not regenerate to accommodate a reader change: this is the v1 contract.
"""
import json
import struct
from pathlib import Path


ROOT = Path(__file__).parent

def u32(n):
    return struct.pack("<I", n)

def u64(n):
    return struct.pack("<Q", n)

def string(s):
    b = s.encode()
    return u64(len(b)) + b

def varint(n):
    result = bytearray()
    while n >= 128:
        result.append((n & 127) | 128)
        n >>= 7
    result.append(n)
    return result

def integers(values):
    result = bytearray()
    previous = 0
    for value in values:
        delta = (value - previous) & ((1 << 64) - 1)
        signed = delta if delta < (1 << 63) else delta - (1 << 64)
        result += varint(((signed << 1) ^ (signed >> 63)) & ((1 << 64) - 1))
        previous = value
    return result

def dictionary(values):
    entries = [None]
    for value in values:
        if value not in entries:
            entries.append(value)
    data = u64(len(entries)) + b"\0"
    for value in entries[1:]:
        data += b"\1" + string(value)
    return varint(len(data)) + data + b"".join(varint(entries.index(v)) for v in values)

def checksum(data):
    """XXH64, seed zero, written independently of the production Rust codec."""
    mask = (1 << 64) - 1
    p1, p2, p3, p4, p5 = (11400714785074694791, 14029467366897019727,
                           1609587929392839161, 9650029242287828579, 2870177450012600261)

    def rotate(value, bits):
        return ((value << bits) | (value >> (64 - bits))) & mask

    def round64(acc, lane):
        return (rotate((acc + lane * p2) & mask, 31) * p1) & mask

    pos = 0
    if len(data) >= 32:
        lanes = [(p1 + p2) & mask, p2, 0, (-p1) & mask]
        while pos + 32 <= len(data):
            for index in range(4):
                lanes[index] = round64(lanes[index], int.from_bytes(data[pos:pos+8], "little"))
                pos += 8
        result = sum(rotate(value, bits) for value, bits in zip(lanes, [1, 7, 12, 18])) & mask
        for value in lanes:
            result = ((result ^ round64(0, value)) * p1 + p4) & mask
    else:
        result = p5
    result = (result + len(data)) & mask
    while pos + 8 <= len(data):
        result ^= round64(0, int.from_bytes(data[pos:pos+8], "little"))
        result = (rotate(result, 27) * p1 + p4) & mask
        pos += 8
    if pos + 4 <= len(data):
        result ^= int.from_bytes(data[pos:pos+4], "little") * p1
        result = (rotate(result & mask, 23) * p2 + p3) & mask
        pos += 4
    for byte in data[pos:]:
        result ^= byte * p5
        result = (rotate(result & mask, 11) * p1) & mask
    result ^= result >> 33
    result = (result * p2) & mask
    result ^= result >> 29
    result = (result * p3) & mask
    return result ^ (result >> 32)

assert checksum(b"") == 0xef46db3751d8e999
assert checksum(b"hello") == 0x26c7827d889f6da3

output = bytearray(b"GTSERIES" + u32(1) + u32(0))

def payload(data, encoding):
    offset = len(output)
    output.extend(data)
    return struct.pack("<QIIQHHI", offset, len(data), len(data), checksum(data), encoding, 0, 0)

blocks = []
for table, ids, counts, tags in [(7, [10, 11], [2, 3], [None, ""]), (9, [5], [1], ["é"])]:
    columns = []
    for name, data, encoding, nulls, bounds in [
        ("__tsid", integers(ids), 1, 0, None),
        ("__series_row_count", integers(counts), 1, 0, None),
        ("tag_0", dictionary(tags), 2, tags.count(None), (min(v for v in tags if v is not None), max(v for v in tags if v is not None))),
    ]:
        column = string(name) + payload(data, encoding) + u32(nulls) + u32(int(bounds is not None))
        if bounds is not None:
            column += string(bounds[0]) + string(bounds[1])
        columns.append(column)
    block = u32(table) + u64(ids[0]) + u64(ids[-1]) + u32(len(ids)) + u32(len(columns)) + b"".join(columns)
    bounds = u32(table) + u64(ids[0]) + u32(table) + u64(ids[-1])
    blocks.append((bounds, block))

payload_end = len(output)
children = []
for bounds, block in blocks:
    children.append(bounds + payload(u32(0) + u64(1) + block, 1))
root_bounds = u32(7) + u64(10) + u32(9) + u64(5)
root = root_bounds + payload(u32(1) + u64(2) + b"".join(children), 1)
trailer = root + u32(2) + u32(1) + u64(len(output) + 96) + u64(payload_end) + b"GTSERIES"
assert len(trailer) == 88
output += trailer + u64(checksum(trailer))
(ROOT / "part-0.series").write_bytes(output)
coverage = json.loads((ROOT.parent / "legacy_catalog.json").read_text())["indexes"][0]
coverage["file_size"] = 0
manifest = dict(format="columnar-series", version=1, region_id=4294967297,
                generation=coverage["index_uuid"], tags=[dict(column_id=0, name="tag_0"), dict(column_id=1, name="tag_1")],
                coverage=coverage, files=[dict(part_id=0, file_size=len(output), num_series=3,
                first=dict(table_id=7, tsid=10), last=dict(table_id=9, tsid=5))])
(ROOT / "manifest.json").write_text(json.dumps(manifest, indent=2) + "\n")
