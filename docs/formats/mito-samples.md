# Mito columnar samples format

This document specifies the version 1 on-disk layout, encodings, ordering rules,
and validation limits for samples artifacts. It shares artifact conventions with
[Mito columnar series format](mito-series.md).

Samples store nullable Float64 fields, timestamps, sequences, and operation types
in single-series blocks bounded by rows and decoded bytes.

## Artifact manifest

The catalog identifier is `columnar-samples`. Its file and manifest version is 1,
independent of the series format's version and both formats' codec identifiers.
Existing `parquet` and `columnar-series` identifiers keep their meaning.

One artifact is derived from one immutable source SST. It owns these objects:

```text
<region_id>/samples/<source_file_id>/<generation>/manifest.json
<region_id>/samples/<source_file_id>/<generation>/part-0.samples
<region_id>/samples/<source_file_id>/<generation>/part-1.samples
...
```

The source file ID and generation use existing `FileId` serialization. The
generation is unique for each build. A manifest contains:

| Field | Meaning |
| --- | --- |
| `format` | String `columnar-samples` |
| `version` | Integer 1 |
| `region_id` | Physical region ID, u64 |
| `source_file_id` | SST `FileId` matching `SamplesEntry.file_id` |
| `generation` | `FileId` matching the catalog artifact generation |
| `timestamp` | Object with `column_id`, `name`, and `unit` |
| `fields` | Ordered objects with `column_id`, `name`, `type`, and `nullable` |
| `files` | Ordered part descriptors, defined below |

The timestamp's ID is u32, its name is the region's time-index name, and its unit
is one of `second`, `millisecond`, `microsecond`, or `nanosecond`. Values are
signed i64 counts since the Unix epoch in that unit. There is no dataset-relative
origin, unit conversion, or timezone in the file. The timestamp is non-null.

The field list records the build-time region field schema in metadata order.
Require 1..=1,024 fields, each with `type = "float64"` and `nullable = true`.
A non-nullable source Float64 field is represented by this same nullable logical
type with no null values. Sparse logical-table fields may be entirely null.
Timestamp and field IDs must be distinct. All output names must be unique,
including the internal names below. Tags are not sample fields and are not stored here.

The stored logical Arrow schema is `__table_id: UInt32`, `__tsid: UInt64`, the
timestamp column with its recorded unit, `__sequence: UInt64`, `__op_type: UInt8`,
then nullable Float64 fields in manifest order. The first five columns are
non-null. No `__primary_key` or tag columns are stored.

Each `files` entry contains `part_id: u32`, `file_size: u64`, `num_rows: u64`,
`num_blocks: u64`, `first`, `last`, `min_timestamp: i64`, and `max_timestamp: i64`.
`first` and `last` are inclusive `{ table_id: u32, tsid: u64 }` bounds. Time bounds
are the minimum and maximum over **all** blocks in the part, not just its first
and last rows. Every part is nonempty. Part IDs start at zero, increase by one,
and derive `part-<part_id>.samples` names with decimal IDs and no leading zeros.
No object path is stored in a part descriptor.

Parts follow input order. Adjacent identifier ranges satisfy
`previous.last <= next.first`; equality is allowed because a series can continue
in another part. The shared boundary series can also have equal timestamps on
both sides. These ranges are **not disjoint**, unlike the series-index format.
Readers consider every matching part rather than choosing a unique owner.

`ArtifactMetadata.objects` records the manifest and every part as `{ path, file_size }`;
the artifact's `file_size` is their checked sum. An empty artifact has an empty
file list and no part files. The manifest is at most 64 MiB.

Region, source SST, and generation must match the catalog entry. The artifact
object list must contain exactly the derived manifest and part paths, without
duplicates, extra objects, or inconsistent size totals. Part order comes from
the manifest.

## Part file

```text
┌──────────────────────────────────────────────┐
│ Header                              16 bytes │
├──────────────────────────────────────────────┤
│ Independently encoded/compressed chunks      │
│   Block 0: time, sequence, operation, fields  │
│   Block 1: ...                               │
├──────────────────────────────────────────────┤ ← payload_end
│ Leaf pages (one is the root at depth 1)       │
│ Optional internal root (depth 2)              │
├──────────────────────────────────────────────┤
│ Trailer                            112 bytes │
└──────────────────────────────────────────────┘
```

All fixed-width numbers are little-endian, without alignment padding. Byte ranges
are half-open; offsets are absolute from the start of the part. Parse each field
explicitly. Rust struct layout, enum order, and `usize` are not wire contracts.
Reserved fields must be zero, and decoded structures must have no trailing bytes.

### Header

| Field | Bytes | Value |
| --- | ---: | --- |
| Magic | 8 | ASCII `GTSAMPLE` |
| File version | 4 | 1 |
| Reserved | 4 | Zero |

### Column payloads and row semantics

Each block contains exactly one `(table_id, tsid)`. Identity is stored once in
its descriptor. Timestamp, sequence, operation type, and each present value field
are independent chunks; their references are in the metadata, not inline headers.
Emit chunks in that order, with value fields following manifest order.

Rows preserve delete markers, duplicate timestamps, and equal-key ties, including
conflicting values at the same sequence. The format does not encode a deduplicated
or snapshot-filtered view.

Identifiers are nondecreasing, timestamps are nondecreasing within an identifier,
and sequences are nonincreasing within equal timestamps, including across parts.
Equal ordering keys retain input order; operation type is not a sorting key.

`(table_id, tsid)` is the complete identity under the uniqueness assumption.
Equal TSIDs in different tables are distinct. Exact sparse primary keys, tags,
and collision discriminators are not stored.

Blocks split on an identity change or before the next row would exceed a packing
limit. There is no hourly split and no padding of short blocks. The default row
limit is 8,192, also the version-1 reader maximum; a writer may choose a smaller
positive limit without changing the version. With `F` manifest fields and `N`
rows, the decoded accounting size is:

```text
N * (8 timestamp + 8 sequence + 1 operation + 8 * F values)
    + F * ceil(N / 8) validity bytes
```

Require this to be at most 262,144 bytes. Count all manifest fields, including
omitted/all-null fields, and reserve validity bytes even for all-valid fields.
This is a bound on decoded column buffers, not total allocator usage or RSS.
The effective row cap is the largest integer satisfying both limits: 8,192 for
one field and 31 for 1,024 fields. Encoded buffers and metadata have separate
bounds below. Increasing the reader maximum requires an explicit compatibility
decision; it is not merely a writer option in this version.

### Payload reference

Every stored column and metadata page uses the same 32-byte reference layout as
the series format:

| Field | Bytes | Meaning |
| --- | ---: | --- |
| Offset | 8 | Absolute location of stored bytes |
| Stored length | 4 | Bytes to fetch |
| Decoded length | 4 | Bytes after decompression, before column/page decoding |
| Checksum | 8 | XXH64 of stored bytes, seed zero |
| Encoding ID | 2 | Sample column or metadata encoding, according to context |
| Compression ID | 2 | Compression of the encoded bytes |
| Reserved | 4 | Zero |

Lengths are nonzero. Check `offset + stored_length` for u64 overflow before
checking the enclosing area. u32 payload lengths do not limit the part or its
offsets to 4 GiB. Verify the checksum before decompression; require exactly the
declared decoded length, with bounded output even if a codec frame claims more.
Checksums detect accidental corruption, not malicious modification.

### Metadata pages

A leaf describes blocks. An internal page references leaves. The root is either
the only leaf (depth 1) or an internal page (depth 2). Other depths are unsupported
in version 1; roll parts before a third level would be needed.

As in the series format, the entire page, including its kind and count, is encoded
and independently compressed. The reference in the parent or trailer supplies
its storage location, lengths, checksum, and codecs. There is no separate stored
page header. Decoded pages begin with `kind: u32` and `entry_count: u64`:

```text
Leaf (kind 0):                     Internal root (kind 1):
  kind                 u32          kind                 u32
  entry_count          u64          entry_count          u64
  block descriptor 0                page reference 0     72 bytes
  ...                               ...
  block descriptor N-1              page reference N-1   72 bytes
```

Pages are nonempty and limited to 65,536 decoded bytes, including the 12-byte
prefix. Descriptors never straddle pages. Unlike series label descriptors,
sample descriptors have no variable-size strings, and even the widest supported
descriptor fits in one page. No oversized-leaf exception is needed.

#### Block descriptor

| Field | Bytes | Meaning |
| --- | ---: | --- |
| Table ID | 4 | Constant for this block |
| TSID | 8 | Constant for this block |
| Minimum timestamp | 8 | Signed i64, first row's timestamp |
| Maximum timestamp | 8 | Signed i64, last row's timestamp |
| Block ordinal | 8 | Per-identifier ordinal across the artifact |
| Row count | 4 | Nonzero, subject to decoded-block limits |
| Column count | 4 | Number of following column descriptors |
| Column descriptors | 44 each | Defined below |

The fixed prefix is 44 bytes. Ordinals start at zero for each identity and
increase by one across block and part boundaries. Blocks sort lexicographically
by `(table_id, tsid, min_timestamp, ordinal)`. Adjacent blocks of the same series
satisfy `previous.max_timestamp <= next.min_timestamp`; equality is permitted.

Each column descriptor is:

| Field | Bytes | Meaning |
| --- | ---: | --- |
| Role | 4 | 1 timestamp, 2 sequence, 3 operation type, 4 value field |
| Column ID | 4 | Timestamp/field physical ID; zero for sequence/operation |
| Payload reference | 32 | Independently stored chunk |
| Null count | 4 | Number of null rows |

Role and column ID together identify a chunk. The role namespace avoids reserving
physical field IDs for internal columns. Time, sequence, and operation descriptors
are mandatory, appear first in that order, and have zero null counts. Value
descriptors follow manifest order with no repeated or unknown IDs. A value with
all rows null is omitted and synthesized as null by the reader; present value
chunks have `null_count < row_count`. No value statistics are stored.

A block with `P` present fields has `C = 3 + P` chunks and a descriptor of
`44 + 44 * C` bytes. This is 220 bytes for one present field and 45,232 bytes for
1,024 present fields. The leaf prefix makes the latter 45,244 bytes. All chunks
are physically ordered and disjoint in the column area, across blocks as well
as within a block. There is no inline block header or block-wide compression.

#### Page reference and time bounds

Samples extend the series format's reference with two timestamp bounds:

| Field | Bytes | Meaning |
| --- | ---: | --- |
| First table ID; first TSID | 4 + 8 | Inclusive first identifier |
| Last table ID; last TSID | 4 + 8 | Inclusive last identifier |
| Minimum timestamp | 8 | Minimum over all descendant blocks |
| Maximum timestamp | 8 | Maximum over all descendant blocks |
| Payload reference | 32 | Location and codecs of the child page |

The reference is 72 bytes. Compare identifiers as integer pairs, not as serialized
little-endian bytes. Time bounds use the manifest unit. The time range is an
envelope over all identifiers in the page; it need not describe the first or last
identifier alone. This can admit extra reads, but cannot exclude matching rows.
Every part/page identifier interval requires `first <= last`, and all part/page/
block time intervals require `min_timestamp <= max_timestamp`, even when pruning
would skip their payloads.

Child pages follow block order. Their identifier bounds may touch at a shared
series, just like part bounds. Do not require strict disjointness or use an
exclusive next-child lower bound. Validate each loaded page's derived identifier
and time bounds against its reference. At depth 2 all children must be leaves;
reject an internal child. Child byte ranges are ordered, disjoint, within the
metadata area, and precede the parent. The root ends immediately before the trailer.

An internal root has `12 + 72 * N` decoded bytes, so it holds at most
`floor((65536 - 12) / 72) = 910` leaf references. The writer must account for the
next descriptor's leaf placement before writing its block. If the descriptor
would require a 911th leaf, finish this part and start the block in a new part.

#### Navigation and equal-timestamp example

For a requested qualified identity and optional half-open interval `[start, end)`,
select every part/page whose identifier interval contains the identity and whose
time envelope overlaps the request:

```text
(start is absent || max_timestamp >= start)
    && (end is absent || min_timestamp < end)
```

For multiple selected identities, intersect the ordered ID set with the inclusive
identifier range. A table restriction also intersects this selection. Visit each
selected page once per request. Apply the same overlap test to leaf blocks, then
filter decoded timestamps exactly. Metadata bounds are conservative candidates.
No predecessor-only or unique-child lookup is used: a shared identity or timestamp
can require several adjacent children, and even several parts.

```text
Part 0, depth 2:
  Root R
    ├─ Leaf A: IDs [(7,42),(7,42)], times [0,100]
    │    block ordinal 0: (7,42), times [0,100]
    └─ Leaf B: IDs [(7,42),(7,43)], times [0,200]
         block ordinal 1: (7,42), times [100,200]
         block ordinal 0: (7,43), times [0,50]

Part 1:
  Leaf/root C: IDs [(7,43),(7,43)], times [50,80]
    block ordinal 1: (7,43), times [50,80]
```

For `(7,42)` and `[100,101)`, visit both A and B, keeping all rows at timestamp
100 from blocks 0 and 1. Do not decode `(7,43)` payloads. For `(7,43)` and
`[50,51)`, visit B and part 1's C: both may contain rows at 50. A `[51,60)` request
for `(7,43)` can skip B's `(7,43)` block even though B's time envelope overlaps.
For `(7,42)` and `[90,95)`, A is still visited although its block starts before
the requested interval. Global time envelopes must not be computed from just
the first and last blocks: B's range is `[0,200]`, not `[100,50]`.

The series-index format has no sample timestamps or time bounds.

### Trailer

| Field | Bytes | Meaning |
| --- | ---: | --- |
| Root page reference | 72 | Inclusive ID/time bounds and payload reference |
| Index depth | 4 | 1 or 2 |
| File version | 4 | 1 |
| File length | 8 | Includes header and trailer |
| `payload_end` | 8 | End of column area, start of metadata area |
| Magic | 8 | ASCII `GTSAMPLE` |
| Trailer checksum | 8 | XXH64 of the preceding 104 trailer bytes, seed zero |

The trailer is 112 bytes. Locate it using the recorded object size without a stat
request. Validate the checksum, magic, version, length, depth, area boundaries,
root bounds against the manifest part descriptor, and root end position before
traversal. Column references must lie in `[16, payload_end)`; page references lie
in `[payload_end, file_length - 112)`. The header and trailer versions must agree.

## Encodings and compression

Sample column encoding IDs below are distinct from series column encoding IDs,
even where their numeric values coincide. Metadata and compression have separate
namespaces as well. Never reuse an assigned ID with a different meaning.

| Namespace | ID | Meaning |
| --- | ---: | --- |
| Column encoding | 1 | Timestamp constant step |
| Column encoding | 2 | Timestamp delta-of-delta |
| Column encoding | 3 | Constant sequence |
| Column encoding | 4 | Sequence delta |
| Column encoding | 5 | Nullable Float64 XOR |
| Column encoding | 6 | Constant operation type |
| Column encoding | 7 | Raw operation types |
| Metadata encoding | 1 | Leaf/internal page layout above |
| Compression | 0 | Raw encoded bytes |
| Compression | 1 | Zstd frame |

Encode first, then compress each chunk/page independently. The default writer
tries Zstd level 3 and chooses it only if strictly smaller than the raw encoded
bytes. Compression 0 requires equal stored and
decoded lengths. Compression level is a writer choice; decoding information is
in the frame.

Unsigned varints use base-128/LEB128: seven data bits per byte and a high
continuation bit. Writers emit the shortest representation. Readers reject
truncation, more than ten bytes, a tenth byte greater than 1, and trailing bytes
after the expected values. ZigZag maps signed i64 `x` to
`((x as u64) << 1) ^ ((x >> 63) as u64)` and reverses that mapping on decode.
All wrapping arithmetic below is modulo 2^64, not signed overflowing arithmetic.

### Timestamp encodings 1 and 2

Encoding 1 contains `first: i64` and `step: i64`, exactly 16 bytes. Use it when
every adjacent difference is the same nonnegative, representable i64. A singleton
uses step zero. Reconstruct by checked addition; reject overflow. Zero step is
valid for repeated timestamps.

Encoding 2 contains the first i64, followed for two or more rows by the first
delta as a ZigZag varint, then ZigZag varints of differences between deltas.
Compute deltas and delta differences with wrapping arithmetic; reconstruct in
the same way. This permits differences across the full signed timestamp range.
After either decoding, require nondecreasing signed timestamps and exact agreement
of first/last values with the descriptor. Consume exactly the recorded row count.
Use encoding 1 when applicable, otherwise encoding 2.

### Sequence encodings 3 and 4

Encoding 3 contains one u64, repeated for all rows. Encoding 4 contains the first
u64 followed by each wrapping u64 difference from its predecessor, reinterpreted
as i64 and written as a ZigZag varint. Decode with wrapping addition. Use 3 if
all sequences are equal, otherwise 4. Preserve all u64 bit patterns. Sequence
need not be monotonic across different timestamps; equal-timestamp runs retain
the input's nonincreasing order.

### Nullable Float64 encoding 5

```text
validity_kind <u8>                 # 0 = all valid, 1 = bitmap
validity <ceil(row_count/8) bytes>  # only for kind 1
first_non_null_bits <u64>
xor_deltas <one uvarint per remaining non-null value>
```

Bitmap bit `i % 8` in byte `i / 8` is one for a present value; unused high bits of
the final byte are zero. Kind 0 has no bitmap and requires zero null count. Kind
1 requires `0 < null_count < row_count` and an exactly matching count of unset
in-range bits. All-null columns have no descriptor or payload, so every stored
Float64 chunk contains at least one non-null value.

Process non-null values in row order. Write the first IEEE-754 bit pattern as
u64, then varints of XOR with the previous **non-null** bit pattern. Nulls do not
reset that previous value. Decode exactly `row_count - null_count` values and
scatter according to validity. Preserve all bits, including signed zero, NaN
payloads, infinities, and subnormals. Null and NaN are distinct. The unspecified
value-buffer bits behind nulls are not part of the logical preservation contract.

### Operation encodings 6 and 7

Operation values are u8: 0 delete, 1 put, matching Mito's current `OpType` values.
Unknown operation values fail decoding. Encoding 6 stores one byte repeated for
the whole block; encoding 7 stores exactly one byte per row. Use 6 when all values
are equal, otherwise 7. Neither encoding filters delete rows or discards their
field values.

### Limits and evolution

An individual chunk has at most 1 MiB of encoded bytes after decompression and
1 MiB plus 64 KiB of stored bytes. Pages have at most 65,536 decoded and 131,072
stored bytes. Bound sizes before allocating, fetching, or decompressing. Validate
counts against enclosing bytes before allocating descriptor vectors; row counts
must also satisfy the decoded-block formula. Null output materialization remains
subject to the same block bound even when chunks are omitted.

Unknown file versions, depths, reserved fields, roles, or metadata codecs fail
open/read explicitly. Column role must agree with its encoding: time 1/2,
sequence 3/4, Float64 5, operation 6/7. An unknown column encoding or compression
can be skipped if its chunk is not needed, but attempting to decode it returns
an unsupported-codec error. Known encodings assigned to a different role are
invalid metadata, even for an unprojected column. Unknown fields or malformed
references must not be interpreted as omitted all-null columns.

New column encodings or compression methods receive new IDs. Structural changes,
changed field meanings, or incompatible limits require a new version. Deeper
metadata trees require an explicit reader-support and rollout decision; an
unchanged layout alone does not make them readable by version-1 implementations.
