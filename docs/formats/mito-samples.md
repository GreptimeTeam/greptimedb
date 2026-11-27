# Narthex format: samples

The **Narthex format**, selected by `sst_format = 'narthex'`, is optimized
for Prometheus-style metric data. It separates series identities and string tags
from timestamped numeric samples.

The format has two artifact roles: `NarthexSamples` owns sample data, and
`NarthexSeriesIndex` indexes exact series identities.
Region manifests serialize these roles as `narthex_samples` and
`narthex_series_index`.

## Object inventory and publication

A samples SST owns one shared `samples.idx` and numbered `part-<n>.samples`
objects in its immutable file directory. Catalog-backed samples use the same
names under their generation prefix. Part IDs start at zero. The owning region
manifest or samples catalog records the object sizes. Readers pin this inventory
for the lifetime of their streams. COPY copies the samples objects byte-for-byte
and resolves their paths from the target file handle.

The writer closes payload parts at block boundaries after reaching the 512 MiB
payload target. Leaves roll independently. The writer streams keys and completed
leaves to the index, spools fixed-width directory entries, and appends the spool
at close. After closing all objects it returns the inventory for publication.

## Shared index

All integer fields are little-endian. Offsets are absolute within the referenced
object. Checksums are XXH64 with seed zero.

```text
[key area][descriptor leaf] ... [key area][descriptor leaf]
[leaf directory][SST metadata block][fixed footer]
```

### Footer and SST metadata

The final 100 bytes contain, in order, the eight bytes `GTSMPIDX`, a u32
version (2), the immutable source and generation UUIDs (16 bytes each), a u64
directory offset, a u64 leaf count, a 32-byte payload reference to the metadata
block, and the checksum of the preceding 92 bytes. The directory ends at the
metadata block and the metadata ends at the footer. Check the geometry against
the recorded index size before reading variable-length sections.

The metadata block is bounded to 64 MiB decoded. Entries consist of a u32 key
length, u32 value length, key bytes, and value bytes. The required keys occur
once each. Readers can skip unknown entries by length.

| Key | JSON value |
| --- | --- |
| `schema` | Timestamp column (ID, name, unit) and ordered value fields (ID, name, type, nullable). |
| `parts` | Ordered list of part IDs, encoded sizes, row/block counts, first/last coarse IDs, and inclusive timestamp bounds. |

The whole block uses metadata encoding 1 and the raw or zstd compression ID in
its reference. Readers open the file with a bounded suffix read, fetching the
metadata separately if it extends beyond the suffix.

### Leaf directory and keys

The directory contains one 88-byte entry per leaf, in block order. Each entry
holds the key-area start (u64), first and last coarse IDs (u32 table ID and u64
TSID each), inclusive time bounds (two i64s), the leaf payload reference (32
bytes), and an eight-byte checksum of the preceding 80 bytes. The leaf starts
immediately after its keys. Readers can search directory pages by coarse ID and
skip leaves by table and time bounds.

Each leaf's key area contains one canonical exact series key per consecutive
series and its eight-byte checksum. Descriptors refer to these copies by offset
and length. A series crossing a leaf boundary gets another key copy. A series
crossing a payload part boundary inside a leaf continues using the same copy.
The reader checks exact key bytes after coarse pruning to distinguish collisions.

Decoded leaves are bounded to 64 KiB. A leaf starts with a zero u32 and a u64
block count followed by variable-width block descriptors. Each descriptor has a
u32 payload part ID, u32 table ID, u64 TSID, u64 index key offset, u32 key length,
i64 minimum and maximum timestamps, u64 per-series ordinal, u32 row count, and
u32 column count. Each column adds 44 bytes (u32 role, u32 column ID, payload
reference, u32 null count). Roles 1–4 identify time, sequence, operation, and
value fields respectively. An all-null value field is omitted. Column offsets
refer to the numbered payload part.

## Payload parts

Payload parts are concatenated column chunks in block order. The first chunk starts at offset zero.

### Column payloads and row semantics

Each block contains exactly one series and names it by that series' exact canonical
sparse primary key, which is stored once per series in the leaf's [key area](#leaf-directory-and-keys)
and referenced by the block descriptor. The key is the SST's own encoding of
`(table_id, tsid, label column-id/value pairs sorted by name, non-null only)`, and
`(table_id, tsid)` is only its order-preserving prefix. Two distinct series may
share a prefix. Timestamp, sequence, operation type, and each present value field
are independent chunks; their references are in the metadata, not inline headers.
Emit chunks in that order, with value fields following manifest order.

Rows preserve delete markers, duplicate timestamps, and equal-key ties, including
conflicting values at the same sequence. The format does not encode a deduplicated
or snapshot-filtered view.

Block keys are nondecreasing in byte-lexicographic order, timestamps are
nondecreasing within one exact key, and sequences are nonincreasing within equal
timestamps, including across parts. A change of exact key restarts timestamps and
ordinals, even when the coarse `(table_id, tsid)` prefix is unchanged. Equal
ordering keys retain input order; operation type is not a sorting key. The key area
follows the same order, one copy per series. Block keys never decrease, so a key that
is read must not sort before the key read before it; a repeated key must continue its
timestamps and ordinals rather than restart them. Because key bytes are read lazily,
the descriptor-only checks use the `(table_id, tsid)` prefix and the order, disjointness,
and reuse of key references inside one leaf, and only the keys a request actually loads
are checked for byte order and continuation.

Blocks split on an identity change or before the next row would exceed a packing
limit. There is no hourly split and no padding of short blocks. The default row
limit is 8,192, also the version-2 reader maximum; a writer may choose a smaller
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
| Metadata encoding | 1 | Descriptor leaf or SST metadata, according to its reference |
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
