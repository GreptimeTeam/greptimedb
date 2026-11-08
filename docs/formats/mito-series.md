# Mito columnar series format

This document specifies the version 1 on-disk layout, encodings, ordering rules,
and validation limits for series-index artifacts. For samples, see
[Mito columnar samples format](mito-samples.md).

## Artifact manifest

The catalog format identifier is `columnar-series`. Its wire version is separate
from that identifier and from all column-encoding and compression identifiers.
Legacy catalog entries and the `parquet` identifier keep their existing meaning.

One immutable artifact owns the following objects under a unique generation:

```text
<region_id>/series/<index_uuid>/manifest.json
<region_id>/series/<index_uuid>/part-0.series
<region_id>/series/<index_uuid>/part-1.series
...
```

`manifest.json` contains these fields:

| Field | Meaning |
| --- | --- |
| `format` | String `columnar-series` |
| `version` | Integer 1 |
| `region_id` | Physical region ID |
| `generation` | Index UUID, using the existing `FileId` serialization |
| `tags` | Ordered objects with `column_id` and `name`; all are nullable UTF-8 |
| `coverage` | Existing `SeriesIndexEntry` representation, with `artifact` omitted and `file_size` zero |
| `files` | Ordered objects with `part_id`, `file_size`, `num_series`, `first`, and `last` |

`part_id` is an unsigned 32-bit integer, starting at zero and increasing by one
in file order. The object name is derived as `part-<part_id>.series`, using the
decimal ID without leading zeros, under the artifact's generation prefix. No
path is stored in a file entry. `first` and `last` are inclusive identifiers,
represented as objects with `table_id` and `tsid`. Ranges are strictly ordered
and disjoint. Coverage and generation must match the catalog entry.

The tag list records the schema at build time, including tags currently absent
from some logical tables. The logical Arrow schema is `__table_id`, `__tsid`,
`__series_row_count`, followed by stored tags in manifest order. It has no
timestamp columns and is separate from the existing Parquet index schema.

`ArtifactMetadata.objects` lists the manifest and every part as `{ path, file_size }`
objects. The artifact-level `file_size` is their total stored size; the manifest
does not store its own size. Object paths derive from the generation and part IDs.
Part order comes from the manifest. An empty artifact has an empty file list and
no part files.

## Part file

```text
┌──────────────────────────────────────────────┐
│ Header                              16 bytes │
├──────────────────────────────────────────────┤
│ Independently encoded/compressed columns     │
│   Block 0: TSIDs, counts, tags               │
│   Block 1: ...                               │
├──────────────────────────────────────────────┤ ← payload_end
│ Leaf pages (one is the root at depth 1)       │
│ Optional internal root (depth 2)              │
├──────────────────────────────────────────────┤
│ Trailer                             96 bytes │
└──────────────────────────────────────────────┘
```

All fixed-width integers are little-endian. Offsets are absolute from the start
of the part; ranges are half-open. There is
no alignment padding. A string is `length <u64>` followed by that many UTF-8
bytes. Parse fields explicitly; Rust struct layout, `usize`, and enum declaration
order are not wire contracts. Reject trailing bytes in decoded structures.

### Header

| Field | Bytes | Value |
| --- | ---: | --- |
| Magic | 8 | ASCII `GTSERIES` |
| File version | 4 | 1 |
| Reserved | 4 | Zero |

### Column payloads

An artifact indexes the series in one Mito series-index bucket. Authoritative
data remains in Parquet SSTs; these indexes can be rebuilt. Each record contains:

| Logical column | Type | Storage |
| --- | --- | --- |
| `__table_id` | UInt32 | One constant in each block descriptor |
| `__tsid` | UInt64 | Independently encoded column chunk |
| `__series_row_count` | UInt64 | Independently encoded column chunk |
| Tag columns | Nullable UTF-8 | Independent dictionary/code chunks |

Group records from one table into a block, using a writer row limit that defaults
to 8,192. This is a packing choice, not a wire-format limit: every block records
its actual number of series. A writer can use another limit without changing
the file version, provided the encoded counts and payload sizes remain supported.
Readers must not assume that blocks contain 8,192 rows. Write each stored column
as an independent encoded and compressed chunk, then continue with the next
block. Chunks have no inline header: their locations, lengths, codecs, and
checksums are recorded in the later [metadata pages](#metadata-pages).

`(__table_id, __tsid)` is the complete identifier **under the current uniqueness
assumption**. Equal TSIDs in different tables are distinct. Records are ordered
lexicographically by this pair and identifiers do not repeat after aggregation.
There is no stored `__primary_key` column. Different label sets may hash to the
same TSID within a table; version 1 stores no collision discriminator.

The row count records input rows aggregated for each identifier. Tags are constant
for that identifier under the uniqueness assumption. The count is not necessarily
the query-visible row count after deduplication or snapshot filtering.

No per-series or per-block timestamps or timestamp unit are stored. Time coverage
is recorded only in the catalog bucket. Tags named `job` and `instance` are
ordinary tag columns, not dedicated identifier components.

#### Integer encoding 1

Start each chunk with `previous = 0`. For each value, compute wrapping u64
subtraction from the previous value, reinterpret the result as i64,
ZigZag-encode it, and write an unsigned base-128 varint. The previous value
becomes the current value. Decoding
uses wrapping addition after reversing ZigZag. This preserves all UInt64 values,
including differences beyond the signed range.

A varint has seven low data bits per byte and the high continuation bit. It is
at most ten bytes; the tenth byte can contain only bit zero and cannot continue.
The writer uses the shortest representation. Decode exactly the block's number
of series and reject truncation, overflow, and trailing bytes. TSID values must
be strictly increasing and agree with the descriptor's first/last values.

#### Dictionary encoding 2

```text
dictionary_byte_length <uvarint>
dictionary:
    entry_count <u64>
    repeated entries:
        present <u8>              # 0 = null, 1 = UTF-8 string
        string <length + bytes>   # only if present = 1
row_codes <one uvarint per series>
```

Entry zero is null; remaining entries are distinct non-null strings in
first-occurrence order. Null cannot occur at another entry. The empty string is
a non-null entry with zero bytes. Dictionary length includes its count and entry
framing. Codes must be in range; the dictionary and code streams must each be
consumed exactly.

### Metadata pages

The metadata is a tree that maps identifier ranges to column locations:

- A **leaf page** describes data blocks and where their column chunks are stored.
- An **internal page** points to other metadata pages, with an identifier range
  for each child so the reader can skip unrelated children.
- The **root page** is the tree's entrypoint, referenced by the trailer. It is
  either a leaf or an internal page; there is no separate root page encoding.

Version 1 supports depths 1 and 2 only; internal pages cannot occur below the root.

Each page is encoded and compressed independently. The diagrams below show its
**decoded bytes**. The stored bytes are the compressed form of that entire page;
the kind and entry count are inside the compressed payload. The parent's page
reference supplies the location, lengths, checksum, and codecs needed to read it.
For the root, that reference is in the trailer.

#### Leaf page

A leaf uses kind 0. Its entry count is the number of block descriptors, not the
number of series. Block descriptors have variable length because their column
names and label statistics have variable length.

```text
┌───────────────────────────────────────────────────────┐
│ kind = 0                                        u32   │
│ entry_count = N                                 u64   │
├───────────────────────────────────────────────────────┤
│ Block descriptor 0                                    │
│   table_id                                      u32   │
│   first_tsid, last_tsid                         2 × u64│
│   num_series, num_columns                       2 × u32│
│   Column descriptor 0                                │
│     name                                      string  │
│     payload reference                       32 bytes  │──→ column chunk
│     null_count, statistics_kind                 2 × u32│    in payload area
│     optional label min/max strings                    │
│   ...                                                 │
│   Column descriptor num_columns - 1                   │
├───────────────────────────────────────────────────────┤
│ ...                                                   │
│ Block descriptor N - 1                                │
└───────────────────────────────────────────────────────┘
```

The leaf contains locations and statistics, not the column values themselves.
Its table ID supplies the constant `__table_id` value for each series in a block.
The exact descriptor fields are specified under [Block descriptor](#block-descriptor).

#### Internal page

An internal page uses kind 1. Its entry count is the number of child metadata
pages. Each entry is a fixed-size 56-byte page reference:

```text
┌───────────────────────────────────────────────────────┐
│ kind = 1                                        u32   │
│ entry_count = N                                 u64   │
├───────────────────────────────────────────────────────┤
│ Child page reference 0                       56 bytes │
│   first = (table_id, tsid)                    12 bytes │
│   last  = (table_id, tsid)                    12 bytes │
│   payload reference                          32 bytes │──→ child page
├───────────────────────────────────────────────────────┤
│ ...                                                   │
│ Child page reference N - 1                    56 bytes │
└───────────────────────────────────────────────────────┘
```

An internal page's decoded size is `12 + N × 56` bytes. It is always the root and all its children are leaves. An internal page has no
block descriptors or column data. At the 65,536-byte page limit, the root holds
at most `floor((65536 - 12) / 56) = 1170` leaf references.

#### Root page and traversal example

If all block descriptors fit in one leaf, that leaf is also the root and the
trailer records depth 1. Otherwise the root is an internal page and the trailer
records depth 2: root → leaves. Other depths are invalid in version 1.

For example, four data blocks can be indexed by two leaves and an internal root.
The ranges below are inclusive `(table_id, tsid)` bounds; they do not imply that
every ID between the endpoints exists.

```text
Trailer: depth = 2
  root reference: [(7,10), (7,49)]
    │
    ▼
  Root R: internal page, kind = 1, entry_count = 2
    ├─ reference [(7,10), (7,29)] ──→ Leaf A: kind = 0, entry_count = 2
    │                                  ├─ Block 0: table 7, TSIDs [10,19]
    │                                  └─ Block 1: table 7, TSIDs [20,29]
    └─ reference [(7,30), (7,49)] ──→ Leaf B: kind = 0, entry_count = 2
                                       ├─ Block 2: table 7, TSIDs [30,39]
                                       └─ Block 3: table 7, TSIDs [40,49]

Physical order, from payload_end to end of part:
  [compressed Leaf A] [compressed Leaf B] [compressed Root R] [Trailer]
```

To find `(7,35)`, read the trailer and Root R, then follow only the reference to
Leaf B. Its Block 2 descriptor locates the TSID and requested label/count chunks
in the earlier column payload area. Decode the TSID chunk to find the exact row;
metadata bounds only identify a candidate block. Leaf A and Block 3's payloads
are not needed.

The root's own bounds and storage reference live in the trailer; its page bytes
have exactly the leaf or internal layout above. At depth 2 the writer emits all
leaves and then the internal root. At depth 1 it emits only the single leaf.

#### Page ordering and size limits

Pages are nonempty. A leaf's bounds are its first/last block bounds; an internal
page's bounds are its first/last child bounds. Child key ranges are strictly
ordered and disjoint, as are block ranges. Children physically precede their
parent and their stored ranges do not overlap. Columns within a leaf are likewise
physically ordered and non-overlapping.

Pages target 65,536 decoded bytes including the 12-byte prefix. A descriptor
never straddles pages. A descriptor larger than the target gets its own leaf,
capped at 8 MiB including framing; a larger descriptor fails the build. Internal
pages never exceed 65,536 decoded bytes. Stored pages are capped at 8 MiB plus
64 KiB. Readers validate sizes and counts before allocating or decompressing.

#### Payload reference

Every column chunk and metadata page has this 32-byte reference:

| Field | Bytes | Meaning |
| --- | ---: | --- |
| Offset | 8 | Absolute location of stored bytes |
| Stored length | 4 | Bytes to fetch |
| Decoded length | 4 | Bytes after decompression, before decoding |
| Checksum | 8 | XXH64 of stored bytes, seed zero |
| Encoding ID | 2 | Column or metadata encoding, according to context |
| Compression ID | 2 | Compression of the encoded payload |
| Reserved | 4 | Zero |

The offset is u64, so a payload can start beyond the 4 GiB boundary. The two u32
lengths limit a single stored or decoded payload to at most `2^32 - 1` bytes;
they do not limit the whole part to 4 GiB. File length and `payload_end` are also
u64. This version keeps u32 payload lengths because chunks and pages have much
smaller bounded sizes. Check `offset + stored_length` in u64 with overflow
handling, and validate decoded sizes before converting to allocation sizes.

The checksum is verified before decompression. Decompression must produce exactly
the declared length. Encoding and compression are independent: encode first,
compress second. Compression parameters needed for decoding must be in the codec
frame; the Zstd level is a writer choice, not a decoding requirement.

#### Page reference

A page reference is 56 bytes: `first table ID <u32>`, `first TSID <u64>`,
`last table ID <u32>`, `last TSID <u64>`, then a 32-byte payload reference.
Bounds are inclusive. Compare identifiers as integer pairs, not by the byte
ordering of their little-endian representation.

#### Block descriptor

A block contains series from one table; its actual row count is recorded below.
The writer defaults to 8,192 series per block. A table boundary ends the current
block without necessarily ending the part. The decoded block descriptor has the
following fields in order:

| Field | Encoding |
| --- | --- |
| Table ID | u32 |
| First and last TSID | u64 each, inclusive |
| Number of series | u32, nonzero |
| Number of stored columns | u32 |
| Column descriptors | Repeated in logical-schema order, excluding table ID |

Each column descriptor contains `name <string>`, `payload <32-byte reference>`,
`null_count <u32>`, and `statistics_kind <u32>`. Statistics kind 0 has no extra
fields; kind 1 adds minimum and maximum non-null strings, in that order. Only
tag columns use kind 1. String bounds use UTF-8 byte lexicographic order.

TSID and row count are mandatory, non-null columns. An all-null tag column can
be omitted; its logical values are all null
and its inferred null count equals the number of series. A present tag chunk
has string bounds if it contains any non-null values. Null and the empty string
are distinct. Names must be unique and belong to the manifest schema.

### Trailer

| Field | Bytes | Meaning |
| --- | ---: | --- |
| Root page reference | 56 | Root bounds and payload location/codecs |
| Index depth | 4 | Leaf-only index = 1 |
| File version | 4 | 1 |
| File length | 8 | Includes header and trailer |
| `payload_end` | 8 | End of column area, start of metadata area |
| Magic | 8 | ASCII `GTSERIES` |
| Trailer checksum | 8 | XXH64 of the preceding 88 trailer bytes, seed zero |

The root ends immediately before the trailer. Supported depths are 1 and 2. Page ranges
lie in the metadata area; column ranges lie between the header and `payload_end`.
Checksums detect accidental corruption; they do not authenticate data.

## Codec selection and evolution

Encoding IDs have separate column and metadata namespaces. IDs are explicitly
assigned and never reused. Version 1 supports:

| Namespace | ID | Meaning |
| --- | ---: | --- |
| Column encoding | 1 | Wrapping delta, ZigZag, unsigned varint integer stream |
| Column encoding | 2 | Nullable UTF-8 dictionary and unsigned varint codes |
| Metadata encoding | 1 | Block descriptors / page references defined above |
| Compression | 0 | None; stored length equals decoded length |
| Compression | 1 | Zstd frame |

Compression 0 stores encoded bytes without compression. Compression 1 stores
a Zstd frame; its level does not affect the decoding contract.

Changing the writer's block row limit (for example, from 8,192 to 16,384) requires
no codec ID, trailer extension, or file-version change: the block descriptor
already supplies the actual row count. The same applies to part rollover choices.
These choices remain subject to field-width and reader resource limits. They
are distinct from changing the serialized structure of a descriptor or trailer.

A new encoding gets a new ID and its own documented payload interpretation;
it can coexist with older encodings in the same file. A codec can define its
own payload prefix for parameters. A new compression method likewise receives
a new ID. Neither change requires rewriting the outer layout. Changing the outer structures or the meaning of an existing field
requires a new file version. Reserved fields must remain zero in version 1.

Unknown versions, reserved bits, or metadata codecs fail open/read explicitly.
An unknown column codec may be skipped when its column is neither projected nor
needed for filtering; attempting to read it returns an unsupported-codec error.
It must never be interpreted as null or silently decoded with a default codec.

## Format limits

Version 1 permits metadata depths 1 and 2 only. At most 1,170 leaf references
fit in the root; an artifact may use additional parts when that limit is reached.
Part size and block packing choices do not alter the wire layout.

An individual column chunk is capped at 64 MiB decoded and 64 MiB plus 1 MiB
stored. A block contains at most 8,388,608 series. The manifest is at most 64 MiB.
Offsets, lengths, and counts must fit their enclosing areas without arithmetic
overflow. Counts must not imply allocations larger than the enclosing decoded
payload permits.

## Local cache compatibility

The local series-index cache has a root `version` marker independent of this
format version. Before workers start, a missing or incompatible marker causes
the entire derived cache (including samples, ranges, and catalogs) to be reset.
Matching-version restarts preserve the cache.
