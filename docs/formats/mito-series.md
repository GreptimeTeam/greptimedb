# Mito columnar series format

**Status: implemented as an opt-in internal backend.** The byte assignments
below are the version 1 compatibility contract. This document describes series-index artifacts only;
sample files and query-consumer integration are separate work.

The presentation follows the field tables and diagrams in the
[Prometheus chunks format documentation](https://github.com/prometheus/prometheus/blob/main/tsdb/docs/format/chunks.md).
This is an independent format, not a Prometheus-compatible file.

The format sections follow storage order: artifact manifest, then each part's
header, column payloads, metadata pages, and trailer. Writer/reader behavior and
validation follow the layout specification.

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
and disjoint. Coverage is captured from
the build request; source SST IDs remain diagnostic information, not a reuse
criterion. Validate coverage and generation against the catalog handle on open.

The tag list records the schema at build time, including tags currently absent
from some logical tables. The logical Arrow schema is `__table_id`, `__tsid`,
`__series_row_count`, followed by stored tags in manifest order. It has no
timestamp columns and is separate from the existing Parquet index schema.

The manifest is written only after every part is closed successfully. The
existing catalog publication commits the artifact. `ArtifactMetadata.objects`
lists the manifest and every part, and `file_size` is their total stored size.
These lifecycle object paths are derived from the generation and part IDs;
the existing artifact ownership API remains unchanged.
The reader obtains part order from the manifest, not object-list ordering.
An empty build has a manifest with an empty file list and creates no part files.

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
There is no stored `__primary_key` column. The writer extracts identifiers and
labels from input sparse primary keys without retaining their bytes on disk.

Repeated input rows for a series, including rows split across input batches,
contribute to its row count. Tags are constant for that identifier under the
uniqueness assumption. Aggregation keeps
the current index writer's input visibility semantics; this format does not add
sample deduplication or make the row count an exact query-visible count for
update-mode data.

No per-series or per-block timestamps are stored, and the series schema has no
timestamp unit. The catalog's bucket coverage allows coarse time pruning. A series selected
from an overlapping bucket is only a candidate for a time-constrained query;
SST/sample reads must apply the exact time filter. The format reader cannot prune
individual series by time or accept filters on timestamp columns.

TODO: evaluate support for optional per-series minimum/maximum timestamps and
benchmark their storage size, build cost, and time-pruning benefit before adding
them to the format. Keep this version free of per-series and per-block time bounds.

TODO: benchmark storing exact input primary-key bytes and reconsider collision
handling. Different label sets can hash to the same TSID within one table. This
version neither preserves a collision discriminator nor promises collision-safe
identity. Add the corresponding TODO at the implementation's identity boundary.

Dedicated `job_id` and `instance_id` identifier/locality components are deferred.
Ordinary tags named `job` or `instance` remain ordinary tag columns.

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
consumed exactly. Keep dictionary/code buffers during filtering rather than
expanding one owned string per row.

TODO: implement and benchmark a plain UTF-8 encoding alternative for string
chunks, particularly high-cardinality labels. Compare dictionary encoding with
null bitmap + offsets + UTF-8 bytes, including Zstd-compressed size, build cost,
and filtering/decoding cost. Evaluate per-chunk selection using estimated encoded
size rather than cardinality alone. A plain encoding gets its own encoding ID;
it can use the existing compression IDs and coexist with dictionary chunks.
This port implements dictionary strings only; the alternative and automatic
selection remain future work.

### Metadata pages

The metadata is a tree that maps identifier ranges to column locations:

- A **leaf page** describes data blocks and where their column chunks are stored.
- An **internal page** points to other metadata pages, with an identifier range
  for each child so the reader can skip unrelated children.
- The **root page** is the tree's entrypoint, referenced by the trailer. It is
  either a leaf or an internal page; there is no separate root page encoding.

This implementation supports only depths 1 and 2. It does not build or traverse
internal pages below the root. The existing depth field and page references
leave room for deeper trees as future work.

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

An internal page's decoded size is `12 + N × 56` bytes. In this implementation,
it is always the root and all its children are leaves. An internal page has no
block descriptors or column data. At the 65,536-byte page limit, the root holds
at most `floor((65536 - 12) / 56) = 1170` leaf references.

#### Root page and traversal example

If all block descriptors fit in one leaf, that leaf is also the root and the
trailer records depth 1. Otherwise the root is an internal page and the trailer
records depth 2: root → leaves. Depth 3 would mean root → internal pages → leaves,
but is not implemented in this port.

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

TODO: evaluate depth 3+ if larger parts or metadata volumes justify it. The
current page-reference representation can point to another internal page, so
the extension need not change the byte layout. It would require new writer and
reader traversal support, benchmarks, and a rollout policy for older readers.
Current readers reject depths other than 1 or 2 explicitly before traversal;
they must never interpret unsupported trees as empty indexes. Future writers
must not assume that an unchanged file version implies old-reader support for
new depths. Older files remain readable by a future deeper-tree implementation.

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

TODO: benchmark replacing repeated column names with numeric references to a
shared schema, including metadata size, decode cost, and selective-read latency.
The manifest already stores the tag schema; leaf descriptors currently repeat
names to identify their chunks. Keep the current descriptors for this port.
Names can be deduplicated later, but payload locations, codecs, null counts, and
statistics remain specific to each block's column chunk.

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

TODO: evaluate and benchmark a variable-length Protobuf footer located by a
small fixed trailer, so optional file metadata can evolve without enlarging the
fixed trailer. Measure footer size, open/read cost, and compatibility behavior
before adopting it. This port retains the 96-byte trailer above; no Protobuf
footer or schema-reference changes are included.

## Codec selection and evolution

Encoding IDs have separate column and metadata namespaces. IDs are explicitly
assigned and never reused. Version 1 supports:

| Namespace | ID | Meaning |
| --- | ---: | --- |
| Column encoding | 1 | Wrapping delta, ZigZag, unsigned varint integer stream |
| Column encoding | 2 | Nullable UTF-8 dictionary and unsigned varint codes |
| Metadata encoding | 1 | Block descriptors / page references defined above |
| Compression | 0 | None; stored length equals decoded length |
| Compression | 1 | Zstd frame; writer default is level 3 |

The default writer uses compression 1 for columns and metadata. Compression 0
provides an explicit uncompressed representation and a second dispatch path to
test. Codec selection is internal; this change adds no public codec configuration.

Changing the writer's block row limit (for example, from 8,192 to 16,384) requires
no codec ID, trailer extension, or file-version change: the block descriptor
already supplies the actual row count. The same applies to part rollover choices.
These choices remain subject to field-width and reader resource limits. They
are distinct from changing the serialized structure of a descriptor or trailer.

A new encoding gets a new ID and its own documented payload interpretation;
it can coexist with older encodings in the same file. A codec can define its
own payload prefix for parameters. A new compression method likewise receives
a new ID. Neither change requires rewriting the outer layout or changing the
format API. Changing the outer structures or the meaning of an existing field
requires a new file version. Reserved fields must remain zero in version 1.

Unknown versions, reserved bits, or metadata codecs fail open/read explicitly.
An unknown column codec may be skipped when its column is neither projected nor
needed for filtering; attempting to read it returns an unsupported-codec error.
It must never be interpreted as null or silently decoded with a default codec.

## Writer and resource bounds

The implementation lives behind `SeriesIndexFormat::writer` and `FormatWriter`.
Its input remains sorted flat `RecordBatch` data. Reuse the existing writer's
sparse-primary-key and string-tag validation, but do not depend on its timestamp
aggregation or timestamp-bearing output schema. This writer consumes primary
keys and counts input rows without decoding timestamp values. Preserve
aggregation across `write` calls; reject decreasing identifiers and checked-count
overflow.

Write column payloads as blocks complete, retaining only the current series,
current block, and bounded per-part descriptors. Finish a part at block boundaries
when the stored position reaches 512 MiB or cumulative serialized descriptors
reach 8 MiB. These are soft limits: the completing block may overshoot them.
The root's leaf-reference capacity is a separate hard packing limit: account for
the next block descriptor's leaf-page placement before writing that block. If
it would require more than 1,170 leaves, finish the current part and write the
block to a new part. Never create a third metadata level. Emit leaf pages, the
internal root only when needed, and then the trailer.

For bounded decoding, an individual column chunk is capped at 64 MiB decoded and
64 MiB plus 1 MiB stored. Oversized chunks fail the build; no partial artifact is
published. A block contains at most 8,388,608 series, bounding each decoded u64
column and row-selection buffer to 64 MiB. Bound the manifest to 64 MiB before parsing. These safety limits are
part of version 1, not new server configuration. Check offsets, lengths, counts,
and arithmetic before allocation; row counts and lengths alone must not authorize
allocations larger than the enclosing decoded data permits.

## OpenDAL I/O and lifecycle

Use `object_store::ObjectStore` (the OpenDAL operator) and its `Reader`/`Writer`
types, as other Mito index implementations do. Do not add a parallel storage trait
or a native filesystem adapter. Memory and filesystem OpenDAL services and the
existing mock layers provide test backends.

- Fetch the manifest and part lengths through OpenDAL. Fetch headers, trailers,
  metadata pages, and selected column ranges through asynchronous range reads.
  Reuse `fetch_byte_ranges` where applicable. Its range coalescing can fetch gaps
  between selected chunks; those bytes do not cause unselected columns to decode.
- Open outputs through `writer_with`, using `DEFAULT_WRITE_BUFFER_SIZE` and
  `DEFAULT_WRITE_CONCURRENCY`. Stream bytes without buffering an entire part.
  Call `close` before reporting success; count all committed object bytes.
- Use existing `OpenDalSnafu` error conversion. Keep codec work separate from I/O
  and run substantial synchronous compression/decompression on Mito's shared
  blocking runtime rather than blocking async worker threads.
- Until successful finish, the writer owns every output in its generation.
  Failure or explicit abort aborts live OpenDAL writers, removes completed
  unpublished objects, and aborts each live OpenDAL writer to clean its own atomic-write temporary
  file. Cleanup never scans temporary files by generic part basenames. A cancellation/drop guard schedules the same cleanup
  on the shared runtime. Cleanup errors are reported without hiding the original
  operation error. Process-crash orphan recovery remains the existing lifecycle's
  responsibility; an incomplete generation is never published as valid.
- After successful finish, ownership transfers to the build/catalog lifecycle.
  Reuse existing publication-failure cleanup and deferred retirement. A reader
  and each returned stream retain a `SeriesIndexFileHandle` until dropped.

## Reader API and execution

Add a reader factory to `SeriesIndexFormat`, accepting the store and pinned
artifact handle, and returning a format reader. The reader exposes its stored
Arrow schema and a `read` method returning a stream of projected record batches.
Reads have independent traversal state and may outlive the reader factory.

The initial read request contains an ordered projection of column names, an
optional table ID, an optional set of `MetricSeriesId` values, and a conjunction
of `SimpleFilterEvaluator` filters. No ID restriction means all IDs; an explicitly
empty set means no rows. Reject unknown projection/filter columns instead of
guessing their meaning. An empty projection still preserves selected row counts.
Returned rows preserve identifier order, even when IDs are not projected.

The reader accepts filters only on its stored logical columns; it has no time
filter. Bucket-level time selection belongs to the caller, and exact time
filtering belongs to subsequent SST/sample reads. This API evaluates supplied
filters exactly; conversion of arbitrary query expressions or schema-evolution predicates into
conservative index candidates remains the future query adapter's responsibility.

1. Open and validate the manifest and selected parts' framing. Use file bounds
   to skip irrelevant parts and page bounds to traverse only relevant metadata.
2. Apply conservative block pruning using identifier and label bounds.
   Missing statistics never exclude possible matches. Label filters do not prune
   internal pages. Full scans need not load the entire metadata tree at once.
3. Decode IDs when needed for selection and predicate columns before output
   columns. Apply existing Mito filter/null semantics, including evaluation on
   tag dictionaries where possible. Null is not converted to the empty string.
4. Fetch and decode remaining projected columns only for blocks with surviving
   rows; each selected chunk is still decompressed as a whole. Materialize output
   only for selected rows. Synthesize table IDs from the block constant and
   omitted tags as null arrays. Reuse already decoded filter columns. Output
   batches target 8 MiB of materialized values (one larger row is allowed), so
   repeated dictionary values do not expand an entire block at once. This is a
   reader packing choice and does not change the wire format.

Keep page traversal state to the active leaf and pending references. There is no
cross-read cache in this implementation. Centralize fetch/validate/decode work in
`load_metadata_page` and `load_column_chunk` entrypoints.

TODO: integrate these entrypoints with Mito's existing cache system. Future keys
need region/store identity, immutable artifact generation, part ID, byte range,
checksum, codec IDs, and representation. Keep that information available; do not
introduce a new cache framework, global cache, configuration, or unused parameter.

Ordinary series scans continue using the existing Parquet path and fall back to
SST discovery for this format. Merely adding a format reader must not mark these
artifacts as supported query coverage. Samples remain disabled by default.

## Compatibility and validation

Existing Parquet files and catalogs remain readable. The new backend is opt-in
through the internal format selection; defaults do not write this format. An
older binary need not decode new codec bytes. These rebuildable artifacts must
not become necessary for reading authoritative SST data. There is no promise
that old binaries understand the new catalog identifier.

Validation covers the layouts, identifier assumption, codec IDs, schema handling,
and safety limits above. Implementation acceptance includes:

- Frozen fixtures under `tests/compatibility/` read by Mito tests, plus legacy
  catalog and Parquet regression tests. Fixture checks must not rely only on
  writer/reader round trips that could share the same mistake.
- Empty input, repeated series across batches, mixed tables/schemas, equal TSIDs
  across tables, nullable/empty tags, forced rollover, depths 1 and 2, and
  oversized payloads/descriptors.
- Root-capacity rollover before a third level is needed, using reduced packing
  limits in tests. Reject depth 0 and unsupported depths including 3 before
  traversal, and reject an internal child where a leaf is required at depth 2.
- Exact projection/filter results, no irrelevant payload decoding, selective
  range reads, and independent readers/streams with caching absent.
- Different block row limits, including 16,384, read by the same format reader
  without a version change. Validate references beyond 4 GiB with small mocked
  payloads, plus overflowing offsets and oversized individual payload lengths.
- Part-ID-to-path derivation, invalid/misordered part IDs, absence of timestamp
  columns, and rejection of timestamp filters. Varying input timestamps must not
  change encoded series records when identifiers, labels, and row counts match.
- Truncation, bad checksums, invalid offsets/counts/order, dictionary corruption,
  unsupported codec/version IDs, and bounded decompression/decoding failures.
- Real-format reconciliation, reopen, publication failures, OpenDAL write/close
  failures, abort/cancellation cleanup, accounting, and retirement with pinned
  streams. Failed builds must not replace a valid catalog entry.
- `cargo nextest run -p mito2`, crate Clippy, formatting, and license checks for
  new source files. No SQL query behavior change is claimed by this phase.

Performance must be measured again after the port: the row-count column, checksums,
metadata framing, OpenDAL range coalescing, and the absence of decoded caches
change the measured workload. Future experiments also cover exact primary keys,
dedicated job/instance locality components, and optional per-series time bounds.
Shared-schema column references and a Protobuf footer require separate benchmarks
and remain outside this port.
Depth 3+ metadata is also deferred until larger parts justify the added traversal.
No performance equivalence is asserted by this document.
