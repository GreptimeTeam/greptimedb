# Mito columnar samples version 1

This fixture freezes the version-1 sample wire contract in
[`mito-samples.md`](../../../../docs/formats/mito-samples.md). It is exercised by
the owning Mito Rust test, not SQLness discovery:

```shell
cargo nextest run -p mito2 frozen_v1_samples_fixture_is_rejected
```

`v1-manifest.json` and `v1-part.hex` describe a 397-byte part with raw compression,
one leaf/root, and two samples for `(table_id=7, tsid=42)` at timestamp 100 ms.
The first row is a put with sequence 9 and Float64 negative zero. The second is a
delete with sequence 8 and a null field. Both rows must survive decoding.

The part was assembled independently of the Rust writer, using explicit
little-endian fields and XXH64 with seed zero. Its layout is:

| Range | Content |
| --- | --- |
| 0..16 | Header |
| 16..32 | Constant-step timestamps: first 100, step 0 |
| 32..41 | Delta sequences: first 9, ZigZag delta -1 |
| 41..43 | Raw operations: put 1, delete 0 |
| 43..53 | Nullable XOR: bitmap mode, validity 0x01, negative-zero bits |
| 53..285 | Leaf/root: one block, four column references |
| 285..397 | Trailer with root reference and checksum |

The current reader writes the same wire version as this fixture, so the version
number alone does not reject it. The manifest carries no `identity_encoding`
discriminator, and the part records only `(table_id, tsid)`, which is a hash: a
part whose two series were merged is structurally valid and ordered exactly like a
correct one. The test installs these exact bytes and recorded object sizes and
asserts that opening them through `SamplesFormat` fails on the missing
`identity_encoding` discriminator, which is what makes maintenance rebuild the
artifact from its SSTs instead of reusing it. Legacy catalog identifier coverage
remains in `series_index::catalog` tests.

The part bytes above also predate two later version-1 shapes, neither of which is
migrated: block descriptors now carry a fixed-size key reference into a separate key
area instead of the key bytes, and the trailer records that area (`key_end`) and is
120 bytes, not 112. The fixture is still rejected for the same reason — the
discriminator is missing — and that rejection happens while opening the manifest,
before any part byte is decoded, so the frozen bytes are not a reader for the current
part layout and must not be regenerated to look like one.

Do not regenerate this fixture just to make a reader change pass. Preserve its
bytes for future reader compatibility; introduce a separate versioned fixture
when the approved format changes. This is a frozen-byte compatibility check,
not a cross-binary upgrade/downgrade test. Older binaries are not promised to
understand the new opt-in `columnar-samples` catalog identifier.

## Mito columnar samples version 1 with the suffix key area

`suffix-v1-manifest.json` and `suffix-v1-part.hex` freeze the suffix key area at manifest
version 1, the shape this format writes while it needs no compact internal page, and they are
exercised by the owning Mito Rust test:

```shell
cargo nextest run -p mito2 frozen_suffix_samples_fixture_is_readable
```

The fixture name carries both fields because they are independent: the manifest's `version`
names the metadata layout and its `identity_encoding` names the key-area layout, so this fixture
is the suffix identity at version 1 and the compact fixture below is the whole-key identity at
version 2.

The part is 1,115 bytes: two leaves under one depth-two root, three one-row
blocks, and a 54-byte key area. Its manifest names
`identity_encoding: exact-sparse-key-area-suffix` at `version: 1`, and its root is the fixed
internal page: a version-1 artifact never holds a compact one.

| Block | `(table_id, tsid)` | Labels | Rows | Stored key area entry |
| --- | --- | --- | --- | --- |
| 0 | 7, 42 | `tag_0="a"` | 100 ms, put, 1.5 | 15 suffix bytes + checksum |
| 1 | 7, 42 | `tag_0="b"` | 200 ms, delete, 2.5 | 15 suffix bytes + checksum |
| 2 | 7, 43 | none | 300 ms, put, null | checksum only (length 0) |

| Range | Content |
| --- | --- |
| 0..16 | Header |
| 16..84 | Column payloads of blocks 0 and 1, raw |
| 84..109 | Column payloads of block 2, raw (no field column) |
| 109..163 | Key area: three stored suffixes, each with its XXH64 |
| 163..639 | Leaf 0: blocks 0 and 1 |
| 639..839 | Leaf 1: block 2 |
| 839..995 | Root: two leaf references |
| 995..1115 | Trailer (120 bytes) with depth 2 and both area ends |

Blocks 0 and 1 share one `(table_id, tsid)` and differ only in their labels, so a
reader that reconstructs keys from the descriptor's identifier returns two distinct
series rather than one merged series. Block 2's key is the reserved prefix alone,
which is the empty-suffix case: its reference length is zero and its stored entry is
only the checksum of the reconstructed key.

The part was assembled independently of the Rust writer, from the wire
specification in [`mito-samples.md`](../../../../docs/formats/mito-samples.md), using
explicit little-endian fields, the sparse key encoding, and XXH64 with seed zero. Do
not regenerate it to make a reader change pass: it exists to prove that a third
implementation of this version can read what this build writes.

The older whole-key key area is covered by
`series_index::samples::tests::keys::whole_key_area_artifact_is_still_readable`, which
rewrites a written artifact into that layout; the version-1 fixture above stays a
rejection test.

`legacy-catalog-entry.json` and `daily-catalog-entry.json` freeze the optional
source-coverage extension, exercised by `frozen_sample_catalog_coverage`. Empty
object lists isolate catalog decoding; the existing part fixture tests actual
payload reads. Legacy entries cover one SST. Daily entries cover the entire
listed source set and become ineligible if any source disappears.

`sparse-daily-catalog-entry.json` freezes explicit populated-day coverage per source.
Older daily catalogs decode with empty coverage and are rebuilt before reuse.
The source Parquet files and sample payload encoding are unchanged.

# Mito columnar samples version 2 with a compact internal page

`compact-v2-manifest.json` and `compact-v2-part.hex` freeze the compact internal page
(metadata encoding 2) introduced with manifest version 2. The manifest names the whole-key area,
`exact-sparse-key-area`: version 2 permits a compact internal page and says nothing about how keys
are stored, so this fixture is the whole-key half of the version-2 column. It is exercised by the
owning Mito Rust test:

```shell
cargo nextest run -p mito2 -E 'test(compact_fixture_v2_reads)'
```

The part is 825 bytes, has depth 2, and its root is a compact page with two entries, one restart
and one predicted from it. Both leaves hold one block of the same series
`(table_id=7, tsid=42)`; the second block continues the first at timestamp 200 and ordinal 1. The
key area holds one sparse-key copy of that series with its XXH64, and it was assembled from the
sparse framing field by field, independently of the Rust writer.

| Range | Content |
| --- | --- |
| 0..16 | Header |
| 16..84 | Column chunks: constant-step time 100, sequence 9, one put, negative zero, then time 200, sequence 10, one put, 2.5 |
| 84..144 | Key area: one sparse key and its checksum |
| 144..388 | Leaf page: one block, four column references |
| 388..632 | Leaf page: the series' second block |
| 632..705 | Compact root: header, two entries, one restart offset |
| 705..825 | Trailer with the root reference, depth 2, and `key_end` |

A reader that only knows manifest version 1 refuses this artifact while opening the manifest, with
no part read, which is why the version is part of the fixture. Do not regenerate these bytes to
make a reader change pass; introduce another versioned fixture when the approved format changes.

The fourth combination, the suffix key area under version 2, has no frozen fixture: it is the
shape this writer produces for any part large enough to need a compact internal page, so
`series_index::samples::tests::compact_page::suffix_key_area_with_a_compact_root_reads` writes it
and reads it back instead of freezing bytes.
