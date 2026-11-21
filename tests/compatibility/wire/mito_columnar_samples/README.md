# Mito columnar samples version 1

These fixtures freeze the version-1 sample wire contract in
[`mito-samples.md`](../../../../docs/formats/mito-samples.md). They are exercised by
the owning Mito Rust tests, not SQLness discovery:

```shell
cargo nextest run -p mito2 frozen_v1_samples_fixture_is_rejected
cargo nextest run -p mito2 frozen_v2_multi_leaf_fixture_is_readable
```

## Fixture generations

The `vN` prefix names a fixture generation, not a wire version: every fixture in this
directory packs version 1.

| Fixture | Expectation |
| --- | --- |
| [`v1-part.hex`](v1-part.hex), [`v1-manifest.json`](v1-manifest.json) | Negative: written under hash-only identity, rejected while its manifest is opened, then rebuilt from SSTs |
| [`v2-multi-leaf/`](v2-multi-leaf/README.md) | Positive: a part the current writer emits, with three leaves under a depth-two internal root, read back row for row |

`v1` freezes what must stay refused; `v2` freezes what the current writer emits and the
current reader must accept. Adding a generation does not retire an older one, and
neither is a cross-binary upgrade/downgrade test.

## `v1-part.hex`: hash-only identity, rejected

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

## `v2-multi-leaf/`: multi-leaf root, readable

[`v2-multi-leaf/`](v2-multi-leaf/README.md) is the readable counterpart: the ignored
regenerator writes it with the real writer, and the non-ignored test opens those stored
bytes and checks every row. It covers what the v1 bytes cannot — a part whose root is
an internal page, one leaf limit for the leaves and a separate root limit for the root
— so a change that silently stops reading multi-leaf parts fails here instead of in
production.

Regenerate it only because the approved format changed, never to make a reader change
pass: bytes a reader refuses are a reader regression, and a new generation gets its own
directory instead of overwriting this one. Rejection and readability are therefore both
frozen, and neither fixture is edited to follow a reader it was meant to check.

## Catalog coverage fixtures

`legacy-catalog-entry.json` and `daily-catalog-entry.json` freeze the optional
source-coverage extension, exercised by `frozen_sample_catalog_coverage`. Empty
object lists isolate catalog decoding; the existing part fixture tests actual
payload reads. Legacy entries cover one SST. Daily entries cover the entire
listed source set and become ineligible if any source disappears.

`sparse-daily-catalog-entry.json` freezes explicit populated-day coverage per source.
Older daily catalogs decode with empty coverage and are rebuilt before reuse.
The source Parquet files and sample payload encoding are unchanged.
