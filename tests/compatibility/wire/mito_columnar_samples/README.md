# Mito columnar samples version 1

This fixture freezes the version-1 sample wire contract in
[`mito-samples.md`](../../../../docs/formats/mito-samples.md). It is exercised by
the owning Mito Rust test, not SQLness discovery:

```shell
cargo nextest run -p mito2 frozen_v1_samples_fixture_and_unknown_codec
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

The test installs these exact bytes and recorded object sizes, opens them through
`SamplesFormat`, and verifies the values and order. It also changes a column
codec to an unknown ID while updating checksums to test that unrequested columns
can be skipped and requested unknown codecs fail explicitly. Legacy catalog
identifier coverage remains in `series_index::catalog` tests.

Do not regenerate this fixture just to make a reader change pass. Preserve its
bytes for future reader compatibility; introduce a separate versioned fixture
when the approved format changes. This is a frozen-byte compatibility check,
not a cross-binary upgrade/downgrade test. Older binaries are not promised to
understand the new opt-in `columnar-samples` catalog identifier.
