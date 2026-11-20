# Frozen multi-leaf samples part

This directory is a **readable** positive fixture for the version-1 sample wire
contract in [`mito-samples.md`](../../../../docs/formats/mito-samples.md): the current
writer produced these bytes, and the current reader must read them back unchanged. It
complements [`../v1-part.hex`](../v1-part.hex) in the parent directory, which stays a
*negative* fixture: that artifact was written under hash-only identity and must be
rejected rather than read, and
`frozen_v1_samples_fixture_is_rejected` keeps it that way.

`v2` is this fixture's generation name, not a wire version. The part is version 1, with
the header, key area, page layout, and trailer the document specifies; a fixture
generation name is only a way to add a new frozen artifact beside the old one.

| File | Size | Content |
| --- | ---: | --- |
| `part.hex` | 2,238 bytes | The 1,085-byte part, hex, 16 bytes per line (68 lines) |
| `manifest.json` | 752 bytes | The manifest the reader opens first |

Its object sizes are the exact lengths of those files, because the reader locates the
trailer from the recorded part size instead of statting the object. The manifest names
one part, `region_id` zero and one generation; those identities are only what the
fixture's writer produced, and the test installs the manifest and part under the paths
they derive. Its suite:

```shell
cargo nextest run -p mito2 frozen_v2_multi_leaf_fixture_is_readable
```

## What it pins

One part holds six single-row blocks, grouped two per leaf, so the part's root is an
internal page of three children with `depth = 2` in its trailer:

```text
Part 0, depth 2:
  Root (kind 1): three page references
    ├─ Leaf A (kind 0): block ordinals 0, 1
    ├─ Leaf B (kind 0): block ordinals 2, 3
    └─ Leaf C (kind 0): block ordinals 4, 5
```

The six rows, in stored order, are one series `(table_id=1, tsid=7)` with sequential
timestamps and sequences:

| # | Leaf | `table_id` | `tsid` | `ts` | sequence | operation | `field_0` bits |
| --- | --- | ---: | ---: | ---: | ---: | --- | --- |
| 0 | A | 1 | 7 | 100 | 0 | put | `0x0000000000000000` (`0.0`) |
| 1 | A | 1 | 7 | 101 | 1 | put | `0x8000000000000000` (`-0.0`) |
| 2 | B | 1 | 7 | 102 | 2 | put | `0x3ff8000000000000` (`1.5`) |
| 3 | B | 1 | 7 | 103 | 3 | put | `0xc000000000000000` (`-2.0`) |
| 4 | C | 1 | 7 | 104 | 4 | delete | null |
| 5 | C | 1 | 7 | 105 | 5 | put | `0x4000000000000000` (`2.0`) |

The values are written as raw Float64 bit patterns because row 1 is negative zero:
`0.0` and `-0.0` compare equal, so only the stored bits show that the format preserved
it. Row 4 is a delete marker with a null field, which the format keeps instead of
filtering.

Each leaf here is far below the 65,536-byte leaf limit; the writer splits after two
descriptors because that is the leaf page the regenerator configures. What the fixture
pins is not that split: it is that a part with several leaves reaches them through an
internal root, that the root is the only page measured against the 16 MiB root limit
while its children stay leaf pages, and that the rows behind such a root decode
unchanged. A single-leaf part is already covered by the round-trip tests in
`src/mito2/src/series_index/samples/tests.rs`, which assert that its root is the leaf
itself, so no single-leaf fixture lives here.

## Regeneration

The fixture is written by an ignored test that runs the real writer:

```shell
cargo test -p mito2 --lib regenerate_frozen_v2_multi_leaf_fixture -- --ignored
```

Regenerate only because the approved format itself changed, never to make a reader
change pass. A reader that refuses these bytes is a reader regression; the bytes stay
frozen until an approved format change makes them obsolete, and a new generation then
gets its own directory beside this one instead of overwriting these files.
