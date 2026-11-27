# Mito columnar samples version 2: depth-three tree

This fixture freezes the first version-2 samples artifact: one part whose metadata
tree is three levels deep, as specified in
[`mito-samples.md`](../../../../docs/formats/mito-samples.md). Version 2 does not
fix the tree height — 910 is a page's fanout, and a part grows another level
instead of splitting — so this is one height among many; the deepest level here is
still an internal page over leaves, as in any taller tree. It is exercised by the
owning Mito Rust test, not SQLness discovery:

```shell
cargo nextest run -p mito2 frozen_depth_three_fixture_reads
```

`manifest.json` (749 bytes) records tree version 2 while the part keeps framing
version 1, so the extra level is announced by the manifest alone. `part.hex` holds
the single part: 1,274 bytes with one block per leaf, two leaves per group, and the
root right before the trailer.

| Range | Content |
| --- | --- |
| 0..16 | Header |
| 16.. | Four blocks' chunks: timestamps, sequences, operations, Float64 values |
| | Key area: one exact key copy plus checksum per series |
| | Leaf pages: 4, one block each |
| | Group pages: 2, two leaf references each |
| | Root page: 1, two group references |
| ..1274 | Trailer with the root reference, depth 3 and checksum |

The blocks are ordered by exact series key: sparse labels `(aa, z)`, `(bb, z)`,
`(bb, z)`, `(cc, z)` with `table_id` 1 and `tsid` 1, 2, 2, 3. The `bb` series
therefore owns the last leaf of the first group and the first leaf of the second,
which is what makes this fixture a walk test rather than only a page-decoding test:
the reader has to enter a group between two blocks of one series and keep that
series' ordinal sequence. Its ordinals are 0 and 1.

Expected rows, in read order. Every row is a put with value `0.0`:

| `table_id` | `tsid` | timestamp (ms) | sequence | operation | `field_0` |
| --- | --- | --- | --- | --- | --- |
| 1 | 1 | 10 | 1 | 1 (put) | 0.0 |
| 1 | 2 | 20 | 1 | 1 (put) | 0.0 |
| 1 | 2 | 30 | 1 | 1 (put) | 0.0 |
| 1 | 3 | 40 | 1 | 1 (put) | 0.0 |

The part descriptor records `num_rows: 4`, `num_blocks: 4`, `file_size: 1274`, and
identifier bounds `(1, 1)` to `(1, 3)` with time bounds `[10, 40]`. The manifest's
`region_id` is 0 because the fixture's region comes from the test util; only the
artifact identity fields (`source_file_id`, `generation`) are fixed, as in the
version-1 fixture.

The part was written by the current writer with the packing options a test can
configure — one block per leaf and two leaves per internal page — so four leaves
already exceed what one internal page holds. The tree shape, page encodings, page
limits, and manifest are the production ones; only the configured capacities are
small enough to keep the fixture readable.

A version-1 reader refuses this manifest while opening it, before it reads a single
part byte: it admits only tree version 1, and only version 2 admits trees deeper
than two levels. The Rust tests cover a taller tree (four levels at a reduced
fanout) directly; this fixture stays the smallest readable depth-3 example.
Do not regenerate this fixture to make a reader change pass.
Preserve its bytes for future reader compatibility; introduce a separate versioned
fixture when the approved format changes.
