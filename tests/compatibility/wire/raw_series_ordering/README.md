# Raw-series ordering schema metadata compatibility

`v2_metadata.json` is a hand-authored, frozen set of Arrow schema metadata maps
for the v2 series frame. It records the established layout/content/labels/time
metadata and the optional ordering entry, for raw frames and for evaluated ones.
The Rust unit test parses these literal maps through `series_frame_ordering` and
through `SeriesFrameDeclaration::parse`; the fixture is not produced by the
schema builder.

Run the focused check:

```shell
cargo nextest run -p common-query -E 'test(raw_series_ordering_metadata_fixture)'
```

The fixture proves that absent ordering metadata defaults to `Contiguous`, the
two known values decode, an unknown value is rejected, and that a whole
declaration is accepted or refused consistently with its layout and content: a
`v3` layout, an unregistered `greptime.series.*` key, a raw frame that declares a
grid, and any content/time or content/labels contradiction are refusals. The
evaluated cases are read as declarations only; the streaming round trips for
evaluated frames are covered by the crate's evaluated tests.

Because the declaration is cross-checked, a frame that names `evaluated` content
is only an evaluated frame when it also declares `explicit` labels, `grid_spans`
time, and a `greptime.series.grid`. `read_key_then_time` parses as a declaration
(a raw frame may legitimately claim it), but no writer produces one on an
evaluated frame: computed values have no storage key to be ordered by, so both the
schema builder and `unpack_evaluated_series` refuse it. The fixture keeps a
hand-written evaluated frame that states it, because a reader must still refuse
such a frame rather than decode it under an ordering its producer could not have
guaranteed.

The fixture does not prove interoperability with an old binary, nor does it test
an end-to-end client or service. Separate pack/IPC tests check the declared
ordering schema and batch schema agreement for both orderings, including empty
streams, and check the same for the evaluated frame across Arrow IPC.
