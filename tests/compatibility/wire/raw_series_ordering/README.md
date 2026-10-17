# Raw-series ordering schema metadata compatibility

`v1_metadata.json` is a hand-authored, frozen set of Arrow schema metadata maps
for the v1 raw-series frame. It records the established layout/content/labels/
time metadata and the new optional ordering entry. The Rust unit test parses
these literal maps through `raw_series_frame_ordering`; the fixture is not
produced by the schema builder.

Run the focused check:

```shell
cargo nextest run -p common-query -E 'test(raw_series_ordering_metadata_fixture)'
```

The fixture proves that absent ordering metadata defaults to `Contiguous`, the
two known values decode, and an unknown value is rejected. It does not prove
interoperability with an old binary, nor does it test an end-to-end client or
service. Separate pack/IPC tests check the declared ordering schema and batch
schema agreement for both orderings, including empty streams.
