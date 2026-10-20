# Raw-series label decode description compatibility

`v1_metadata.json` is a hand-authored, frozen set of Arrow schema metadata maps
for the v1 raw-series frame. It records the established layout/content/labels/
time metadata plus the new `greptime.series.label_decode` entry, whose value is a
JSON label decode description with an explicit `version`. The Rust unit test
parses these literal maps through `raw_series_frame_label_decode`; the fixture is
not produced by the schema builder.

Run the focused check:

```shell
cargo nextest run -p datatypes -E 'test(raw_series_label_decode_metadata_fixture)'
```

The fixture proves that a key-encoded frame's description decodes for both
primary-key encodings, that the ordered key columns and logical tags survive the
wire form with a mixed physical type set, that a logical tag must name a listed key
column, and that the reader rejects a key frame without a description, an explicit
frame that declares one, an unknown version, a missing or unknown field, a wrong
field type, and a non-JSON value.

A timestamp-typed tag column has its own case in the crate's unit tests rather
than here, so this fixture stays a single fixed layout.

The fixture does not prove interoperability with an old binary, nor does it test
an end-to-end client or service. Separate pack and IPC tests check that a packed
stream declares the description derived from its binding and that it agrees with
the schema a consumer receives.

`greptime.series.*` metadata is compared as exact text by remote merges, so the
text stored here is what the single writer produces: compact JSON in declaration
order. A reader must never regenerate it differently; semantically equal text with
different whitespace is a mismatch, not an accepted variant.

The description is versioned independently of the surrounding frame layout: a
reader rejects any version it does not implement instead of guessing, so a future
revision must add a new fixture case rather than relax this one.
