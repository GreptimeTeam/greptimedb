# Raw-series label decode description compatibility

`v1_metadata.json` is a hand-authored, frozen set of Arrow schema metadata maps
for the v1 raw-series frame. It records the established layout/content/labels/
time metadata plus the new `greptime.series.label_decode` entry, whose value is a
JSON label decode description with an explicit `version`. The Rust unit test
parses these literal maps through `raw_series_frame_label_decode`; the fixture is
not produced by the schema builder. `v1_physical_types.json`, described under
[Physical types](#physical-types), is the one generated fixture here: it records
the serde text of every `ConcreteDataType` variant.

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

## Physical types

`physical_type` is `ConcreteDataType`'s own serde form, so this description's
text embeds the serialization of a type defined in `src/datatypes/src/data_type.rs`
and its `types/` modules. That shape is not restated or owned here: whatever
`ConcreteDataType`'s serde writes is what a description carries, and the two
spellings that appear below — `{"String":{"size_type":"Utf8"}}` and
`{"UInt32":{}}` — are examples of it rather than a vocabulary this fixture
defines.

What freezes that shape is `v1_physical_types.json`: one entry per
`ConcreteDataType` variant, `{"variant": "<name>", "text": "<serde_json>"}`, with
`text` generated from `serde_json::to_string` rather than written by hand. The
crate's `every_datatype_round_trips_through_its_frozen_text` test fails when a
variant's serde no longer parses back or no longer re-serializes to the frozen
text, and `the_frozen_text_covers_every_datatype_variant` fails when a variant of
`ConcreteDataType` has no entry, because its exhaustive `match` names every
variant. Changing `ConcreteDataType`'s serde, or any `text` recorded here, changes
the text two nodes compare, so it requires bumping `LABEL_DECODE_VERSION` and
regenerating this fixture together.

`logical_tags` is written in the writer's canonical order (sorted by
`(column_id, name)`). The order carries no meaning — the decoder sorts tags by
name when it binds them — but fixing it means two producers of the same labels
write the same bytes, which is what lets the same equality rule serve both a
local comparison and the byte-exact remote one. The writer canonicalizes a copy
of the description for exactly that reason, leaving the caller's binding order
alone.

Run the physical-type check too:

```shell
cargo nextest run -p datatypes -E 'test(datatype) or test(frozen_text)'
```
