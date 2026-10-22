# Mito metadata-count compatibility fixture

`legacy_file_meta.json` is a hand-curated legacy manifest FileMeta shape. It
intentionally omits both `num_rows` and the new `num_rows_is_logical` capability.
Level 1 is intentional: compaction level alone must not confer logical-count
trust. The fixture is consumed by the owning Rust test, not SQLness discovery.

```sh
cargo nextest run -p mito2 -E 'test(metadata_count_legacy_file_meta)'
```

The test checks missing fields, known physical counts without a capability,
optional-field serialization, and round-trip preservation of a certified new
file. An old reader/writer may discard the new optional field; the resulting
file remains readable but loses eligibility for exact metadata counts.
