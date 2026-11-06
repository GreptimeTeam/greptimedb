# Table-compaction manifest compatibility

These hand-authored, frozen fixtures use the JSON schema of `RegionEdit` and
`RegionManifest` at `4092fe6549` (before table-compaction receipts). They are
schema fixtures, not captures from a released binary. `legacy-edit.json` keeps
both original persisted levels, sequences and flush boundaries;
`legacy-manifest.json` omits the new receipt state as old checkpoints do.

Run the owning Rust test, without SQLness or an Enterprise coordinator:

```sh
cargo nextest run -p mito2 -E 'test(manifest::extension)'
cargo nextest run -p mito2 --features enterprise -E 'test(manifest::extension)'
```

The tests check old-format defaults, preservation of levels/sequences, omission
of unused extension fields, and rejection of unsupported levels. Generic manifest
extension envelopes preserve the existing top-level `table_compaction` JSON key;
there is no new `extensions` wrapper on disk. Both delta and checkpoint decoders
reject unknown extensions rather than silently discarding their state.

The default OSS build contains only the envelopes and storage/GC mechanisms. It
rejects table-compaction state and shared SST metadata. Typed receipts and remap
policy are enabled only by `enterprise`. The follow-up protocol layer adds the
Enterprise engine API and checkpoint/reopen tests for registration, preparation,
commit and receipt cleanup.

This is forward-read compatibility, **not** permission to downgrade after
`LEVEL_SHARED=2` is used. A coordinator must gate every participating engine on
`TABLE_COMPACTION_PROTOCOL_VERSION` before admitting shared references. Mixed
old/new engines must not start table compaction.
