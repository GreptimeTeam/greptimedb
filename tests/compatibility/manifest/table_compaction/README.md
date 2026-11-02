# Table-compaction manifest compatibility

These hand-authored, frozen fixtures use the JSON schema of `RegionEdit` and
`RegionManifest` at `4092fe6549` (before table-compaction receipts). They are
schema fixtures, not captures from a released binary. `legacy-edit.json` keeps
both original persisted levels, sequences and flush boundaries;
`legacy-manifest.json` omits the new receipt state as old checkpoints do.

Run the owning Rust test, without SQLness or an Enterprise coordinator:

```sh
cargo nextest run -p mito2 -E 'test(test_table_compaction_manifest_compatibility)'
```

The test checks old-format defaults, preservation of levels/sequences, omission
of unused protocol fields on serialization, and rejection of unknown levels.
This storage layer supplies replay support and deletion safety without exposing
coordinator operations. The follow-up protocol layer adds the engine API and
checkpoint/reopen tests for registration, preparation, commit and receipt cleanup.

This is forward-read compatibility, **not** permission to downgrade after
`LEVEL_SHARED=2` is used. A coordinator must gate every participating engine on
`TABLE_COMPACTION_PROTOCOL_VERSION` before admitting shared references. Mixed
old/new engines must not start table compaction.
