# File-only compaction manifest compatibility

The hand-authored frozen fixtures use the JSON schema of `RegionEdit` and
`RegionManifest` at `4092fe6549`. They are schema fixtures, not captures from a
released binary. They preserve L0/L1 metadata, sequences and flush boundaries.

V1 publishes compaction results using ordinary file edits. It adds no task IDs,
phases, receipts, request epochs or extension-state fields to the manifest.
Task retries may be submitted more than once; publication checks current inputs.
The Enterprise `publish_table_compaction` API consumes a query-visible input
snapshot and returns `Applied` or `StaleInputs`. A stale attempt must re-snapshot
and replan, never re-import its old inputs. Even stale/error outcomes must not
trigger blind output deletion: an earlier attempt may already have published
those objects. Callers retain output protection until recovery or GC resolves
liveness.

Publication tests (including partial success, lost ACKs and reopened regions):

```sh
cargo nextest run -p mito2 --features enterprise -E 'test(engine::table_compaction)'
```

```sh
cargo nextest run -p mito2 -E 'test(manifest::tests::compatibility)'
cargo nextest run -p mito2 --features enterprise -E 'test(manifest::tests::compatibility)'
```

The tests exercise legacy delta/checkpoint reads and verify that serialization
still contains no task-state fields. Optional future core fields remain readable.
Unknown SST levels are rejected; the shared sentinel level (the u8 maximum)
requires Enterprise capability.
This does not establish downgrade safety after shared files are introduced.

The earlier, unpublished prototype persisted `table_compaction` / `extensions`
task records. Canonical delta/checkpoint decoders reject those records in both
builds rather than silently dropping recovery evidence. They require explicit
migration; this version does not resume that prototype's Prepare/Commit protocol.
