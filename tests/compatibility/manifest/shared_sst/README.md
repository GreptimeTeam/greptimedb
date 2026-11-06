# Shared SST level compatibility

`file-levels.json` freezes the persisted `FileMeta.level` values. Omitted fields
use the existing `FileMeta` defaults; this fixture deliberately tests the level
encoding rather than introducing a new SST or manifest representation.

Run the owning Rust fixture and the engine restart tests:

```sh
cargo nextest run -p mito2 -E 'test(shared_sst_level_compatibility)'
cargo nextest run -p mito2 --features enterprise -E 'test(engine::shared_sst)'
```

- Historical L0=0, L1=1, and omitted-level records retain their meanings.
- LEVEL_SHARED=2 round-trips without changing unrelated FileMeta fields.
- Unknown levels fail decoding/encoding, rather than panicking while indexing
  the fixed-size SST level container.
- The enterprise engine tests exercise ordinary manifest edits, close/reopen,
  stable registration sequences, stale registration rejection, and multi-region
  output installation. Registration and Apply introduce no new manifest action,
  durable task, or unbounded receipt collection.

This is a new persisted level meaning, **not** a promise that an older binary can
load shared SSTs. Callers must enable the path only after every serving/failover
node supports it; downgrade requires draining shared references first. There is
no SQL producer or enterprise deployment switch added by this engine PR, so the
internal API is exercised by Rust tests rather than a SQLness `case.toml`.
