# Pending tail manifest fixtures

Frozen region manifests with pending WAL tails and a handoff floor, checked by the manifest tests
in `src/mito2/src/manifest/action.rs`, which never rewrite them:

```shell
cargo nextest run -p mito2 -E 'test(test_pending_tails_fixtures) | test(test_pending_tails_old_shape_compat)'
```

The first test decodes both files and requires the encoder to reproduce them byte for byte. The
second decodes them with the shapes from before pending tails existed, which must ignore the new
fields, and requires a manifest without pending tails to encode exactly as those shapes do.

## `delta.json`

A delta with one edit. It carries committed sequence `(1 << 40) + 5`, two pending tails, and the
handoff floor `4 << 32` of node 3. The tails are node 1, entries `(1 << 32, (1 << 32) + 5]` with a
reported upper bound, and node 2, entries `(2 << 32, 3 << 32]` with a listed upper bound. 424 bytes,
SHA-256 `520cbdc2cf51de4fb960d3c2f2194c126e3d23f02b4c511ed687705f48073c80`.

## `checkpoint.json`

A checkpoint of version 7 holding the same two tails and floor, flushed entry id `1 << 32`, no
files, and the region metadata of `basic_region_metadata` in `src/mito2/src/manifest/tests/utils.rs`.
1,245 bytes, SHA-256 `088f3f4caf04dbbf2734739aeb72a9a742e5ba8fb20f840dfd24123fb4027dd7`.
