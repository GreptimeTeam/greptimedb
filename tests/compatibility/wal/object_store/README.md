# Object store WAL object fixtures

Frozen objects of the object store WAL, checked byte for byte by the format tests in
`src/log-store/src/object_store_wal/format.rs`, which never rewrite them:

```shell
cargo nextest run -p log-store -E 'test(test_format_matches_release_record_fixture)'
```

## `release_record.wal`

A version 1 object, sequence 3, holding entry `(3 << 20) | 1` of region 1 and, as the single
entry of the reserved region `u64::MAX` at the same position, the release records of region
`(2, 1)` through `(2 << 20) | 1` and of region `(3, 0)` in full. A reader that predates
release records decodes it as an ordinary version 1 object. 272 bytes, SHA-256
`7e8de4d50142f3b48c97a24f20744ee9c0d56f843daf3a36d00ba9b465a5d4b5`.
