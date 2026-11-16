# Authority Head v1 fixtures

`initial_v1.json` and `owned_v1.json` freeze the two RFC examples as compact JSON,
with a final newline excluded from the byte-for-byte codec assertion. Field order
is the RFC example order. UUIDs encode as canonical hyphenated strings.

All fields are required, including nullable `owner`; unknown Head and owner fields
are ignored. Required fields or semantic changes require a new format version.
Versions other than 1 are rejected. Counters reject overflow.

These fixtures are exercised by `common-meta` Rust tests, independently of SQLness:

```sh
cargo nextest run -p common-meta -E 'test(election::object_store::head::tests::)'
```

They establish format compatibility, not qualification of a storage backend.
