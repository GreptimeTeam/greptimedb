# Legacy query-context wire compatibility

`legacy_query_context.pb` is a generated legacy-shaped protobuf fixture, not a
capture from a released binary. It uses the unchanged `greptime.v1.QueryContext`
schema at greptime-proto revision
`9533078d6fd4bdaddbca0750cb9bf7ad64b2e568` and omits the internal
`query.read_preference` extension.

Logical contents:

- Catalog `c1`, schema `s1`, timezone `UTC`.
- Unrelated extension `flow.return_region_seq = true`.
- Channel `Internal` (`255`); no snapshot sequences or explain options.

Run the real protobuf decode and session conversion regression:

```shell
cargo nextest run -p session -E 'test(test_legacy_query_context_wire_defaults_to_leader)'
```

The test checks that the omitted preference defaults to Leader, that the original
fields and unrelated extension survive conversion and re-encoding, and that
Leader does not introduce a preference carrier. Existing session tests cover
present invalid or unsupported values. This fixture does not demonstrate that
an old receiver honors a non-Leader preference sent by an updated node.

## Reproduce the fixture

With `PROTO_ROOT` pointing to the `proto` directory of the revision above:

```shell
printf '%s\n' \
  'current_catalog: "c1"' \
  'current_schema: "s1"' \
  'timezone: "UTC"' \
  'extensions { key: "flow.return_region_seq" value: "true" }' \
  'channel: 255' |
  protoc --proto_path="$PROTO_ROOT" --encode=greptime.v1.QueryContext \
    greptime/v1/common.proto > legacy_query_context.pb
```

Generated with `libprotoc 25.1`. The 48-byte fixture has SHA-256
`4127ebbee8cdb1375ef8ad52754be56ed206c12513e5f61c4354c04587cba638`.
The test never rewrites it and does not require protobuf map ordering to remain
stable.
