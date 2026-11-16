# Truncate request wire compatibility

Two generated protobuf fixtures of `greptime.v1.region.TruncateRequest`, not
captures from a released binary.

`legacy_truncate_unflushed.pb` uses the schema at greptime-proto revision
`c46f7ee9c82ff90f5762ae192fcf8bb2b0d6046f`, before the `pending_tails` kind.
Logical contents: region `4398046511105` (table `1024`, region number `1`),
kind `unflushed`.

SHA-256: `e8d6b6eff673a7d8429d8be313b29d7608ac34da028cbc87742aacc6e484ed4b`.

`truncate_pending_tails.pb` uses the schema at greptime-proto revision
`582f2a84b287110fd4803f224dd8f650bd1dd041`, which adds the `pending_tails`
kind as field 5. Logical contents: region `4398046511105`, kind
`pending_tails` with `node_id` `7`.

SHA-256: `9187010e7f5fcc61ddf4a99f21c92355282f55b779c9616d1720b335a1029627`.

Run the regression:

```shell
cargo nextest run -p store-api -E 'test(test_truncate_request_wire)'
```

The test checks that each fixture decodes to its region request (`Unflushed`,
and `PendingTails` with node `7`), and that the decoded message encodes back to
exactly the fixture bytes, so the existing kind keeps its encoding and the new
kind stays at field 5.

## Reproduce the fixtures

With `PROTO_ROOT` pointing to the `proto` directory of the revision named for
each fixture:

```shell
printf '%s\n' 'region_id: 4398046511105' 'unflushed {}' |
  protoc --proto_path="$PROTO_ROOT" --encode=greptime.v1.region.TruncateRequest \
    greptime/v1/region/server.proto > legacy_truncate_unflushed.pb
printf '%s\n' 'region_id: 4398046511105' 'pending_tails { node_id: 7 }' |
  protoc --proto_path="$PROTO_ROOT" --encode=greptime.v1.region.TruncateRequest \
    greptime/v1/region/server.proto > truncate_pending_tails.pb
```
