# Pending tails discard wire compatibility

A generated protobuf fixture of `greptime.v1.region.RegionRequest`, not a capture
from a released binary.

`discard_pending_tails.pb` uses the schema at greptime-proto revision
`08af5da6e3ba849f982da7f9c1b1cb5a8ac0ac55`, which adds
`DiscardPendingTailsRequest` as body field 24. Logical contents: region
`4398046511105` (table `1024`, region number `1`) and datanode `7`.

SHA-256: `374e0c28eaee73e4df3f2e79a6345bf70caa589800c9eb5a19314d08a9ef5fa0`.

Run the regression:

```shell
cargo nextest run -p store-api -E 'test(test_discard_pending_tails_request_wire)'
```

The test checks that the fixture decodes to a pending tails discard of that
region and datanode, and that the decoded message encodes back to exactly the
fixture bytes, so the request stays at body field 24.

## Reproduce the fixture

With `PROTO_ROOT` pointing to the `proto` directory of that revision:

```shell
printf '%s\n' 'discard_pending_tails { region_id: 4398046511105 datanode_id: 7 }' |
  protoc --proto_path="$PROTO_ROOT" --encode=greptime.v1.region.RegionRequest \
    greptime/v1/region/server.proto > discard_pending_tails.pb
```
