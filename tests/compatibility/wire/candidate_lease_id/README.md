# Legacy region lease wire compatibility

`legacy_granted_region.pb` is a generated legacy-shaped protobuf fixture, not a
capture from a released binary. It uses the `greptime.v1.meta.GrantedRegion`
schema at greptime-proto revision
`5870f5adc4d601173e742888e7db2032a690b583`, which has no `candidate_lease_id`.

Logical contents:

- Region `4398046511105` (table `1024`, region number `1`), role `Follower`.
- Unrelated extension `plugin = "payload"`.

SHA-256: `9b0c4796ce8c5094a0336c1febb00e3bef62c9deb4b8a04eccc26f28ea265ec0`.

Run the regression:

```shell
cargo nextest run -p store-api -E 'test(test_legacy_granted_region_wire)'
```

The test checks that the legacy bytes decode to a lease without a candidate
lease id, and that such a lease encodes back to exactly the legacy bytes, so a
release without the field reads it unchanged.

## Reproduce the fixture

With `PROTO_ROOT` pointing to the `proto` directory of the revision above:

```shell
printf '%s\n' \
  'region_id: 4398046511105' \
  'role: Follower' \
  'extensions { key: "plugin" value: "payload" }' |
  protoc --proto_path="$PROTO_ROOT" --encode=greptime.v1.meta.GrantedRegion \
    greptime/v1/meta/heartbeat.proto > legacy_granted_region.pb
```
