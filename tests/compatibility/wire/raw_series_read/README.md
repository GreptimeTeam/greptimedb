# Raw-series read v1 wire fixture

`v1.json` is a frozen JSON control fixture for `RawSeriesRead`, the reserved
`QueryContext` extension consumed by an FE-authorized raw-series region read.
It covers the exact fixture in the owning Rust test: region `(1, 2)`, metric
`cpu`, ordered primary-key/tag columns `host` (id 1) and `rack` (id 4), field
`value` (id 2, non-null `Int64`), and time index `ts` (id 3, non-null
millisecond timestamp).

This is a control-format compatibility contract, not a protobuf fixture or a
capture from a released binary. The test decodes these literal bytes and
compares their semantic value to the expected fixture, then binds that decoded
value against the matching region metadata. It never regenerates or rewrites
the frozen JSON. Keep v1 decodable unchanged; any future format evolution
requires explicit compatibility handling rather than updating this fixture to
match a new encoder.

Run the focused contract test with:

```shell
cargo nextest run -p common-query -E 'test(decodes_frozen_v1_wire_fixture_and_binds_to_metadata)'
```

This fixture covers only the reserved raw-series extension. An omitted
extension remains an ordinary read, and unsupported extension versions are
rejected by the existing unit tests. An older datanode that ignores an unknown
extension may return an ordinary schema; the frontend's strict schema checks
must reject that response. This fixture does not claim a compatibility fallback
for such a datanode.
