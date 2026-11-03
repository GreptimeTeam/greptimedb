# RawSeriesSource Substrait payload, version 1

`v1_payload.hex` is an independently hand-authored protobuf payload for version 1
of `greptime_proto::substrait_extension::RawSeriesSource` from greptime-proto #349
(revision `9b1dbfb`). It is not a capture from a released binary. It encodes metric
`http_requests_total`, tags `z`, `a`, and fields `value_b`, `value_a` in deliberately
unsorted order. The field numbers are version=1, metric_name=2, tags=3 (repeated),
and fields=4 (repeated). Substrait carries the child plan separately.

The owning promql tests decode this immutable fixture, check binding and schema
order, and compare exact re-encoding. They also check unsupported versions and
truncated bytes. The Substrait test verifies restoration of a real table-scan
child. Older receivers do not recognize the new extension node; this fixture does
not demonstrate mixed-version fallback or distributed acceptance.

```shell
cargo nextest run -p promql -p substrait@1.4.0-alpha.0 -E 'test(raw_series_source)'
```

Until the dependency pin includes #349, these tests require a local patch to that
proto revision.

The bytes are the literal protobuf encoding: `08 01` (version 1), `12 13` plus
UTF-8 metric bytes, `1a 01 7a`, `1a 01 61`, `22 07` plus `value_b`, and `22 07`
plus `value_a`. SHA-256 of the hex file including its trailing newline:
`5d748968ec8817762b63b6d3d0127fd7c9f606c05c25ac88aa0c2bc8700eae51`.
