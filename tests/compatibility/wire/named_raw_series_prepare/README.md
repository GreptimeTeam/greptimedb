# Named raw-series preflight contract v1

`v1_supported.json` and `v1_unsupported.json` are hand-authored, frozen JSON
bodies of the named raw-series preflight action
`greptime.prepare_raw_series.v1` (`PREPARE_RAW_SERIES_ACTION`).

- `v1_supported.json` approves the request with the `contiguous` ordering. Its
  nested `binding` value is the same version-1 request frozen in
  [`../raw_series_request/v1_request.json`](../raw_series_request/v1_request.json)
  (identical JSON text; the envelope embeds it without the source file's
  trailing newline).
- `v1_unsupported.json` is the frozen unsupported shape, exactly
  `{"version":1,"supported":null}`: the version-1 envelope with an explicit
  `null` `supported` field.

The envelope requires `version` (only `1` is supported) and a present-but-nullable
`supported` field. An approving `supported` object rejects unknown fields and
carries exactly the existing raw-series request plus an ordering of `contiguous`
or `read_key_then_time`; `interleaved`, unknown or missing orderings, duplicate
keys, unknown fields, and malformed bindings are rejected. The nested request
keeps its existing deserializer and validation rules; the nested physical
metadata is not checked for unknown fields by this contract.

Run the focused check:

```shell
cargo nextest run -p common-query named_raw_series
```

The fixture proves the version-1 envelope decodes as written and that the
unsupported shape is frozen. It does not prove interoperability with an old
binary; it records the contract this revision writes and reads.
