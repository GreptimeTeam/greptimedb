# Write-retry metadata compatibility

`retry_hints.json` freezes gRPC `x-greptime-err-retry-hint` metadata values
and the expected current client decoding. These are constructed protocol fixtures,
not captures from released binaries.

The client test converts real `tonic::Status` metadata through the production
error decoder. Missing and unknown values default to `NonRetryable`; existing
`retryable` and `non_retryable` values retain their meaning; the new
`retry_after_route_refresh` value preserves its distinct hint. The application
status code and gRPC status code remain unchanged.

```shell
cargo nextest run -p client -E 'test(test_retry_hint_wire_compatibility)'
```

`test_transport_error_parity_and_datanode_mapping` separately exercises these
recognized values over an actual gRPC client/server connection.

An older decoder that recognizes only the two legacy strings treats the new
string as unknown and uses its existing `NonRetryable` fallback. This is a source
compatibility observation, not a claim that an old binary was run. These tests do
not promise that an old client will refresh routing or perform automatic retries.

The fixture is hand-authored and must not be regenerated from test output.
