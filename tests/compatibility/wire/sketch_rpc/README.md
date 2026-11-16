# Sketch RPC wire fixtures

These protobuf request bytes fix the field numbers for region 42, the
millisecond interval [1, 2), column `x`, and a distinct-count query.
`request.hex` omits sampling; `sampled_request.hex` adds max_files=5 and seed=7.
The bytes are written independently of the Rust encoder.

Run `cargo nextest run -p store-api -E 'test(sketch_rpc_wire_fixture)'`.
These fixtures check additive sampling decoding and exact wire encoding, not
support for sketch RPCs in binaries predating this RPC. The client separately
rejects a sampled response that omits sampling diagnostics.
