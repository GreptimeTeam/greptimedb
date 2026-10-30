# Region statistic heartbeat compatibility

`without_flushed_parquet_bytes.json` freezes the JSON extension payload before
`flushed_parquet_bytes` was added. The store-api test checks zero defaulting,
preservation of existing fields, and serialization of the new counter.

Run: `cargo nextest run -p store-api -E 'test(region_statistic_flush_bytes_compatibility)'`.
