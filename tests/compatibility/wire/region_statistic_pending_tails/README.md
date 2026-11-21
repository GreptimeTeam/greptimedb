# Region statistic compatibility

Two JSON fixtures written by the serializers at GreptimeDB commit
`48aa0ed3ae151f2a436b2a5295ce8cd10ece5a9d`, before the `has_pending_tails` field:

- `legacy_region_statistic.json`: a `store_api::region_engine::RegionStatistic`, as a datanode
  puts it into the `__region_statistic` extension of a heartbeat region stat.
  SHA-256: `e46a90c4596e38a3253c7d70c8878c1a101f111b79457f7b48b39a1561cfe6a5`.
- `legacy_region_stat.json`: the `common_meta::datanode::RegionStat` the metasrv derives from
  that heartbeat region stat and stores in `DatanodeStatValue`.
  SHA-256: `8ae052b52653cf3e20b7e5c8085846fb027e3daaaf506f610fd363d4cf24dfb4`.

Logical contents: region `4398046511105` (table `1024`, region number `1`), engine `mito`,
3 rows, a 1024-byte memtable, one 2048-byte SST, manifest version 5, flushed entry id 7.

Run the regression:

```shell
cargo nextest run -p common-meta -E 'test(test_has_pending_tails_wire)'
```

The test checks that both fixtures decode without pending tails and encode back to exactly the
same bytes, so a statistic without pending tails keeps the encoding of a release without the
field, and that a true flag travels from the heartbeat extension to the stored statistic.

## Reproduce the fixtures

At the commit above, run this in a test of `common_meta::datanode`:

```rust
let statistic = RegionStatistic {
    num_rows: 3,
    memtable_size: 1024,
    sst_size: 2048,
    sst_num: 1,
    manifest: store_api::region_engine::RegionManifestInfo::mito(5, 7, 0),
    ..Default::default()
};
let statistic_json = serde_json::to_vec(&statistic).unwrap();
let mut region = api::v1::meta::RegionStat {
    region_id: RegionId::new(1024, 1).as_u64(),
    engine: "mito".to_string(),
    ..Default::default()
};
region
    .extensions
    .insert(REGION_STATISTIC_KEY.to_string(), statistic_json.clone());
std::fs::write("legacy_region_statistic.json", statistic_json).unwrap();
let stat = RegionStat::from(&region);
std::fs::write("legacy_region_stat.json", serde_json::to_vec(&stat).unwrap()).unwrap();
```
