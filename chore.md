# first create table
```bash
mysql --host=127.0.0.1 --port=19195 --database=public;
```

```sql
CREATE TABLE IF NOT EXISTS `public`.`logsbench` (
  `greptime_timestamp` TimestampNanosecond NOT NULL TIME INDEX,
  `app` STRING NULL INVERTED INDEX,
  `cluster` STRING NULL INVERTED INDEX,
  `message` STRING NULL,
  `region` STRING NULL,
  `cloud-provider` STRING NULL,
  `environment` STRING NULL,
  `product` STRING NULL,
  `sub-product` STRING NULL,
  `service` STRING NULL
) WITH (
  append_mode = 'true',
  'compaction.type' = 'twcs',                        
  'compaction.twcs.max_output_file_size' = '500MB',  
  'compaction.twcs.max_active_window_files' = '16',   
  'compaction.twcs.max_active_window_runs' = '4',    
  'compaction.twcs.max_inactive_window_files' = '4', 
  'compaction.twcs.max_inactive_window_runs' = '2',  
);
```

# then ingest
```bash
cargo run --bin=ingester -- --input-dir="/home/discord9/greptimedb/parquet_store" --parquet-dir="." --cfg="ingester.toml" --db-http-addr="http://127.0.0.1:4000"
```