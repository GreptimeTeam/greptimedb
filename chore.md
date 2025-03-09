# first create table
```bash
mysql --host=127.0.0.1 --port=19195 --database=public;
```

```sql
CREATE DATABASE IF NOT EXISTS `cluster1`;
USE `cluster1`;
CREATE TABLE IF NOT EXISTS `app1` (
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

select count(*) from app1;

SELECT * FROM app1 ORDER BY greptime_timestamp ASC LIMIT 10\G
```

# then ingest
```bash
RUST_LOG="debug" cargo run --bin=ingester -- --input-dir="/home/discord9/greptimedb/" --parquet-dir="parquet_store/" --cfg="ingester.toml" --db-http-addr="http://127.0.0.1:4000/v1/sst/ingest_json"
```