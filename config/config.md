# Configurations

- [Configurations](#configurations)
  - [Standalone Mode](#standalone-mode)
  - [Distributed Mode](#distributed-mode)
    - [Frontend](#frontend)
    - [Metasrv](#metasrv)
    - [Datanode](#datanode)
    - [Flownode](#flownode)

## Standalone Mode

| Key | Type | Default | Descriptions |
| --- | -----| ------- | ----------- |
| `mode` | String | `standalone` | The running mode of the datanode. It can be `standalone` or `distributed`. |
| `enable_telemetry` | Bool | `true` | Enable telemetry to collect anonymous usage data. |
| `default_timezone` | String | `None` | The default timezone of the server. |
| `runtime` | -- | -- | The runtime options. |
| `runtime.global_rt_size` | Integer | `8` | The number of threads to execute the runtime for global read operations. |
| `runtime.compact_rt_size` | Integer | `4` | The number of threads to execute the runtime for global write operations. |
| `http` | -- | -- | The HTTP server options. |
| `http.addr` | String | `127.0.0.1:4000` | The address to bind the HTTP server. |
| `http.timeout` | String | `30s` | HTTP request timeout. Set to 0 to disable timeout. |
| `http.body_limit` | String | `64MB` | HTTP request body limit.<br/>The following units are supported: `B`, `KB`, `KiB`, `MB`, `MiB`, `GB`, `GiB`, `TB`, `TiB`, `PB`, `PiB`.<br/>Set to 0 to disable limit. |
| `grpc` | -- | -- | The gRPC server options. |
| `grpc.addr` | String | `127.0.0.1:4001` | The address to bind the gRPC server. |
| `grpc.runtime_size` | Integer | `8` | The number of server worker threads. |
| `grpc.tls` | -- | -- | gRPC server TLS options, see `mysql.tls` section. |
| `grpc.tls.mode` | String | `disable` | TLS mode. |
| `grpc.tls.cert_path` | String | `None` | Certificate file path. |
| `grpc.tls.key_path` | String | `None` | Private key file path. |
| `grpc.tls.watch` | Bool | `false` | Watch for Certificate and key file change and auto reload.<br/>For now, gRPC tls config does not support auto reload. |
| `mysql` | -- | -- | MySQL server options. |
| `mysql.enable` | Bool | `true` | Whether to enable. |
| `mysql.addr` | String | `127.0.0.1:4002` | The addr to bind the MySQL server. |
| `mysql.runtime_size` | Integer | `2` | The number of server worker threads. |
| `mysql.tls` | -- | -- | -- |
| `mysql.tls.mode` | String | `disable` | TLS mode, refer to https://www.postgresql.org/docs/current/libpq-ssl.html<br/>- `disable` (default value)<br/>- `prefer`<br/>- `require`<br/>- `verify-ca`<br/>- `verify-full` |
| `mysql.tls.cert_path` | String | `None` | Certificate file path. |
| `mysql.tls.key_path` | String | `None` | Private key file path. |
| `mysql.tls.watch` | Bool | `false` | Watch for Certificate and key file change and auto reload |
| `postgres` | -- | -- | PostgresSQL server options. |
| `postgres.enable` | Bool | `true` | Whether to enable |
| `postgres.addr` | String | `127.0.0.1:4003` | The addr to bind the PostgresSQL server. |
| `postgres.runtime_size` | Integer | `2` | The number of server worker threads. |
| `postgres.tls` | -- | -- | PostgresSQL server TLS options, see `mysql.tls` section. |
| `postgres.tls.mode` | String | `disable` | TLS mode. |
| `postgres.tls.cert_path` | String | `None` | Certificate file path. |
| `postgres.tls.key_path` | String | `None` | Private key file path. |
| `postgres.tls.watch` | Bool | `false` | Watch for Certificate and key file change and auto reload |
| `opentsdb` | -- | -- | OpenTSDB protocol options. |
| `opentsdb.enable` | Bool | `true` | Whether to enable OpenTSDB put in HTTP API. |
| `influxdb` | -- | -- | InfluxDB protocol options. |
| `influxdb.enable` | Bool | `true` | Whether to enable InfluxDB protocol in HTTP API. |
| `prom_store` | -- | -- | Prometheus remote storage options |
| `prom_store.enable` | Bool | `true` | Whether to enable Prometheus remote write and read in HTTP API. |
| `prom_store.with_metric_engine` | Bool | `true` | Whether to store the data from Prometheus remote write in metric engine. |
| `wal` | -- | -- | The WAL options. |
| `wal.provider` | String | `raft_engine` | The provider of the WAL.<br/>- `raft_engine`: the wal is stored in the local file system by raft-engine.<br/>- `kafka`: it's remote wal that data is stored in Kafka. |
| `wal.dir` | String | `None` | The directory to store the WAL files.<br/>**It's only used when the provider is `raft_engine`**. |
| `wal.file_size` | String | `256MB` | The size of the WAL segment file.<br/>**It's only used when the provider is `raft_engine`**. |
| `wal.purge_threshold` | String | `4GB` | The threshold of the WAL size to trigger a flush.<br/>**It's only used when the provider is `raft_engine`**. |
| `wal.purge_interval` | String | `10m` | The interval to trigger a flush.<br/>**It's only used when the provider is `raft_engine`**. |
| `wal.read_batch_size` | Integer | `128` | The read batch size.<br/>**It's only used when the provider is `raft_engine`**. |
| `wal.sync_write` | Bool | `false` | Whether to use sync write.<br/>**It's only used when the provider is `raft_engine`**. |
| `wal.enable_log_recycle` | Bool | `true` | Whether to reuse logically truncated log files.<br/>**It's only used when the provider is `raft_engine`**. |
| `wal.prefill_log_files` | Bool | `false` | Whether to pre-create log files on start up.<br/>**It's only used when the provider is `raft_engine`**. |
| `wal.sync_period` | String | `10s` | Duration for fsyncing log files.<br/>**It's only used when the provider is `raft_engine`**. |
| `wal.broker_endpoints` | Array | -- | The Kafka broker endpoints.<br/>**It's only used when the provider is `kafka`**. |
| `wal.max_batch_bytes` | String | `1MB` | The max size of a single producer batch.<br/>Warning: Kafka has a default limit of 1MB per message in a topic.<br/>**It's only used when the provider is `kafka`**. |
| `wal.consumer_wait_timeout` | String | `100ms` | The consumer wait timeout.<br/>**It's only used when the provider is `kafka`**. |
| `wal.backoff_init` | String | `500ms` | The initial backoff delay.<br/>**It's only used when the provider is `kafka`**. |
| `wal.backoff_max` | String | `10s` | The maximum backoff delay.<br/>**It's only used when the provider is `kafka`**. |
| `wal.backoff_base` | Integer | `2` | The exponential backoff rate, i.e. next backoff = base * current backoff.<br/>**It's only used when the provider is `kafka`**. |
| `wal.backoff_deadline` | String | `5mins` | The deadline of retries.<br/>**It's only used when the provider is `kafka`**. |
| `metadata_store` | -- | -- | Metadata storage options. |
| `metadata_store.file_size` | String | `256MB` | Kv file size in bytes. |
| `metadata_store.purge_threshold` | String | `4GB` | Kv purge threshold. |
| `procedure` | -- | -- | Procedure storage options. |
| `procedure.max_retry_times` | Integer | `3` | Procedure max retry time. |
| `procedure.retry_delay` | String | `500ms` | Initial retry delay of procedures, increases exponentially |
| `storage` | -- | -- | The data storage options. |
| `storage.data_home` | String | `/tmp/greptimedb/` | The working home directory. |
| `storage.type` | String | `File` | The storage type used to store the data.<br/>- `File`: the data is stored in the local file system.<br/>- `S3`: the data is stored in the S3 object storage.<br/>- `Gcs`: the data is stored in the Google Cloud Storage.<br/>- `Azblob`: the data is stored in the Azure Blob Storage.<br/>- `Oss`: the data is stored in the Aliyun OSS. |
| `storage.cache_path` | String | `None` | Cache configuration for object storage such as 'S3' etc.<br/>The local file cache directory. |
| `storage.cache_capacity` | String | `None` | The local file cache capacity in bytes. |
| `storage.bucket` | String | `None` | The S3 bucket name.<br/>**It's only used when the storage type is `S3`, `Oss` and `Gcs`**. |
| `storage.root` | String | `None` | The S3 data will be stored in the specified prefix, for example, `s3://${bucket}/${root}`.<br/>**It's only used when the storage type is `S3`, `Oss` and `Azblob`**. |
| `storage.access_key_id` | String | `None` | The access key id of the aws account.<br/>It's **highly recommended** to use AWS IAM roles instead of hardcoding the access key id and secret key.<br/>**It's only used when the storage type is `S3` and `Oss`**. |
| `storage.secret_access_key` | String | `None` | The secret access key of the aws account.<br/>It's **highly recommended** to use AWS IAM roles instead of hardcoding the access key id and secret key.<br/>**It's only used when the storage type is `S3`**. |
| `storage.access_key_secret` | String | `None` | The secret access key of the aliyun account.<br/>**It's only used when the storage type is `Oss`**. |
| `storage.account_name` | String | `None` | The account key of the azure account.<br/>**It's only used when the storage type is `Azblob`**. |
| `storage.account_key` | String | `None` | The account key of the azure account.<br/>**It's only used when the storage type is `Azblob`**. |
| `storage.scope` | String | `None` | The scope of the google cloud storage.<br/>**It's only used when the storage type is `Gcs`**. |
| `storage.credential_path` | String | `None` | The credential path of the google cloud storage.<br/>**It's only used when the storage type is `Gcs`**. |
| `storage.container` | String | `None` | The container of the azure account.<br/>**It's only used when the storage type is `Azblob`**. |
| `storage.sas_token` | String | `None` | The sas token of the azure account.<br/>**It's only used when the storage type is `Azblob`**. |
| `storage.endpoint` | String | `None` | The endpoint of the S3 service.<br/>**It's only used when the storage type is `S3`, `Oss`, `Gcs` and `Azblob`**. |
| `storage.region` | String | `None` | The region of the S3 service.<br/>**It's only used when the storage type is `S3`, `Oss`, `Gcs` and `Azblob`**. |
| `[[region_engine]]` | -- | -- | The region engine options. You can configure multiple region engines. |
| `region_engine.mito` | -- | -- | The Mito engine options. |
| `region_engine.mito.num_workers` | Integer | `8` | Number of region workers. |
| `region_engine.mito.worker_channel_size` | Integer | `128` | Request channel size of each worker. |
| `region_engine.mito.worker_request_batch_size` | Integer | `64` | Max batch size for a worker to handle requests. |
| `region_engine.mito.manifest_checkpoint_distance` | Integer | `10` | Number of meta action updated to trigger a new checkpoint for the manifest. |
| `region_engine.mito.compress_manifest` | Bool | `false` | Whether to compress manifest and checkpoint file by gzip (default false). |
| `region_engine.mito.max_background_jobs` | Integer | `4` | Max number of running background jobs |
| `region_engine.mito.auto_flush_interval` | String | `1h` | Interval to auto flush a region if it has not flushed yet. |
| `region_engine.mito.global_write_buffer_size` | String | `1GB` | Global write buffer size for all regions. If not set, it's default to 1/8 of OS memory with a max limitation of 1GB. |
| `region_engine.mito.global_write_buffer_reject_size` | String | `2GB` | Global write buffer size threshold to reject write requests. If not set, it's default to 2 times of `global_write_buffer_size` |
| `region_engine.mito.sst_meta_cache_size` | String | `128MB` | Cache size for SST metadata. Setting it to 0 to disable the cache.<br/>If not set, it's default to 1/32 of OS memory with a max limitation of 128MB. |
| `region_engine.mito.vector_cache_size` | String | `512MB` | Cache size for vectors and arrow arrays. Setting it to 0 to disable the cache.<br/>If not set, it's default to 1/16 of OS memory with a max limitation of 512MB. |
| `region_engine.mito.page_cache_size` | String | `512MB` | Cache size for pages of SST row groups. Setting it to 0 to disable the cache.<br/>If not set, it's default to 1/16 of OS memory with a max limitation of 512MB. |
| `region_engine.mito.enable_experimental_write_cache` | Bool | `false` | Whether to enable the experimental write cache. |
| `region_engine.mito.experimental_write_cache_path` | String | `""` | File system path for write cache, defaults to `{data_home}/write_cache`. |
| `region_engine.mito.experimental_write_cache_size` | String | `512MB` | Capacity for write cache. |
| `region_engine.mito.experimental_write_cache_ttl` | String | `1h` | TTL for write cache. |
| `region_engine.mito.sst_write_buffer_size` | String | `8MB` | Buffer size for SST writing. |
| `region_engine.mito.scan_parallelism` | Integer | `0` | Parallelism to scan a region (default: 1/4 of cpu cores).<br/>- `0`: using the default value (1/4 of cpu cores).<br/>- `1`: scan in current thread.<br/>- `n`: scan in parallelism n. |
| `region_engine.mito.parallel_scan_channel_size` | Integer | `32` | Capacity of the channel to send data from parallel scan tasks to the main task. |
| `region_engine.mito.allow_stale_entries` | Bool | `false` | Whether to allow stale WAL entries read during replay. |
| `region_engine.mito.index` | -- | -- | The options for index in Mito engine. |
| `region_engine.mito.index.aux_path` | String | `""` | Auxiliary directory path for the index in filesystem, used to store intermediate files for<br/>creating the index and staging files for searching the index, defaults to `{data_home}/index_intermediate`.<br/>The default name for this directory is `index_intermediate` for backward compatibility.<br/><br/>This path contains two subdirectories:<br/>- `__intm`: for storing intermediate files used during creating index.<br/>- `staging`: for storing staging files used during searching index. |
| `region_engine.mito.index.staging_size` | String | `2GB` | The max capacity of the staging directory. |
| `region_engine.mito.inverted_index` | -- | -- | The options for inverted index in Mito engine. |
| `region_engine.mito.inverted_index.create_on_flush` | String | `auto` | Whether to create the index on flush.<br/>- `auto`: automatically (default)<br/>- `disable`: never |
| `region_engine.mito.inverted_index.create_on_compaction` | String | `auto` | Whether to create the index on compaction.<br/>- `auto`: automatically (default)<br/>- `disable`: never |
| `region_engine.mito.inverted_index.apply_on_query` | String | `auto` | Whether to apply the index on query<br/>- `auto`: automatically (default)<br/>- `disable`: never |
| `region_engine.mito.inverted_index.mem_threshold_on_create` | String | `auto` | Memory threshold for performing an external sort during index creation.<br/>- `auto`: automatically determine the threshold based on the system memory size (default)<br/>- `unlimited`: no memory limit<br/>- `[size]` e.g. `64MB`: fixed memory threshold |
| `region_engine.mito.inverted_index.intermediate_path` | String | `""` | Deprecated, use `region_engine.mito.index.aux_path` instead. |
| `region_engine.mito.inverted_index.metadata_cache_size` | String | `64MiB` | Cache size for inverted index metadata. |
| `region_engine.mito.inverted_index.content_cache_size` | String | `128MiB` | Cache size for inverted index content. |
| `region_engine.mito.fulltext_index` | -- | -- | The options for full-text index in Mito engine. |
| `region_engine.mito.fulltext_index.create_on_flush` | String | `auto` | Whether to create the index on flush.<br/>- `auto`: automatically (default)<br/>- `disable`: never |
| `region_engine.mito.fulltext_index.create_on_compaction` | String | `auto` | Whether to create the index on compaction.<br/>- `auto`: automatically (default)<br/>- `disable`: never |
| `region_engine.mito.fulltext_index.apply_on_query` | String | `auto` | Whether to apply the index on query<br/>- `auto`: automatically (default)<br/>- `disable`: never |
| `region_engine.mito.fulltext_index.mem_threshold_on_create` | String | `auto` | Memory threshold for index creation.<br/>- `auto`: automatically determine the threshold based on the system memory size (default)<br/>- `unlimited`: no memory limit<br/>- `[size]` e.g. `64MB`: fixed memory threshold |
| `region_engine.mito.memtable` | -- | -- | -- |
| `region_engine.mito.memtable.type` | String | `time_series` | Memtable type.<br/>- `time_series`: time-series memtable<br/>- `partition_tree`: partition tree memtable (experimental) |
| `region_engine.mito.memtable.index_max_keys_per_shard` | Integer | `8192` | The max number of keys in one shard.<br/>Only available for `partition_tree` memtable. |
| `region_engine.mito.memtable.data_freeze_threshold` | Integer | `32768` | The max rows of data inside the actively writing buffer in one shard.<br/>Only available for `partition_tree` memtable. |
| `region_engine.mito.memtable.fork_dictionary_bytes` | String | `1GiB` | Max dictionary bytes.<br/>Only available for `partition_tree` memtable. |
| `region_engine.file` | -- | -- | Enable the file engine. |
| `logging` | -- | -- | The logging options. |
| `logging.dir` | String | `/tmp/greptimedb/logs` | The directory to store the log files. |
| `logging.level` | String | `None` | The log level. Can be `info`/`debug`/`warn`/`error`. |
| `logging.enable_otlp_tracing` | Bool | `false` | Enable OTLP tracing. |
| `logging.otlp_endpoint` | String | `None` | The OTLP tracing endpoint. |
| `logging.append_stdout` | Bool | `true` | Whether to append logs to stdout. |
| `logging.tracing_sample_ratio` | -- | -- | The percentage of tracing will be sampled and exported.<br/>Valid range `[0, 1]`, 1 means all traces are sampled, 0 means all traces are not sampled, the default value is 1.<br/>ratio > 1 are treated as 1. Fractions < 0 are treated as 0 |
| `logging.tracing_sample_ratio.default_ratio` | Float | `1.0` | -- |
| `export_metrics` | -- | -- | The datanode can export its metrics and send to Prometheus compatible service (e.g. send to `greptimedb` itself) from remote-write API.<br/>This is only used for `greptimedb` to export its own metrics internally. It's different from prometheus scrape. |
| `export_metrics.enable` | Bool | `false` | whether enable export metrics. |
| `export_metrics.write_interval` | String | `30s` | The interval of export metrics. |
| `export_metrics.self_import` | -- | -- | For `standalone` mode, `self_import` is recommend to collect metrics generated by itself |
| `export_metrics.self_import.db` | String | `None` | -- |
| `export_metrics.remote_write` | -- | -- | -- |
| `export_metrics.remote_write.url` | String | `""` | The url the metrics send to. The url example can be: `http://127.0.0.1:4000/v1/prometheus/write?db=information_schema`. |
| `export_metrics.remote_write.headers` | InlineTable | -- | HTTP headers of Prometheus remote-write carry. |
| `tracing` | -- | -- | The tracing options. Only effect when compiled with `tokio-console` feature. |
| `tracing.tokio_console_addr` | String | `None` | The tokio console address. |


## Distributed Mode

### Frontend

| Key | Type | Default | Descriptions |
| --- | -----| ------- | ----------- |
| `default_timezone` | String | `None` | The default timezone of the server. |
| `runtime` | -- | -- | The runtime options. |
| `runtime.global_rt_size` | Integer | `8` | The number of threads to execute the runtime for global read operations. |
| `runtime.compact_rt_size` | Integer | `4` | The number of threads to execute the runtime for global write operations. |
| `heartbeat` | -- | -- | The heartbeat options. |
| `heartbeat.interval` | String | `18s` | Interval for sending heartbeat messages to the metasrv. |
| `heartbeat.retry_interval` | String | `3s` | Interval for retrying to send heartbeat messages to the metasrv. |
| `http` | -- | -- | The HTTP server options. |
| `http.addr` | String | `127.0.0.1:4000` | The address to bind the HTTP server. |
| `http.timeout` | String | `30s` | HTTP request timeout. Set to 0 to disable timeout. |
| `http.body_limit` | String | `64MB` | HTTP request body limit.<br/>The following units are supported: `B`, `KB`, `KiB`, `MB`, `MiB`, `GB`, `GiB`, `TB`, `TiB`, `PB`, `PiB`.<br/>Set to 0 to disable limit. |
| `grpc` | -- | -- | The gRPC server options. |
| `grpc.addr` | String | `127.0.0.1:4001` | The address to bind the gRPC server. |
| `grpc.hostname` | String | `127.0.0.1` | The hostname advertised to the metasrv,<br/>and used for connections from outside the host |
| `grpc.runtime_size` | Integer | `8` | The number of server worker threads. |
| `grpc.tls` | -- | -- | gRPC server TLS options, see `mysql.tls` section. |
| `grpc.tls.mode` | String | `disable` | TLS mode. |
| `grpc.tls.cert_path` | String | `None` | Certificate file path. |
| `grpc.tls.key_path` | String | `None` | Private key file path. |
| `grpc.tls.watch` | Bool | `false` | Watch for Certificate and key file change and auto reload.<br/>For now, gRPC tls config does not support auto reload. |
| `mysql` | -- | -- | MySQL server options. |
| `mysql.enable` | Bool | `true` | Whether to enable. |
| `mysql.addr` | String | `127.0.0.1:4002` | The addr to bind the MySQL server. |
| `mysql.runtime_size` | Integer | `2` | The number of server worker threads. |
| `mysql.tls` | -- | -- | -- |
| `mysql.tls.mode` | String | `disable` | TLS mode, refer to https://www.postgresql.org/docs/current/libpq-ssl.html<br/>- `disable` (default value)<br/>- `prefer`<br/>- `require`<br/>- `verify-ca`<br/>- `verify-full` |
| `mysql.tls.cert_path` | String | `None` | Certificate file path. |
| `mysql.tls.key_path` | String | `None` | Private key file path. |
| `mysql.tls.watch` | Bool | `false` | Watch for Certificate and key file change and auto reload |
| `postgres` | -- | -- | PostgresSQL server options. |
| `postgres.enable` | Bool | `true` | Whether to enable |
| `postgres.addr` | String | `127.0.0.1:4003` | The addr to bind the PostgresSQL server. |
| `postgres.runtime_size` | Integer | `2` | The number of server worker threads. |
| `postgres.tls` | -- | -- | PostgresSQL server TLS options, see `mysql.tls` section. |
| `postgres.tls.mode` | String | `disable` | TLS mode. |
| `postgres.tls.cert_path` | String | `None` | Certificate file path. |
| `postgres.tls.key_path` | String | `None` | Private key file path. |
| `postgres.tls.watch` | Bool | `false` | Watch for Certificate and key file change and auto reload |
| `opentsdb` | -- | -- | OpenTSDB protocol options. |
| `opentsdb.enable` | Bool | `true` | Whether to enable OpenTSDB put in HTTP API. |
| `influxdb` | -- | -- | InfluxDB protocol options. |
| `influxdb.enable` | Bool | `true` | Whether to enable InfluxDB protocol in HTTP API. |
| `prom_store` | -- | -- | Prometheus remote storage options |
| `prom_store.enable` | Bool | `true` | Whether to enable Prometheus remote write and read in HTTP API. |
| `prom_store.with_metric_engine` | Bool | `true` | Whether to store the data from Prometheus remote write in metric engine. |
| `meta_client` | -- | -- | The metasrv client options. |
| `meta_client.metasrv_addrs` | Array | -- | The addresses of the metasrv. |
| `meta_client.timeout` | String | `3s` | Operation timeout. |
| `meta_client.heartbeat_timeout` | String | `500ms` | Heartbeat timeout. |
| `meta_client.ddl_timeout` | String | `10s` | DDL timeout. |
| `meta_client.connect_timeout` | String | `1s` | Connect server timeout. |
| `meta_client.tcp_nodelay` | Bool | `true` | `TCP_NODELAY` option for accepted connections. |
| `meta_client.metadata_cache_max_capacity` | Integer | `100000` | The configuration about the cache of the metadata. |
| `meta_client.metadata_cache_ttl` | String | `10m` | TTL of the metadata cache. |
| `meta_client.metadata_cache_tti` | String | `5m` | -- |
| `datanode` | -- | -- | Datanode options. |
| `datanode.client` | -- | -- | Datanode client options. |
| `datanode.client.connect_timeout` | String | `10s` | -- |
| `datanode.client.tcp_nodelay` | Bool | `true` | -- |
| `logging` | -- | -- | The logging options. |
| `logging.dir` | String | `/tmp/greptimedb/logs` | The directory to store the log files. |
| `logging.level` | String | `None` | The log level. Can be `info`/`debug`/`warn`/`error`. |
| `logging.enable_otlp_tracing` | Bool | `false` | Enable OTLP tracing. |
| `logging.otlp_endpoint` | String | `None` | The OTLP tracing endpoint. |
| `logging.append_stdout` | Bool | `true` | Whether to append logs to stdout. |
| `logging.tracing_sample_ratio` | -- | -- | The percentage of tracing will be sampled and exported.<br/>Valid range `[0, 1]`, 1 means all traces are sampled, 0 means all traces are not sampled, the default value is 1.<br/>ratio > 1 are treated as 1. Fractions < 0 are treated as 0 |
| `logging.tracing_sample_ratio.default_ratio` | Float | `1.0` | -- |
| `export_metrics` | -- | -- | The datanode can export its metrics and send to Prometheus compatible service (e.g. send to `greptimedb` itself) from remote-write API.<br/>This is only used for `greptimedb` to export its own metrics internally. It's different from prometheus scrape. |
| `export_metrics.enable` | Bool | `false` | whether enable export metrics. |
| `export_metrics.write_interval` | String | `30s` | The interval of export metrics. |
| `export_metrics.self_import` | -- | -- | For `standalone` mode, `self_import` is recommend to collect metrics generated by itself |
| `export_metrics.self_import.db` | String | `None` | -- |
| `export_metrics.remote_write` | -- | -- | -- |
| `export_metrics.remote_write.url` | String | `""` | The url the metrics send to. The url example can be: `http://127.0.0.1:4000/v1/prometheus/write?db=information_schema`. |
| `export_metrics.remote_write.headers` | InlineTable | -- | HTTP headers of Prometheus remote-write carry. |
| `tracing` | -- | -- | The tracing options. Only effect when compiled with `tokio-console` feature. |
| `tracing.tokio_console_addr` | String | `None` | The tokio console address. |


### Metasrv

| Key | Type | Default | Descriptions |
| --- | -----| ------- | ----------- |
| `data_home` | String | `/tmp/metasrv/` | The working home directory. |
| `bind_addr` | String | `127.0.0.1:3002` | The bind address of metasrv. |
| `server_addr` | String | `127.0.0.1:3002` | The communication server address for frontend and datanode to connect to metasrv,  "127.0.0.1:3002" by default for localhost. |
| `store_addr` | String | `127.0.0.1:2379` | Etcd server address. |
| `selector` | String | `lease_based` | Datanode selector type.<br/>- `lease_based` (default value).<br/>- `load_based`<br/>For details, please see "https://docs.greptime.com/developer-guide/metasrv/selector". |
| `use_memory_store` | Bool | `false` | Store data in memory. |
| `enable_telemetry` | Bool | `true` | Whether to enable greptimedb telemetry. |
| `store_key_prefix` | String | `""` | If it's not empty, the metasrv will store all data with this key prefix. |
| `enable_region_failover` | Bool | `false` | Whether to enable region failover.<br/>This feature is only available on GreptimeDB running on cluster mode and<br/>- Using Remote WAL<br/>- Using shared storage (e.g., s3). |
| `runtime` | -- | -- | The runtime options. |
| `runtime.global_rt_size` | Integer | `8` | The number of threads to execute the runtime for global read operations. |
| `runtime.compact_rt_size` | Integer | `4` | The number of threads to execute the runtime for global write operations. |
| `procedure` | -- | -- | Procedure storage options. |
| `procedure.max_retry_times` | Integer | `12` | Procedure max retry time. |
| `procedure.retry_delay` | String | `500ms` | Initial retry delay of procedures, increases exponentially |
| `procedure.max_metadata_value_size` | String | `1500KiB` | Auto split large value<br/>GreptimeDB procedure uses etcd as the default metadata storage backend.<br/>The etcd the maximum size of any request is 1.5 MiB<br/>1500KiB = 1536KiB (1.5MiB) - 36KiB (reserved size of key)<br/>Comments out the `max_metadata_value_size`, for don't split large value (no limit). |
| `failure_detector` | -- | -- | -- |
| `failure_detector.threshold` | Float | `8.0` | The threshold value used by the failure detector to determine failure conditions. |
| `failure_detector.min_std_deviation` | String | `100ms` | The minimum standard deviation of the heartbeat intervals, used to calculate acceptable variations. |
| `failure_detector.acceptable_heartbeat_pause` | String | `10000ms` | The acceptable pause duration between heartbeats, used to determine if a heartbeat interval is acceptable. |
| `failure_detector.first_heartbeat_estimate` | String | `1000ms` | The initial estimate of the heartbeat interval used by the failure detector. |
| `datanode` | -- | -- | Datanode options. |
| `datanode.client` | -- | -- | Datanode client options. |
| `datanode.client.timeout` | String | `10s` | Operation timeout. |
| `datanode.client.connect_timeout` | String | `10s` | Connect server timeout. |
| `datanode.client.tcp_nodelay` | Bool | `true` | `TCP_NODELAY` option for accepted connections. |
| `wal` | -- | -- | -- |
| `wal.provider` | String | `raft_engine` | -- |
| `wal.broker_endpoints` | Array | -- | The broker endpoints of the Kafka cluster. |
| `wal.num_topics` | Integer | `64` | Number of topics to be created upon start. |
| `wal.selector_type` | String | `round_robin` | Topic selector type.<br/>Available selector types:<br/>- `round_robin` (default) |
| `wal.topic_name_prefix` | String | `greptimedb_wal_topic` | A Kafka topic is constructed by concatenating `topic_name_prefix` and `topic_id`. |
| `wal.replication_factor` | Integer | `1` | Expected number of replicas of each partition. |
| `wal.create_topic_timeout` | String | `30s` | Above which a topic creation operation will be cancelled. |
| `wal.backoff_init` | String | `500ms` | The initial backoff for kafka clients. |
| `wal.backoff_max` | String | `10s` | The maximum backoff for kafka clients. |
| `wal.backoff_base` | Integer | `2` | Exponential backoff rate, i.e. next backoff = base * current backoff. |
| `wal.backoff_deadline` | String | `5mins` | Stop reconnecting if the total wait time reaches the deadline. If this config is missing, the reconnecting won't terminate. |
| `logging` | -- | -- | The logging options. |
| `logging.dir` | String | `/tmp/greptimedb/logs` | The directory to store the log files. |
| `logging.level` | String | `None` | The log level. Can be `info`/`debug`/`warn`/`error`. |
| `logging.enable_otlp_tracing` | Bool | `false` | Enable OTLP tracing. |
| `logging.otlp_endpoint` | String | `None` | The OTLP tracing endpoint. |
| `logging.append_stdout` | Bool | `true` | Whether to append logs to stdout. |
| `logging.tracing_sample_ratio` | -- | -- | The percentage of tracing will be sampled and exported.<br/>Valid range `[0, 1]`, 1 means all traces are sampled, 0 means all traces are not sampled, the default value is 1.<br/>ratio > 1 are treated as 1. Fractions < 0 are treated as 0 |
| `logging.tracing_sample_ratio.default_ratio` | Float | `1.0` | -- |
| `export_metrics` | -- | -- | The datanode can export its metrics and send to Prometheus compatible service (e.g. send to `greptimedb` itself) from remote-write API.<br/>This is only used for `greptimedb` to export its own metrics internally. It's different from prometheus scrape. |
| `export_metrics.enable` | Bool | `false` | whether enable export metrics. |
| `export_metrics.write_interval` | String | `30s` | The interval of export metrics. |
| `export_metrics.self_import` | -- | -- | For `standalone` mode, `self_import` is recommend to collect metrics generated by itself |
| `export_metrics.self_import.db` | String | `None` | -- |
| `export_metrics.remote_write` | -- | -- | -- |
| `export_metrics.remote_write.url` | String | `""` | The url the metrics send to. The url example can be: `http://127.0.0.1:4000/v1/prometheus/write?db=information_schema`. |
| `export_metrics.remote_write.headers` | InlineTable | -- | HTTP headers of Prometheus remote-write carry. |
| `tracing` | -- | -- | The tracing options. Only effect when compiled with `tokio-console` feature. |
| `tracing.tokio_console_addr` | String | `None` | The tokio console address. |


### Datanode

| Key | Type | Default | Descriptions |
| --- | -----| ------- | ----------- |
| `mode` | String | `standalone` | The running mode of the datanode. It can be `standalone` or `distributed`. |
| `node_id` | Integer | `None` | The datanode identifier and should be unique in the cluster. |
| `require_lease_before_startup` | Bool | `false` | Start services after regions have obtained leases.<br/>It will block the datanode start if it can't receive leases in the heartbeat from metasrv. |
| `init_regions_in_background` | Bool | `false` | Initialize all regions in the background during the startup.<br/>By default, it provides services after all regions have been initialized. |
| `enable_telemetry` | Bool | `true` | Enable telemetry to collect anonymous usage data. |
| `init_regions_parallelism` | Integer | `16` | Parallelism of initializing regions. |
| `rpc_addr` | String | `None` | Deprecated, use `grpc.addr` instead. |
| `rpc_hostname` | String | `None` | Deprecated, use `grpc.hostname` instead. |
| `rpc_runtime_size` | Integer | `None` | Deprecated, use `grpc.runtime_size` instead. |
| `rpc_max_recv_message_size` | String | `None` | Deprecated, use `grpc.rpc_max_recv_message_size` instead. |
| `rpc_max_send_message_size` | String | `None` | Deprecated, use `grpc.rpc_max_send_message_size` instead. |
| `grpc` | -- | -- | The gRPC server options. |
| `grpc.addr` | String | `127.0.0.1:3001` | The address to bind the gRPC server. |
| `grpc.hostname` | String | `127.0.0.1` | The hostname advertised to the metasrv,<br/>and used for connections from outside the host |
| `grpc.runtime_size` | Integer | `8` | The number of server worker threads. |
| `grpc.max_recv_message_size` | String | `512MB` | The maximum receive message size for gRPC server. |
| `grpc.max_send_message_size` | String | `512MB` | The maximum send message size for gRPC server. |
| `grpc.tls` | -- | -- | gRPC server TLS options, see `mysql.tls` section. |
| `grpc.tls.mode` | String | `disable` | TLS mode. |
| `grpc.tls.cert_path` | String | `None` | Certificate file path. |
| `grpc.tls.key_path` | String | `None` | Private key file path. |
| `grpc.tls.watch` | Bool | `false` | Watch for Certificate and key file change and auto reload.<br/>For now, gRPC tls config does not support auto reload. |
| `runtime` | -- | -- | The runtime options. |
| `runtime.global_rt_size` | Integer | `8` | The number of threads to execute the runtime for global read operations. |
| `runtime.compact_rt_size` | Integer | `4` | The number of threads to execute the runtime for global write operations. |
| `heartbeat` | -- | -- | The heartbeat options. |
| `heartbeat.interval` | String | `3s` | Interval for sending heartbeat messages to the metasrv. |
| `heartbeat.retry_interval` | String | `3s` | Interval for retrying to send heartbeat messages to the metasrv. |
| `meta_client` | -- | -- | The metasrv client options. |
| `meta_client.metasrv_addrs` | Array | -- | The addresses of the metasrv. |
| `meta_client.timeout` | String | `3s` | Operation timeout. |
| `meta_client.heartbeat_timeout` | String | `500ms` | Heartbeat timeout. |
| `meta_client.ddl_timeout` | String | `10s` | DDL timeout. |
| `meta_client.connect_timeout` | String | `1s` | Connect server timeout. |
| `meta_client.tcp_nodelay` | Bool | `true` | `TCP_NODELAY` option for accepted connections. |
| `meta_client.metadata_cache_max_capacity` | Integer | `100000` | The configuration about the cache of the metadata. |
| `meta_client.metadata_cache_ttl` | String | `10m` | TTL of the metadata cache. |
| `meta_client.metadata_cache_tti` | String | `5m` | -- |
| `wal` | -- | -- | The WAL options. |
| `wal.provider` | String | `raft_engine` | The provider of the WAL.<br/>- `raft_engine`: the wal is stored in the local file system by raft-engine.<br/>- `kafka`: it's remote wal that data is stored in Kafka. |
| `wal.dir` | String | `None` | The directory to store the WAL files.<br/>**It's only used when the provider is `raft_engine`**. |
| `wal.file_size` | String | `256MB` | The size of the WAL segment file.<br/>**It's only used when the provider is `raft_engine`**. |
| `wal.purge_threshold` | String | `4GB` | The threshold of the WAL size to trigger a flush.<br/>**It's only used when the provider is `raft_engine`**. |
| `wal.purge_interval` | String | `10m` | The interval to trigger a flush.<br/>**It's only used when the provider is `raft_engine`**. |
| `wal.read_batch_size` | Integer | `128` | The read batch size.<br/>**It's only used when the provider is `raft_engine`**. |
| `wal.sync_write` | Bool | `false` | Whether to use sync write.<br/>**It's only used when the provider is `raft_engine`**. |
| `wal.enable_log_recycle` | Bool | `true` | Whether to reuse logically truncated log files.<br/>**It's only used when the provider is `raft_engine`**. |
| `wal.prefill_log_files` | Bool | `false` | Whether to pre-create log files on start up.<br/>**It's only used when the provider is `raft_engine`**. |
| `wal.sync_period` | String | `10s` | Duration for fsyncing log files.<br/>**It's only used when the provider is `raft_engine`**. |
| `wal.broker_endpoints` | Array | -- | The Kafka broker endpoints.<br/>**It's only used when the provider is `kafka`**. |
| `wal.max_batch_bytes` | String | `1MB` | The max size of a single producer batch.<br/>Warning: Kafka has a default limit of 1MB per message in a topic.<br/>**It's only used when the provider is `kafka`**. |
| `wal.consumer_wait_timeout` | String | `100ms` | The consumer wait timeout.<br/>**It's only used when the provider is `kafka`**. |
| `wal.backoff_init` | String | `500ms` | The initial backoff delay.<br/>**It's only used when the provider is `kafka`**. |
| `wal.backoff_max` | String | `10s` | The maximum backoff delay.<br/>**It's only used when the provider is `kafka`**. |
| `wal.backoff_base` | Integer | `2` | The exponential backoff rate, i.e. next backoff = base * current backoff.<br/>**It's only used when the provider is `kafka`**. |
| `wal.backoff_deadline` | String | `5mins` | The deadline of retries.<br/>**It's only used when the provider is `kafka`**. |
| `storage` | -- | -- | The data storage options. |
| `storage.data_home` | String | `/tmp/greptimedb/` | The working home directory. |
| `storage.type` | String | `File` | The storage type used to store the data.<br/>- `File`: the data is stored in the local file system.<br/>- `S3`: the data is stored in the S3 object storage.<br/>- `Gcs`: the data is stored in the Google Cloud Storage.<br/>- `Azblob`: the data is stored in the Azure Blob Storage.<br/>- `Oss`: the data is stored in the Aliyun OSS. |
| `storage.cache_path` | String | `None` | Cache configuration for object storage such as 'S3' etc.<br/>The local file cache directory. |
| `storage.cache_capacity` | String | `None` | The local file cache capacity in bytes. |
| `storage.bucket` | String | `None` | The S3 bucket name.<br/>**It's only used when the storage type is `S3`, `Oss` and `Gcs`**. |
| `storage.root` | String | `None` | The S3 data will be stored in the specified prefix, for example, `s3://${bucket}/${root}`.<br/>**It's only used when the storage type is `S3`, `Oss` and `Azblob`**. |
| `storage.access_key_id` | String | `None` | The access key id of the aws account.<br/>It's **highly recommended** to use AWS IAM roles instead of hardcoding the access key id and secret key.<br/>**It's only used when the storage type is `S3` and `Oss`**. |
| `storage.secret_access_key` | String | `None` | The secret access key of the aws account.<br/>It's **highly recommended** to use AWS IAM roles instead of hardcoding the access key id and secret key.<br/>**It's only used when the storage type is `S3`**. |
| `storage.access_key_secret` | String | `None` | The secret access key of the aliyun account.<br/>**It's only used when the storage type is `Oss`**. |
| `storage.account_name` | String | `None` | The account key of the azure account.<br/>**It's only used when the storage type is `Azblob`**. |
| `storage.account_key` | String | `None` | The account key of the azure account.<br/>**It's only used when the storage type is `Azblob`**. |
| `storage.scope` | String | `None` | The scope of the google cloud storage.<br/>**It's only used when the storage type is `Gcs`**. |
| `storage.credential_path` | String | `None` | The credential path of the google cloud storage.<br/>**It's only used when the storage type is `Gcs`**. |
| `storage.container` | String | `None` | The container of the azure account.<br/>**It's only used when the storage type is `Azblob`**. |
| `storage.sas_token` | String | `None` | The sas token of the azure account.<br/>**It's only used when the storage type is `Azblob`**. |
| `storage.endpoint` | String | `None` | The endpoint of the S3 service.<br/>**It's only used when the storage type is `S3`, `Oss`, `Gcs` and `Azblob`**. |
| `storage.region` | String | `None` | The region of the S3 service.<br/>**It's only used when the storage type is `S3`, `Oss`, `Gcs` and `Azblob`**. |
| `[[region_engine]]` | -- | -- | The region engine options. You can configure multiple region engines. |
| `region_engine.mito` | -- | -- | The Mito engine options. |
| `region_engine.mito.num_workers` | Integer | `8` | Number of region workers. |
| `region_engine.mito.worker_channel_size` | Integer | `128` | Request channel size of each worker. |
| `region_engine.mito.worker_request_batch_size` | Integer | `64` | Max batch size for a worker to handle requests. |
| `region_engine.mito.manifest_checkpoint_distance` | Integer | `10` | Number of meta action updated to trigger a new checkpoint for the manifest. |
| `region_engine.mito.compress_manifest` | Bool | `false` | Whether to compress manifest and checkpoint file by gzip (default false). |
| `region_engine.mito.max_background_jobs` | Integer | `4` | Max number of running background jobs |
| `region_engine.mito.auto_flush_interval` | String | `1h` | Interval to auto flush a region if it has not flushed yet. |
| `region_engine.mito.global_write_buffer_size` | String | `1GB` | Global write buffer size for all regions. If not set, it's default to 1/8 of OS memory with a max limitation of 1GB. |
| `region_engine.mito.global_write_buffer_reject_size` | String | `2GB` | Global write buffer size threshold to reject write requests. If not set, it's default to 2 times of `global_write_buffer_size` |
| `region_engine.mito.sst_meta_cache_size` | String | `128MB` | Cache size for SST metadata. Setting it to 0 to disable the cache.<br/>If not set, it's default to 1/32 of OS memory with a max limitation of 128MB. |
| `region_engine.mito.vector_cache_size` | String | `512MB` | Cache size for vectors and arrow arrays. Setting it to 0 to disable the cache.<br/>If not set, it's default to 1/16 of OS memory with a max limitation of 512MB. |
| `region_engine.mito.page_cache_size` | String | `512MB` | Cache size for pages of SST row groups. Setting it to 0 to disable the cache.<br/>If not set, it's default to 1/16 of OS memory with a max limitation of 512MB. |
| `region_engine.mito.enable_experimental_write_cache` | Bool | `false` | Whether to enable the experimental write cache. |
| `region_engine.mito.experimental_write_cache_path` | String | `""` | File system path for write cache, defaults to `{data_home}/write_cache`. |
| `region_engine.mito.experimental_write_cache_size` | String | `512MB` | Capacity for write cache. |
| `region_engine.mito.experimental_write_cache_ttl` | String | `1h` | TTL for write cache. |
| `region_engine.mito.sst_write_buffer_size` | String | `8MB` | Buffer size for SST writing. |
| `region_engine.mito.scan_parallelism` | Integer | `0` | Parallelism to scan a region (default: 1/4 of cpu cores).<br/>- `0`: using the default value (1/4 of cpu cores).<br/>- `1`: scan in current thread.<br/>- `n`: scan in parallelism n. |
| `region_engine.mito.parallel_scan_channel_size` | Integer | `32` | Capacity of the channel to send data from parallel scan tasks to the main task. |
| `region_engine.mito.allow_stale_entries` | Bool | `false` | Whether to allow stale WAL entries read during replay. |
| `region_engine.mito.index` | -- | -- | The options for index in Mito engine. |
| `region_engine.mito.index.aux_path` | String | `""` | Auxiliary directory path for the index in filesystem, used to store intermediate files for<br/>creating the index and staging files for searching the index, defaults to `{data_home}/index_intermediate`.<br/>The default name for this directory is `index_intermediate` for backward compatibility.<br/><br/>This path contains two subdirectories:<br/>- `__intm`: for storing intermediate files used during creating index.<br/>- `staging`: for storing staging files used during searching index. |
| `region_engine.mito.index.staging_size` | String | `2GB` | The max capacity of the staging directory. |
| `region_engine.mito.inverted_index` | -- | -- | The options for inverted index in Mito engine. |
| `region_engine.mito.inverted_index.create_on_flush` | String | `auto` | Whether to create the index on flush.<br/>- `auto`: automatically (default)<br/>- `disable`: never |
| `region_engine.mito.inverted_index.create_on_compaction` | String | `auto` | Whether to create the index on compaction.<br/>- `auto`: automatically (default)<br/>- `disable`: never |
| `region_engine.mito.inverted_index.apply_on_query` | String | `auto` | Whether to apply the index on query<br/>- `auto`: automatically (default)<br/>- `disable`: never |
| `region_engine.mito.inverted_index.mem_threshold_on_create` | String | `auto` | Memory threshold for performing an external sort during index creation.<br/>- `auto`: automatically determine the threshold based on the system memory size (default)<br/>- `unlimited`: no memory limit<br/>- `[size]` e.g. `64MB`: fixed memory threshold |
| `region_engine.mito.inverted_index.intermediate_path` | String | `""` | Deprecated, use `region_engine.mito.index.aux_path` instead. |
| `region_engine.mito.fulltext_index` | -- | -- | The options for full-text index in Mito engine. |
| `region_engine.mito.fulltext_index.create_on_flush` | String | `auto` | Whether to create the index on flush.<br/>- `auto`: automatically (default)<br/>- `disable`: never |
| `region_engine.mito.fulltext_index.create_on_compaction` | String | `auto` | Whether to create the index on compaction.<br/>- `auto`: automatically (default)<br/>- `disable`: never |
| `region_engine.mito.fulltext_index.apply_on_query` | String | `auto` | Whether to apply the index on query<br/>- `auto`: automatically (default)<br/>- `disable`: never |
| `region_engine.mito.fulltext_index.mem_threshold_on_create` | String | `auto` | Memory threshold for index creation.<br/>- `auto`: automatically determine the threshold based on the system memory size (default)<br/>- `unlimited`: no memory limit<br/>- `[size]` e.g. `64MB`: fixed memory threshold |
| `region_engine.mito.memtable` | -- | -- | -- |
| `region_engine.mito.memtable.type` | String | `time_series` | Memtable type.<br/>- `time_series`: time-series memtable<br/>- `partition_tree`: partition tree memtable (experimental) |
| `region_engine.mito.memtable.index_max_keys_per_shard` | Integer | `8192` | The max number of keys in one shard.<br/>Only available for `partition_tree` memtable. |
| `region_engine.mito.memtable.data_freeze_threshold` | Integer | `32768` | The max rows of data inside the actively writing buffer in one shard.<br/>Only available for `partition_tree` memtable. |
| `region_engine.mito.memtable.fork_dictionary_bytes` | String | `1GiB` | Max dictionary bytes.<br/>Only available for `partition_tree` memtable. |
| `region_engine.file` | -- | -- | Enable the file engine. |
| `logging` | -- | -- | The logging options. |
| `logging.dir` | String | `/tmp/greptimedb/logs` | The directory to store the log files. |
| `logging.level` | String | `None` | The log level. Can be `info`/`debug`/`warn`/`error`. |
| `logging.enable_otlp_tracing` | Bool | `false` | Enable OTLP tracing. |
| `logging.otlp_endpoint` | String | `None` | The OTLP tracing endpoint. |
| `logging.append_stdout` | Bool | `true` | Whether to append logs to stdout. |
| `logging.tracing_sample_ratio` | -- | -- | The percentage of tracing will be sampled and exported.<br/>Valid range `[0, 1]`, 1 means all traces are sampled, 0 means all traces are not sampled, the default value is 1.<br/>ratio > 1 are treated as 1. Fractions < 0 are treated as 0 |
| `logging.tracing_sample_ratio.default_ratio` | Float | `1.0` | -- |
| `export_metrics` | -- | -- | The datanode can export its metrics and send to Prometheus compatible service (e.g. send to `greptimedb` itself) from remote-write API.<br/>This is only used for `greptimedb` to export its own metrics internally. It's different from prometheus scrape. |
| `export_metrics.enable` | Bool | `false` | whether enable export metrics. |
| `export_metrics.write_interval` | String | `30s` | The interval of export metrics. |
| `export_metrics.self_import` | -- | -- | For `standalone` mode, `self_import` is recommend to collect metrics generated by itself |
| `export_metrics.self_import.db` | String | `None` | -- |
| `export_metrics.remote_write` | -- | -- | -- |
| `export_metrics.remote_write.url` | String | `""` | The url the metrics send to. The url example can be: `http://127.0.0.1:4000/v1/prometheus/write?db=information_schema`. |
| `export_metrics.remote_write.headers` | InlineTable | -- | HTTP headers of Prometheus remote-write carry. |
| `tracing` | -- | -- | The tracing options. Only effect when compiled with `tokio-console` feature. |
| `tracing.tokio_console_addr` | String | `None` | The tokio console address. |


### Flownode

| Key | Type | Default | Descriptions |
| --- | -----| ------- | ----------- |
| `mode` | String | `distributed` | The running mode of the flownode. It can be `standalone` or `distributed`. |
| `node_id` | Integer | `None` | The flownode identifier and should be unique in the cluster. |
| `grpc` | -- | -- | The gRPC server options. |
| `grpc.addr` | String | `127.0.0.1:6800` | The address to bind the gRPC server. |
| `grpc.hostname` | String | `127.0.0.1` | The hostname advertised to the metasrv,<br/>and used for connections from outside the host |
| `grpc.runtime_size` | Integer | `2` | The number of server worker threads. |
| `grpc.max_recv_message_size` | String | `512MB` | The maximum receive message size for gRPC server. |
| `grpc.max_send_message_size` | String | `512MB` | The maximum send message size for gRPC server. |
| `meta_client` | -- | -- | The metasrv client options. |
| `meta_client.metasrv_addrs` | Array | -- | The addresses of the metasrv. |
| `meta_client.timeout` | String | `3s` | Operation timeout. |
| `meta_client.heartbeat_timeout` | String | `500ms` | Heartbeat timeout. |
| `meta_client.ddl_timeout` | String | `10s` | DDL timeout. |
| `meta_client.connect_timeout` | String | `1s` | Connect server timeout. |
| `meta_client.tcp_nodelay` | Bool | `true` | `TCP_NODELAY` option for accepted connections. |
| `meta_client.metadata_cache_max_capacity` | Integer | `100000` | The configuration about the cache of the metadata. |
| `meta_client.metadata_cache_ttl` | String | `10m` | TTL of the metadata cache. |
| `meta_client.metadata_cache_tti` | String | `5m` | -- |
| `heartbeat` | -- | -- | The heartbeat options. |
| `heartbeat.interval` | String | `3s` | Interval for sending heartbeat messages to the metasrv. |
| `heartbeat.retry_interval` | String | `3s` | Interval for retrying to send heartbeat messages to the metasrv. |
| `logging` | -- | -- | The logging options. |
| `logging.dir` | String | `/tmp/greptimedb/logs` | The directory to store the log files. |
| `logging.level` | String | `None` | The log level. Can be `info`/`debug`/`warn`/`error`. |
| `logging.enable_otlp_tracing` | Bool | `false` | Enable OTLP tracing. |
| `logging.otlp_endpoint` | String | `None` | The OTLP tracing endpoint. |
| `logging.append_stdout` | Bool | `true` | Whether to append logs to stdout. |
| `logging.tracing_sample_ratio` | -- | -- | The percentage of tracing will be sampled and exported.<br/>Valid range `[0, 1]`, 1 means all traces are sampled, 0 means all traces are not sampled, the default value is 1.<br/>ratio > 1 are treated as 1. Fractions < 0 are treated as 0 |
| `logging.tracing_sample_ratio.default_ratio` | Float | `1.0` | -- |
| `tracing` | -- | -- | The tracing options. Only effect when compiled with `tokio-console` feature. |
| `tracing.tokio_console_addr` | String | `None` | The tokio console address. |
