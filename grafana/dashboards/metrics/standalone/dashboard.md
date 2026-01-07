# Overview
| Title | Query | Type | Description | Datasource | Unit | Legend Format |
| --- | --- | --- | --- | --- | --- | --- |
| Uptime | `time() - process_start_time_seconds` | `stat` | The start time of GreptimeDB. | `prometheus` | `s` | `__auto` |
| Version | `SELECT pkg_version FROM information_schema.build_info` | `stat` | GreptimeDB version. | `mysql` | -- | -- |
| Total Ingestion Rate | `sum(rate(greptime_table_operator_ingest_rows[$__rate_interval]))` | `stat` | Total ingestion rate. | `prometheus` | `rowsps` | `__auto` |
| Total Storage Size | `select SUM(disk_size) from information_schema.region_statistics;` | `stat` | Total number of data file size. | `mysql` | `decbytes` | -- |
| Total Rows | `select SUM(region_rows) from information_schema.region_statistics;` | `stat` | Total number of data rows in the cluster. Calculated by sum of rows from each region. | `mysql` | `sishort` | -- |
| Deployment | `SELECT count(*) as datanode FROM information_schema.cluster_info WHERE peer_type = 'DATANODE';`<br/>`SELECT count(*) as frontend FROM information_schema.cluster_info WHERE peer_type = 'FRONTEND';`<br/>`SELECT count(*) as metasrv FROM information_schema.cluster_info WHERE peer_type = 'METASRV';`<br/>`SELECT count(*) as flownode FROM information_schema.cluster_info WHERE peer_type = 'FLOWNODE';` | `stat` | The deployment topology of GreptimeDB. | `mysql` | -- | -- |
| Database Resources | `SELECT COUNT(*) as databases FROM information_schema.schemata WHERE schema_name NOT IN ('greptime_private', 'information_schema')`<br/>`SELECT COUNT(*) as tables FROM information_schema.tables WHERE table_schema != 'information_schema'`<br/>`SELECT COUNT(region_id) as regions FROM information_schema.region_peers`<br/>`SELECT COUNT(*) as flows FROM information_schema.flows` | `stat` | The number of the key resources in GreptimeDB. | `mysql` | -- | -- |
| Data Size | `SELECT SUM(memtable_size) * 0.42825 as WAL FROM information_schema.region_statistics;`<br/>`SELECT SUM(index_size) as index FROM information_schema.region_statistics;`<br/>`SELECT SUM(manifest_size) as manifest FROM information_schema.region_statistics;` | `stat` | The data size of wal/index/manifest in the GreptimeDB. | `mysql` | `decbytes` | -- |
# Ingestion
| Title | Query | Type | Description | Datasource | Unit | Legend Format |
| --- | --- | --- | --- | --- | --- | --- |
| Total Ingestion Rate | `sum(rate(greptime_table_operator_ingest_rows{}[$__rate_interval]))` | `timeseries` | Total ingestion rate.<br/><br/>Here we listed 3 primary protocols:<br/><br/>- Prometheus remote write<br/>- Greptime's gRPC API (when using our ingest SDK)<br/>- Log ingestion http API<br/> | `prometheus` | `rowsps` | `ingestion` |
| Ingestion Rate by Type | `sum(rate(greptime_servers_http_logs_ingestion_counter[$__rate_interval]))`<br/>`sum(rate(greptime_servers_prometheus_remote_write_samples[$__rate_interval]))` | `timeseries` | Total ingestion rate.<br/><br/>Here we listed 3 primary protocols:<br/><br/>- Prometheus remote write<br/>- Greptime's gRPC API (when using our ingest SDK)<br/>- Log ingestion http API<br/> | `prometheus` | `rowsps` | `http-logs` |
# Queries
| Title | Query | Type | Description | Datasource | Unit | Legend Format |
| --- | --- | --- | --- | --- | --- | --- |
| Total Query Rate | `sum (rate(greptime_servers_mysql_query_elapsed_count{}[$__rate_interval]))`<br/>`sum (rate(greptime_servers_postgres_query_elapsed_count{}[$__rate_interval]))`<br/>`sum (rate(greptime_servers_http_promql_elapsed_counte{}[$__rate_interval]))` | `timeseries` | Total rate of query API calls by protocol. This metric is collected from frontends.<br/><br/>Here we listed 3 main protocols:<br/>- MySQL<br/>- Postgres<br/>- Prometheus API<br/><br/>Note that there are some other minor query APIs like /sql are not included | `prometheus` | `reqps` | `mysql` |
# Resources
| Title | Query | Type | Description | Datasource | Unit | Legend Format |
| --- | --- | --- | --- | --- | --- | --- |
| Datanode Memory per Instance | `sum(process_resident_memory_bytes{}) by (instance, pod)`<br/>`max(greptime_memory_limit_in_bytes{})` | `timeseries` | Current memory usage by instance | `prometheus` | `bytes` | `[{{instance}}]-[{{ pod }}]` |
| Datanode CPU Usage per Instance | `sum(rate(process_cpu_seconds_total{}[$__rate_interval]) * 1000) by (instance, pod)`<br/>`max(greptime_cpu_limit_in_millicores{})` | `timeseries` | Current cpu usage by instance | `prometheus` | `none` | `[{{ instance }}]-[{{ pod }}]` |
| Frontend Memory per Instance | `sum(process_resident_memory_bytes{}) by (instance, pod)`<br/>`max(greptime_memory_limit_in_bytes{})` | `timeseries` | Current memory usage by instance | `prometheus` | `bytes` | `[{{ instance }}]-[{{ pod }}]` |
| Frontend CPU Usage per Instance | `sum(rate(process_cpu_seconds_total{}[$__rate_interval]) * 1000) by (instance, pod)`<br/>`max(greptime_cpu_limit_in_millicores{})` | `timeseries` | Current cpu usage by instance | `prometheus` | `none` | `[{{ instance }}]-[{{ pod }}]-cpu` |
| Metasrv Memory per Instance | `sum(process_resident_memory_bytes{}) by (instance, pod)`<br/>`max(greptime_memory_limit_in_bytes{})` | `timeseries` | Current memory usage by instance | `prometheus` | `bytes` | `[{{ instance }}]-[{{ pod }}]-resident` |
| Metasrv CPU Usage per Instance | `sum(rate(process_cpu_seconds_total{}[$__rate_interval]) * 1000) by (instance, pod)`<br/>`max(greptime_cpu_limit_in_millicores{})` | `timeseries` | Current cpu usage by instance | `prometheus` | `none` | `[{{ instance }}]-[{{ pod }}]` |
| Flownode Memory per Instance | `sum(process_resident_memory_bytes{}) by (instance, pod)`<br/>`max(greptime_memory_limit_in_bytes{})` | `timeseries` | Current memory usage by instance | `prometheus` | `bytes` | `[{{ instance }}]-[{{ pod }}]` |
| Flownode CPU Usage per Instance | `sum(rate(process_cpu_seconds_total{}[$__rate_interval]) * 1000) by (instance, pod)`<br/>`max(greptime_cpu_limit_in_millicores{})` | `timeseries` | Current cpu usage by instance | `prometheus` | `none` | `[{{ instance }}]-[{{ pod }}]` |
# Frontend Requests
| Title | Query | Type | Description | Datasource | Unit | Legend Format |
| --- | --- | --- | --- | --- | --- | --- |
| HTTP QPS per Instance | `sum by(instance, pod, path, method, code) (rate(greptime_servers_http_requests_elapsed_count{path!~"/health\|/metrics"}[$__rate_interval]))` | `timeseries` | HTTP QPS per Instance. | `prometheus` | `reqps` | `[{{instance}}]-[{{pod}}]-[{{path}}]-[{{method}}]-[{{code}}]` |
| HTTP P99 per Instance | `histogram_quantile(0.99, sum by(instance, pod, le, path, method, code) (rate(greptime_servers_http_requests_elapsed_bucket{path!~"/health\|/metrics"}[$__rate_interval])))` | `timeseries` | HTTP P99 per Instance. | `prometheus` | `s` | `[{{instance}}]-[{{pod}}]-[{{path}}]-[{{method}}]-[{{code}}]-p99` |
| gRPC QPS per Instance | `sum by(instance, pod, path, code) (rate(greptime_servers_grpc_requests_elapsed_count{}[$__rate_interval]))` | `timeseries` | gRPC QPS per Instance. | `prometheus` | `reqps` | `[{{instance}}]-[{{pod}}]-[{{path}}]-[{{code}}]` |
| gRPC P99 per Instance | `histogram_quantile(0.99, sum by(instance, pod, le, path, code) (rate(greptime_servers_grpc_requests_elapsed_bucket{}[$__rate_interval])))` | `timeseries` | gRPC P99 per Instance. | `prometheus` | `s` | `[{{instance}}]-[{{pod}}]-[{{path}}]-[{{method}}]-[{{code}}]-p99` |
| MySQL QPS per Instance | `sum by(pod, instance)(rate(greptime_servers_mysql_query_elapsed_count{}[$__rate_interval]))` | `timeseries` | MySQL QPS per Instance. | `prometheus` | `reqps` | `[{{instance}}]-[{{pod}}]` |
| MySQL P99 per Instance | `histogram_quantile(0.99, sum by(pod, instance, le) (rate(greptime_servers_mysql_query_elapsed_bucket{}[$__rate_interval])))` | `timeseries` | MySQL P99 per Instance. | `prometheus` | `s` | `[{{ instance }}]-[{{ pod }}]-p99` |
| PostgreSQL QPS per Instance | `sum by(pod, instance)(rate(greptime_servers_postgres_query_elapsed_count{}[$__rate_interval]))` | `timeseries` | PostgreSQL QPS per Instance. | `prometheus` | `reqps` | `[{{instance}}]-[{{pod}}]` |
| PostgreSQL P99 per Instance | `histogram_quantile(0.99, sum by(pod,instance,le) (rate(greptime_servers_postgres_query_elapsed_bucket{}[$__rate_interval])))` | `timeseries` | PostgreSQL P99 per Instance. | `prometheus` | `s` | `[{{instance}}]-[{{pod}}]-p99` |
# Frontend to Datanode
| Title | Query | Type | Description | Datasource | Unit | Legend Format |
| --- | --- | --- | --- | --- | --- | --- |
| Ingest Rows per Instance | `sum by(instance, pod)(rate(greptime_table_operator_ingest_rows{}[$__rate_interval]))` | `timeseries` | Ingestion rate by row as in each frontend | `prometheus` | `rowsps` | `[{{instance}}]-[{{pod}}]` |
| Region Call QPS per Instance | `sum by(instance, pod, request_type) (rate(greptime_grpc_region_request_count{}[$__rate_interval]))` | `timeseries` | Region Call QPS per Instance. | `prometheus` | `ops` | `[{{instance}}]-[{{pod}}]-[{{request_type}}]` |
| Region Call P99 per Instance | `histogram_quantile(0.99, sum by(instance, pod, le, request_type) (rate(greptime_grpc_region_request_bucket{}[$__rate_interval])))` | `timeseries` | Region Call P99 per Instance. | `prometheus` | `s` | `[{{instance}}]-[{{pod}}]-[{{request_type}}]` |
| Frontend Handle Bulk Insert Elapsed Time  | `sum by(instance, pod, stage) (rate(greptime_table_operator_handle_bulk_insert_sum[$__rate_interval]))/sum by(instance, pod, stage) (rate(greptime_table_operator_handle_bulk_insert_count[$__rate_interval]))`<br/>`histogram_quantile(0.99, sum by(instance, pod, stage, le) (rate(greptime_table_operator_handle_bulk_insert_bucket[$__rate_interval])))` | `timeseries` | Per-stage time for frontend to handle bulk insert requests | `prometheus` | `s` | `[{{instance}}]-[{{pod}}]-[{{stage}}]-AVG` |
# Mito Engine
| Title | Query | Type | Description | Datasource | Unit | Legend Format |
| --- | --- | --- | --- | --- | --- | --- |
| Request OPS per Instance | `sum by(instance, pod, type) (rate(greptime_mito_handle_request_elapsed_count{}[$__rate_interval]))` | `timeseries` | Request QPS per Instance. | `prometheus` | `ops` | `[{{instance}}]-[{{pod}}]-[{{type}}]` |
| Request P99 per Instance | `histogram_quantile(0.99, sum by(instance, pod, le, type) (rate(greptime_mito_handle_request_elapsed_bucket{}[$__rate_interval])))` | `timeseries` | Request P99 per Instance. | `prometheus` | `s` | `[{{instance}}]-[{{pod}}]-[{{type}}]` |
| Write Buffer per Instance | `greptime_mito_write_buffer_bytes{}` | `timeseries` | Write Buffer per Instance. | `prometheus` | `decbytes` | `[{{instance}}]-[{{pod}}]` |
| Write Rows per Instance | `sum by (instance, pod) (rate(greptime_mito_write_rows_total{}[$__rate_interval]))` | `timeseries` | Ingestion size by row counts. | `prometheus` | `rowsps` | `[{{instance}}]-[{{pod}}]` |
| Flush OPS per Instance | `sum by(instance, pod, reason) (rate(greptime_mito_flush_requests_total{}[$__rate_interval]))` | `timeseries` | Flush QPS per Instance. | `prometheus` | `ops` | `[{{instance}}]-[{{pod}}]-[{{reason}}]` |
| Write Stall per Instance | `sum by(instance, pod) (greptime_mito_write_stall_total{})` | `timeseries` | Write Stall per Instance. | `prometheus` | -- | `[{{instance}}]-[{{pod}}]` |
| Read Stage OPS per Instance | `sum by(instance, pod) (rate(greptime_mito_read_stage_elapsed_count{ stage="total"}[$__rate_interval]))` | `timeseries` | Read Stage OPS per Instance. | `prometheus` | `ops` | `[{{instance}}]-[{{pod}}]` |
| Read Stage P99 per Instance | `histogram_quantile(0.99, sum by(instance, pod, le, stage) (rate(greptime_mito_read_stage_elapsed_bucket{}[$__rate_interval])))` | `timeseries` | Read Stage P99 per Instance. | `prometheus` | `s` | `[{{instance}}]-[{{pod}}]-[{{stage}}]` |
| Write Stage P99 per Instance | `histogram_quantile(0.99, sum by(instance, pod, le, stage) (rate(greptime_mito_write_stage_elapsed_bucket{}[$__rate_interval])))` | `timeseries` | Write Stage P99 per Instance. | `prometheus` | `s` | `[{{instance}}]-[{{pod}}]-[{{stage}}]` |
| Compaction OPS per Instance | `sum by(instance, pod) (rate(greptime_mito_compaction_total_elapsed_count{}[$__rate_interval]))` | `timeseries` | Compaction OPS per Instance. | `prometheus` | `ops` | `[{{ instance }}]-[{{pod}}]` |
| Compaction Elapsed Time per Instance by Stage | `histogram_quantile(0.99, sum by(instance, pod, le, stage) (rate(greptime_mito_compaction_stage_elapsed_bucket{}[$__rate_interval])))`<br/>`sum by(instance, pod, stage) (rate(greptime_mito_compaction_stage_elapsed_sum{}[$__rate_interval]))/sum by(instance, pod, stage) (rate(greptime_mito_compaction_stage_elapsed_count{}[$__rate_interval]))` | `timeseries` | Compaction latency by stage | `prometheus` | `s` | `[{{instance}}]-[{{pod}}]-[{{stage}}]-p99` |
| Compaction P99 per Instance | `histogram_quantile(0.99, sum by(instance, pod, le,stage) (rate(greptime_mito_compaction_total_elapsed_bucket{}[$__rate_interval])))` | `timeseries` | Compaction P99 per Instance. | `prometheus` | `s` | `[{{instance}}]-[{{pod}}]-[{{stage}}]-compaction` |
| WAL write size | `histogram_quantile(0.95, sum by(le,instance, pod) (rate(raft_engine_write_size_bucket[$__rate_interval])))`<br/>`histogram_quantile(0.99, sum by(le,instance,pod) (rate(raft_engine_write_size_bucket[$__rate_interval])))`<br/>`sum by (instance, pod)(rate(raft_engine_write_size_sum[$__rate_interval]))` | `timeseries` | Write-ahead logs write size as bytes. This chart includes stats of p95 and p99 size by instance, total WAL write rate. | `prometheus` | `bytes` | `[{{instance}}]-[{{pod}}]-req-size-p95` |
| Cached Bytes per Instance | `greptime_mito_cache_bytes{}` | `timeseries` | Cached Bytes per Instance. | `prometheus` | `decbytes` | `[{{instance}}]-[{{pod}}]-[{{type}}]` |
| Inflight Compaction | `greptime_mito_inflight_compaction_count` | `timeseries` | Ongoing compaction task count | `prometheus` | `none` | `[{{instance}}]-[{{pod}}]` |
| WAL sync duration seconds | `histogram_quantile(0.99, sum by(le, type, node, instance, pod) (rate(raft_engine_sync_log_duration_seconds_bucket[$__rate_interval])))` | `timeseries` | Raft engine (local disk) log store sync latency, p99 | `prometheus` | `s` | `[{{instance}}]-[{{pod}}]-p99` |
| Log Store op duration seconds | `histogram_quantile(0.99, sum by(le,logstore,optype,instance, pod) (rate(greptime_logstore_op_elapsed_bucket[$__rate_interval])))` | `timeseries` | Write-ahead log operations latency at p99 | `prometheus` | `s` | `[{{instance}}]-[{{pod}}]-[{{logstore}}]-[{{optype}}]-p99` |
| Inflight Flush | `greptime_mito_inflight_flush_count` | `timeseries` | Ongoing flush task count | `prometheus` | `none` | `[{{instance}}]-[{{pod}}]` |
| Compaction Input/Output Bytes | `sum by(instance, pod) (greptime_mito_compaction_input_bytes)`<br/>`sum by(instance, pod) (greptime_mito_compaction_output_bytes)` | `timeseries` | Compaction oinput output bytes | `prometheus` | `bytes` | `[{{instance}}]-[{{pod}}]-input` |
| Region Worker Handle Bulk Insert Requests | `histogram_quantile(0.95, sum by(le,instance, stage, pod) (rate(greptime_region_worker_handle_write_bucket[$__rate_interval])))`<br/>`sum by(instance, stage, pod) (rate(greptime_region_worker_handle_write_sum[$__rate_interval]))/sum by(instance, stage, pod) (rate(greptime_region_worker_handle_write_count[$__rate_interval]))` | `timeseries` | Per-stage elapsed time for region worker to handle bulk insert region requests. | `prometheus` | `s` | `[{{instance}}]-[{{pod}}]-[{{stage}}]-P95` |
| Active Series and Field Builders Count | `sum by(instance, pod) (greptime_mito_memtable_active_series_count)`<br/>`sum by(instance, pod) (greptime_mito_memtable_field_builder_count)` | `timeseries` | Compaction oinput output bytes | `prometheus` | `none` | `[{{instance}}]-[{{pod}}]-series` |
| Region Worker Convert Requests | `histogram_quantile(0.95, sum by(le, instance, stage, pod) (rate(greptime_datanode_convert_region_request_bucket[$__rate_interval])))`<br/>`sum by(le,instance, stage, pod) (rate(greptime_datanode_convert_region_request_sum[$__rate_interval]))/sum by(le,instance, stage, pod) (rate(greptime_datanode_convert_region_request_count[$__rate_interval]))` | `timeseries` | Per-stage elapsed time for region worker to decode requests. | `prometheus` | `s` | `[{{instance}}]-[{{pod}}]-[{{stage}}]-P95` |
| Cache Miss | `sum by (instance,pod, type) (rate(greptime_mito_cache_miss{}[$__rate_interval]))` | `timeseries` | The local cache miss of the datanode. | `prometheus` | -- | `[{{instance}}]-[{{pod}}]-[{{type}}]` |
# OpenDAL
| Title | Query | Type | Description | Datasource | Unit | Legend Format |
| --- | --- | --- | --- | --- | --- | --- |
| QPS per Instance | `sum by(instance, pod, scheme, operation) (rate(opendal_operation_duration_seconds_count{}[$__rate_interval]))` | `timeseries` | QPS per Instance. | `prometheus` | `ops` | `[{{instance}}]-[{{pod}}]-[{{scheme}}]-[{{operation}}]` |
| Read QPS per Instance | `sum by(instance, pod, scheme, operation) (rate(opendal_operation_duration_seconds_count{ operation=~"read\|Reader::read"}[$__rate_interval]))` | `timeseries` | Read QPS per Instance. | `prometheus` | `ops` | `[{{instance}}]-[{{pod}}]-[{{scheme}}]-[{{operation}}]` |
| Read P99 per Instance | `histogram_quantile(0.99, sum by(instance, pod, le, scheme, operation) (rate(opendal_operation_duration_seconds_bucket{operation=~"read\|Reader::read"}[$__rate_interval])))` | `timeseries` | Read P99 per Instance. | `prometheus` | `s` | `[{{instance}}]-[{{pod}}]-[{{scheme}}]-[{{operation}}]` |
| Write QPS per Instance | `sum by(instance, pod, scheme, operation) (rate(opendal_operation_duration_seconds_count{ operation=~"write\|Writer::write\|Writer::close"}[$__rate_interval]))` | `timeseries` | Write QPS per Instance. | `prometheus` | `ops` | `[{{instance}}]-[{{pod}}]-[{{scheme}}]-[{{operation}}]` |
| Write P99 per Instance | `histogram_quantile(0.99, sum by(instance, pod, le, scheme, operation) (rate(opendal_operation_duration_seconds_bucket{ operation =~ "Writer::write\|Writer::close\|write"}[$__rate_interval])))` | `timeseries` | Write P99 per Instance. | `prometheus` | `s` | `[{{instance}}]-[{{pod}}]-[{{scheme}}]-[{{operation}}]` |
| List QPS per Instance | `sum by(instance, pod, scheme) (rate(opendal_operation_duration_seconds_count{ operation="list"}[$__rate_interval]))` | `timeseries` | List QPS per Instance. | `prometheus` | `ops` | `[{{instance}}]-[{{pod}}]-[{{scheme}}]` |
| List P99 per Instance | `histogram_quantile(0.99, sum by(instance, pod, le, scheme) (rate(opendal_operation_duration_seconds_bucket{ operation="list"}[$__rate_interval])))` | `timeseries` | List P99 per Instance. | `prometheus` | `s` | `[{{instance}}]-[{{pod}}]-[{{scheme}}]` |
| Other Requests per Instance | `sum by(instance, pod, scheme, operation) (rate(opendal_operation_duration_seconds_count{operation!~"read\|write\|list\|stat"}[$__rate_interval]))` | `timeseries` | Other Requests per Instance. | `prometheus` | `ops` | `[{{instance}}]-[{{pod}}]-[{{scheme}}]-[{{operation}}]` |
| Other Request P99 per Instance | `histogram_quantile(0.99, sum by(instance, pod, le, scheme, operation) (rate(opendal_operation_duration_seconds_bucket{ operation!~"read\|write\|list\|Writer::write\|Writer::close\|Reader::read"}[$__rate_interval])))` | `timeseries` | Other Request P99 per Instance. | `prometheus` | `s` | `[{{instance}}]-[{{pod}}]-[{{scheme}}]-[{{operation}}]` |
| Opendal traffic | `sum by(instance, pod, scheme, operation) (rate(opendal_operation_bytes_sum{}[$__rate_interval]))` | `timeseries` | Total traffic as in bytes by instance and operation | `prometheus` | `decbytes` | `[{{instance}}]-[{{pod}}]-[{{scheme}}]-[{{operation}}]` |
| OpenDAL errors per Instance | `sum by(instance, pod, scheme, operation, error) (rate(opendal_operation_errors_total{ error!="NotFound"}[$__rate_interval]))` | `timeseries` | OpenDAL error counts per Instance. | `prometheus` | -- | `[{{instance}}]-[{{pod}}]-[{{scheme}}]-[{{operation}}]-[{{error}}]` |
# Remote WAL
| Title | Query | Type | Description | Datasource | Unit | Legend Format |
| --- | --- | --- | --- | --- | --- | --- |
| Triggered region flush total | `meta_triggered_region_flush_total` | `timeseries` | Triggered region flush total | `prometheus` | `none` | `{{pod}}-{{topic_name}}` |
| Triggered region checkpoint total | `meta_triggered_region_checkpoint_total` | `timeseries` | Triggered region checkpoint total | `prometheus` | `none` | `{{pod}}-{{topic_name}}` |
| Topic estimated replay size | `meta_topic_estimated_replay_size` | `timeseries` | Topic estimated max replay size | `prometheus` | `bytes` | `{{pod}}-{{topic_name}}` |
| Kafka logstore's bytes traffic | `rate(greptime_logstore_kafka_client_bytes_total[$__rate_interval])` | `timeseries` | Kafka logstore's bytes traffic | `prometheus` | `bytes` | `{{pod}}-{{logstore}}` |
# Metasrv
| Title | Query | Type | Description | Datasource | Unit | Legend Format |
| --- | --- | --- | --- | --- | --- | --- |
| Region migration datanode | `greptime_meta_region_migration_stat{datanode_type="src"}`<br/>`greptime_meta_region_migration_stat{datanode_type="desc"}` | `status-history` | Counter of region migration by source and destination | `prometheus` | -- | `from-datanode-{{datanode_id}}` |
| Region migration error | `greptime_meta_region_migration_error` | `timeseries` | Counter of region migration error | `prometheus` | `none` | `{{pod}}-{{state}}-{{error_type}}` |
| Datanode load | `greptime_datanode_load` | `timeseries` | Gauge of load information of each datanode, collected via heartbeat between datanode and metasrv. This information is for metasrv to schedule workloads. | `prometheus` | `binBps` | `Datanode-{{datanode_id}}-writeload` |
| Rate of SQL Executions (RDS) | `rate(greptime_meta_rds_pg_sql_execute_elapsed_ms_count[$__rate_interval])` | `timeseries` | Displays the rate of SQL executions processed by the Meta service using the RDS backend. | `prometheus` | `none` | `{{pod}} {{op}} {{type}} {{result}} ` |
| SQL Execution Latency (RDS) | `histogram_quantile(0.90, sum by(pod, op, type, result, le) (rate(greptime_meta_rds_pg_sql_execute_elapsed_ms_bucket[$__rate_interval])))` | `timeseries` | Measures the response time of SQL executions via the RDS backend.  | `prometheus` | `ms` | `{{pod}} {{op}} {{type}} {{result}} p90` |
| Handler Execution Latency | `histogram_quantile(0.90, sum by(pod, le, name) (
  rate(greptime_meta_handler_execute_bucket[$__rate_interval])
))` | `timeseries` | Shows latency of Meta handlers by pod and handler name, useful for monitoring handler performance and detecting latency spikes.<br/> | `prometheus` | `s` | `{{pod}} {{name}} p90` |
| Heartbeat Packet Size | `histogram_quantile(0.9, sum by(pod, le) (greptime_meta_heartbeat_stat_memory_size_bucket))` | `timeseries` | Shows p90 heartbeat message sizes, helping track network usage and identify anomalies in heartbeat payload.<br/> | `prometheus` | `bytes` | `{{pod}}` |
| Meta Heartbeat Receive Rate | `rate(greptime_meta_heartbeat_rate[$__rate_interval])` | `timeseries` | Gauge of load information of each datanode, collected via heartbeat between datanode and metasrv. This information is for metasrv to schedule workloads. | `prometheus` | `s` | `{{pod}}` |
| Meta KV Ops Latency | `histogram_quantile(0.99, sum by(pod, le, op, target) (greptime_meta_kv_request_elapsed_bucket))` | `timeseries` | Gauge of load information of each datanode, collected via heartbeat between datanode and metasrv. This information is for metasrv to schedule workloads. | `prometheus` | `s` | `{{pod}}-{{op}} p99` |
| Rate of meta KV Ops | `rate(greptime_meta_kv_request_elapsed_count[$__rate_interval])` | `timeseries` | Gauge of load information of each datanode, collected via heartbeat between datanode and metasrv. This information is for metasrv to schedule workloads. | `prometheus` | `none` | `{{pod}}-{{op}} p99` |
| DDL Latency | `histogram_quantile(0.9, sum by(le, pod, step) (greptime_meta_procedure_create_tables_bucket))`<br/>`histogram_quantile(0.9, sum by(le, pod, step) (greptime_meta_procedure_create_table))`<br/>`histogram_quantile(0.9, sum by(le, pod, step) (greptime_meta_procedure_create_view))`<br/>`histogram_quantile(0.9, sum by(le, pod, step) (greptime_meta_procedure_create_flow))`<br/>`histogram_quantile(0.9, sum by(le, pod, step) (greptime_meta_procedure_drop_table))`<br/>`histogram_quantile(0.9, sum by(le, pod, step) (greptime_meta_procedure_alter_table))` | `timeseries` | Gauge of load information of each datanode, collected via heartbeat between datanode and metasrv. This information is for metasrv to schedule workloads. | `prometheus` | `s` | `CreateLogicalTables-{{step}} p90` |
| Reconciliation stats | `greptime_meta_reconciliation_stats` | `timeseries` | Reconciliation stats | `prometheus` | `s` | `{{pod}}-{{table_type}}-{{type}}` |
| Reconciliation steps | `histogram_quantile(0.9, greptime_meta_reconciliation_procedure_bucket)` | `timeseries` | Time spent evaluating triggers. | `prometheus` | `s` | `{{procedure_name}}-{{step}}-P90` |
# Flownode
| Title | Query | Type | Description | Datasource | Unit | Legend Format |
| --- | --- | --- | --- | --- | --- | --- |
| Flow Ingest / Output Rate | `sum by(instance, pod, direction) (rate(greptime_flow_processed_rows[$__rate_interval]))` | `timeseries` | Flow Ingest / Output Rate. | `prometheus` | -- | `[{{pod}}]-[{{instance}}]-[{{direction}}]` |
| Flow Ingest Latency | `histogram_quantile(0.95, sum(rate(greptime_flow_insert_elapsed_bucket[$__rate_interval])) by (le, instance, pod))`<br/>`histogram_quantile(0.99, sum(rate(greptime_flow_insert_elapsed_bucket[$__rate_interval])) by (le, instance, pod))` | `timeseries` | Flow Ingest Latency. | `prometheus` | -- | `[{{instance}}]-[{{pod}}]-p95` |
| Flow Operation Latency | `histogram_quantile(0.95, sum(rate(greptime_flow_processing_time_bucket[$__rate_interval])) by (le,instance,pod,type))`<br/>`histogram_quantile(0.99, sum(rate(greptime_flow_processing_time_bucket[$__rate_interval])) by (le,instance,pod,type))` | `timeseries` | Flow Operation Latency. | `prometheus` | -- | `[{{instance}}]-[{{pod}}]-[{{type}}]-p95` |
| Flow Buffer Size per Instance | `greptime_flow_input_buf_size` | `timeseries` | Flow Buffer Size per Instance. | `prometheus` | -- | `[{{instance}}]-[{{pod}]` |
| Flow Processing Error per Instance | `sum by(instance,pod,code) (rate(greptime_flow_errors[$__rate_interval]))` | `timeseries` | Flow Processing Error per Instance. | `prometheus` | -- | `[{{instance}}]-[{{pod}}]-[{{code}}]` |
# Row title
| Title | Query | Type | Description | Datasource | Unit | Legend Format |
| --- | --- | --- | --- | --- | --- | --- |
| Trigger Count | `greptime_trigger_count{}` | `timeseries` |  | `prometheus` | -- | `__auto` |
| Trigger Eval Elapsed | `histogram_quantile(0.99, 
  rate(greptime_trigger_evaluate_elapsed_bucket[$__rate_interval])
)`<br/>`histogram_quantile(0.75, 
  rate(greptime_trigger_evaluate_elapsed_bucket[$__rate_interval])
)` | `timeseries` | Failure rate of trigger evaluation. | `prometheus` | `s` | `[{{instance}}]-[{{pod}}]-p99` |
| Trigger Eval Failure Rate | `rate(greptime_trigger_evaluate_failure_count[$__rate_interval])` | `timeseries` | Time to send trigger alerts. | `prometheus` | `s` | `__auto` |
| Send Alert Elapsed | `histogram_quantile(0.99, 
  rate(greptime_trigger_send_alert_elapsed_bucket[$__rate_interval])
)`<br/>`histogram_quantile(0.75, 
  rate(greptime_trigger_send_alert_elapsed_bucket[$__rate_interval])
)` | `timeseries` | Failure rate while sending trigger alerts. | `prometheus` | `s` | `[{{instance}}]-[{{pod}}]-[{{channel_type}}]-p99` |
| Send Alert Failure Rate | `rate(greptime_trigger_send_alert_failure_count[$__rate_interval])` | `timeseries` | Time to persist trigger alert records. | `prometheus` | `s` | `__auto` |
| Save Alert Elapsed | `histogram_quantile(0.99, 
  rate(greptime_trigger_save_alert_record_elapsed_bucket[$__rate_interval])
)`<br/>`histogram_quantile(0.75, 
  rate(greptime_trigger_save_alert_record_elapsed_bucket[$__rate_interval])
)` | `timeseries` | Time to persist trigger alert records. | `prometheus` | `s` | `[{{instance}}]-[{{pod}}]-[{{storage_type}}]-p99` |
| Save Alert Failure Rate | `rate(greptime_trigger_save_alert_record_failure_count[$__rate_interval])` | `timeseries` | Failure rate of saving trigger alert records. | `prometheus` | `s` | `__auto` |
