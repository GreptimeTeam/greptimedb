# GreptimeDB troubleshooting dashboard review

This document records a metric-oriented review of the current GreptimeDB Grafana metrics dashboards from an operator and SRE perspective. It is intended to be the source design before changing generated dashboard JSON directly.

## Review scope

- Current cluster and standalone metric dashboards use the same panel groups: Overview, Ingestion, Queries, Resources, Frontend Requests, Frontend to Datanode, Mito Engine, OpenDAL, Remote WAL, Metasrv, Flownode, Trigger, Hotspot, and Autopilot.
- The existing dashboard already exposes many throughput and latency panels, but its first screen is mostly business/resource totals. Error, saturation, backlog, and metasrv health signals are spread across later sections, so an on-call engineer cannot quickly answer: "Is GreptimeDB healthy?", "Are users failing?", "Where is the bottleneck?", and "Which component should I inspect first?".
- The review below groups metrics by the failure mode they explain, then proposes dashboard sections and panels that make incident triage faster.

## Metric inventory and operational meaning

Most metrics are defined in `metrics.rs` files. The following inventory focuses on what each metric family tells an operator during incident triage.

### Process and resource metrics

| Metric family | Meaning | SRE usage |
| --- | --- | --- |
| `process_start_time_seconds` | Process start timestamp from the Prometheus process collector. | Detect restarts and calculate uptime. |
| `process_resident_memory_bytes` | Resident memory consumed by a process. | Check memory pressure and correlate with query/compaction rejections. |
| `process_cpu_seconds_total` | CPU time consumed by a process. | Check CPU saturation and noisy instances. |
| `greptime_memory_limit_in_bytes`, `greptime_cpu_limit_in_millicores` | Runtime/container resource limits exported by GreptimeDB. | Compare usage against limits rather than looking only at absolute usage. |
| `greptime_runtime_threads_alive`, `greptime_runtime_threads_idle` | Runtime thread pool size and idle threads. | Detect runtime starvation when idle threads approach zero under load. |

### Frontend, protocol, and user-facing request metrics

| Metric family | Meaning | SRE usage |
| --- | --- | --- |
| `greptime_servers_http_request_counter`, `greptime_servers_http_requests_total`, `greptime_servers_http_requests_elapsed` | HTTP request volume and latency, labeled by method, path, code, and database where available. | Build HTTP QPS, non-2xx rate, and tail-latency panels for APIs such as SQL, PromQL, logs, OTLP, and InfluxDB write. |
| `greptime_servers_grpc_requests_total`, `greptime_servers_grpc_requests_elapsed` | gRPC request volume and latency by path/code. | Detect SDK and internal gRPC failures or slow paths. |
| `greptime_servers_mysql_query_elapsed`, `greptime_servers_postgres_query_elapsed` | SQL query count and latency for MySQL/PostgreSQL protocols. | Track SQL availability and p95/p99 latency by frontend instance. |
| `greptime_servers_error` | Server error counter labeled by protocol. | Primary user-visible error signal for server protocols. Put this in the first health row. |
| `greptime_servers_auth_failure_count` | Authentication failures by code. | Distinguish credential/configuration issues from database failures. |
| `greptime_servers_request_memory_in_use_bytes`, `greptime_servers_request_memory_limit_bytes`, `greptime_servers_request_memory_rejected_total` | Frontend request memory pool usage, limit, and rejected requests. | Identify memory saturation that directly rejects user requests. |
| `greptime_frontend_grpc_handle_query_elapsed`, `greptime_frontend_promql_query_metrics_elapsed` | Frontend query handling and PromQL metric query latency. | Separate protocol latency from query execution latency. |
| `greptime_frontend_otlp_metrics_rows`, `greptime_frontend_otlp_logs_rows`, `greptime_frontend_otlp_traces_rows`, `greptime_frontend_otlp_traces_failure_count` | OTLP ingestion rows and trace failures. | Make observability-ingestion problems visible alongside logs and metrics ingestion. |
| `greptime_frontend_heartbeat_send_count`, `greptime_frontend_heartbeat_recv_count` | Frontend heartbeat send/receive counts. | Metasrv liveness of frontend nodes. |

### Ingestion, table operator, and pipeline metrics

| Metric family | Meaning | SRE usage |
| --- | --- | --- |
| `greptime_table_operator_ingest_rows`, `greptime_table_operator_delete_rows` | Rows ingested/deleted through table operators. | Main data-ingestion throughput and delete workload trend. |
| `greptime_table_operator_handle_bulk_insert`, `greptime_table_operator_bulk_insert_message_rows`, `greptime_table_operator_bulk_insert_message_size` | Bulk insert latency and message size/row counters. | Distinguish large batch effects from backend write latency. |
| `greptime_servers_prometheus_remote_write_samples`, `greptime_servers_http_logs_ingestion_counter`, `greptime_servers_loki_logs_ingestion_counter`, `greptime_servers_elasticsearch_logs_docs_count` | Protocol-specific ingestion volume. | Show which ingestion protocol is active during spikes. |
| `greptime_prom_store_pending_rows`, `greptime_prom_store_pending_batches`, `greptime_prom_store_pending_workers`, `greptime_prom_store_flush_total`, `greptime_prom_store_flush_rows`, `greptime_prom_store_flush_elapsed` | Prometheus remote-write buffering and flush pipeline state. | Detect backlog growth before it becomes data-loss or high latency. |
| `greptime_pending_rows_flush_failures`, `greptime_pending_rows_flush_dropped_rows` | Pending-row flush failures and dropped rows. | Critical data-loss/error indicators for the first triage page. |
| `greptime_pipeline_create_duration_seconds`, `greptime_pipeline_delete_duration_seconds`, `greptime_pipeline_retrieve_duration_seconds`, `greptime_pipeline_table_find_count` | Pipeline-management latency and lookup count. | Diagnose log pipeline configuration or lookup bottlenecks. |

### Query execution metrics

| Metric family | Meaning | SRE usage |
| --- | --- | --- |
| `greptime_query_stage_elapsed` | Query-stage latency. | Identify whether planning, scanning, merging, or other stages dominate tail latency. |
| `greptime_query_memory_pool_usage_bytes`, `greptime_query_memory_pool_rejected_total` | Query memory usage and query memory rejections. | Explain query failures caused by memory saturation. |
| `greptime_query_merge_scan_regions`, `greptime_query_merge_scan_errors_total` | Merge-scan region fan-out and errors. | Identify high fan-out, region-level query errors, and expensive table layouts. |
| `greptime_push_down_fallback_errors_total` | Pushdown fallback errors. | Detect failed query optimization paths that can inflate query latency. |
| `greptime_promql_series_count` | Series count touched by PromQL. | Correlate PromQL latency with cardinality. |

### Datanode and Mito engine metrics

| Metric family | Meaning | SRE usage |
| --- | --- | --- |
| `greptime_datanode_handle_region_request_elapsed`, `greptime_datanode_region_request_fail_count` | Region request latency and failures at datanodes. | Identify backend request failures after the frontend routes a request. |
| `greptime_datanode_region_failed_insert_count`, `greptime_datanode_region_changed_row_count` | Region-level failed inserts and changed rows. | Detect write errors and write amplification per datanode/region. |
| `greptime_datanode_heartbeat_send_count`, `greptime_datanode_heartbeat_recv_count`, `greptime_last_sent_heartbeat_lease_elapsed`, `greptime_last_received_heartbeat_lease_elapsed`, `greptime_lease_expired_region`, `greptime_heartbeat_region_leases` | Datanode heartbeat and region lease state. | Detect metasrv connectivity and lease-expiry symptoms. |
| `greptime_mito_handle_request_elapsed` | Mito request latency by request type. | Main backend storage-engine service-time signal. |
| `greptime_mito_write_stage_elapsed`, `greptime_mito_read_stage_elapsed` | Per-stage write/read latency. | Localize slow writes and reads inside the engine. |
| `greptime_mito_write_buffer_bytes`, `greptime_mito_write_rows_total`, `greptime_mito_write_stall_total`, `greptime_mito_write_stalling_count`, `greptime_mito_write_reject_total` | Write-buffer size, rows, stalls, active stalling, and rejected writes. | Detect write-path saturation and backpressure. |
| `greptime_mito_flush_requests_total`, `greptime_mito_flush_elapsed`, `greptime_mito_flush_bytes_total`, `greptime_mito_flush_file_total`, `greptime_mito_inflight_flush_count`, `greptime_mito_flush_failure_total` | Flush work, latency, bytes/files, in-flight jobs, and failures. | Identify flush bottlenecks, large flush jobs, and durable-write risk. |
| `greptime_mito_compaction_requests_total`, `greptime_mito_compaction_total_elapsed`, `greptime_mito_compaction_stage_elapsed`, `greptime_mito_compaction_input_bytes`, `greptime_mito_compaction_output_bytes`, `greptime_mito_inflight_compaction_count`, `greptime_mito_compaction_failure_total` | Compaction work, latency, IO, in-flight jobs, and failures. | Diagnose read amplification, object-store pressure, and compaction backlog/failures. |
| `greptime_mito_compaction_memory_in_use_bytes`, `greptime_mito_compaction_memory_limit_bytes`, `greptime_mito_compaction_memory_rejected_total`, `greptime_mito_compaction_memory_wait_seconds` | Compaction memory pool usage, limit, rejections, and wait time. | Explain delayed or rejected compactions caused by memory limits. |
| `greptime_mito_scan_memory_usage_bytes`, `greptime_mito_scan_memory_exhausted_total`, `greptime_mito_scan_requests_rejected_total` | Scan memory usage and rejections/exhaustion. | Explain query failures or slow scans under memory pressure. |
| `greptime_mito_cache_bytes`, `greptime_mito_cache_hit`, `greptime_mito_cache_miss`, `greptime_mito_cache_eviction` | Cache size, hit/miss, and eviction. | Diagnose cache efficiency and object-store read pressure. |
| `greptime_mito_memtable_active_series_count`, `greptime_mito_memtable_field_builder_count`, `greptime_mito_memtable_dict_bytes` | Memtable cardinality and memory-like structures. | Identify high cardinality and schema/cardinality-driven write pressure. |
| `greptime_mito_region_count`, `greptime_mito_in_progress_scan` | Region count and active scans. | Correlate load with region placement and concurrent scans. |
| `greptime_mito_gc_runs_total`, `greptime_mito_gc_duration_seconds`, `greptime_mito_gc_errors_total`, `greptime_mito_gc_files_deleted_total`, `greptime_mito_gc_delete_file_count`, `greptime_mito_gc_orphaned_index_files`, `greptime_mito_gc_skipped_unparsable_files` | Mito garbage-collection activity, duration, errors, deleted files, and skipped files. | Detect cleanup failures and orphaned-file growth. |
| `greptime_manifest_op_elapsed` | Manifest operation latency. | Diagnose metadata persistence latency in the engine. |
| `greptime_region_worker_handle_write`, `greptime_datanode_convert_region_request` | Region worker write handling and request conversion latency by stage. | Decompose write-path latency around region-worker processing. |
| `greptime_index_apply_elapsed`, `greptime_index_apply_memory_usage` | Index application latency and memory usage. | Explain indexed-read latency and memory pressure when applying index filters. |
| `greptime_index_create_elapsed`, `greptime_index_create_rows_total`, `greptime_index_create_bytes_total`, `greptime_index_create_memory_usage` | Index creation latency, indexed rows/bytes, and memory usage. | Explain slow flush/compaction jobs and storage pressure caused by index creation. |
| `greptime_index_io_bytes_total`, `greptime_index_io_op_total` | Index IO bytes and operations by operation and file type. | Diagnose puffin/intermediate-file read/write/seek/flush pressure. |

### Object store, WAL, and log-store metrics

| Metric family | Meaning | SRE usage |
| --- | --- | --- |
| `opendal_operation_*` | OpenDAL request count, latency, bytes, and errors by scheme/operation/error. | Diagnose object-store availability, latency, and noisy operation types. |
| `raft_engine_sync_log_duration_seconds` | Local raft-engine WAL sync latency. | Detect local disk WAL fsync latency. |
| `greptime_logstore_op_elapsed`, `greptime_logstore_op_bytes_total` | Log-store operation latency and bytes by logstore/optype. | Compare WAL read/write latency across log-store backends. |
| `greptime_logstore_kafka_client_bytes_total`, `greptime_logstore_kafka_client_traffic_total`, `greptime_logstore_kafka_client_produce_elapsed` | Kafka client bytes, traffic, and produce latency. | Diagnose remote WAL/Kafka throughput and latency. |
| `meta_triggered_region_flush_total`, `meta_triggered_region_checkpoint_total` | Remote WAL flush/checkpoint triggers. | Confirm metasrv-triggered remote WAL maintenance is running. |

### Metasrv and catalog metrics

| Metric family | Meaning | SRE usage |
| --- | --- | --- |
| `greptime_meta_heartbeat_connection_num`, `greptime_meta_heartbeat_rate`, `greptime_meta_heartbeat_recv`, `greptime_meta_heartbeat_stat_memory_size` | Metasrv heartbeat connection count, receive rate, and heartbeat payload size. | Detect missing nodes, heartbeat storms, or oversized heartbeat payloads. |
| `greptime_meta_handler_execute` | Metasrv handler latency by name. | Identify slow metasrv handlers. |
| `greptime_meta_inactive_regions` | Count of inactive regions. | Critical availability signal for data serving. |
| `greptime_meta_region_migration_stat`, `greptime_meta_region_migration_execute`, `greptime_meta_region_migration_stage_elapsed`, `greptime_meta_region_migration_error`, `greptime_meta_region_migration_fail` | Region migration state, action count, stage latency, errors, and failures. | Diagnose rebalance/failover incidents. |
| `greptime_meta_kv_request_elapsed`, `greptime_meta_kv_cache_hit`, `greptime_meta_kv_cache_miss`, `greptime_meta_txn_request` | Metadata KV latency, cache efficiency, and transaction requests. | Diagnose metadata store latency or cache misses. |
| `greptime_meta_rds_pg_sql_execute_elapsed_ms` | RDS PostgreSQL-backed metadata SQL latency. | Diagnose RDS-backed metadata store performance. |
| `greptime_meta_procedure_*`, `greptime_meta_reconciliation_*` | Procedure latency/counters and reconciliation steps/errors/stats. | Find failing DDL/reconciliation flows. |
| `greptime_metasrv_gc_*` | Metasrv garbage-collection cycles, duration, failed regions, datanode calls, and candidates. | Diagnose metasrv cleanup health. |
| `greptime_catalog_*`, `greptime_process_*` | Catalog counts/KV latency and process-list/kill metrics. | Understand metadata growth and administrative query activity. |

### Flow, trigger, metric-engine, and client metrics

| Metric family | Meaning | SRE usage |
| --- | --- | --- |
| `greptime_flow_task_count`, `greptime_flow_input_buf_size`, `greptime_flow_processed_rows`, `greptime_flow_processing_time`, `greptime_flow_errors` | Flow task count, input backlog, processed rows, processing latency, and errors. | Diagnose flownode backlog and flow processing failures. |
| `greptime_flow_batching_*` | Flow batching query windows, query time, slow queries, decisions, and mode counts. | Explain slow or stalled batch windows. |
| `greptime_trigger_*` | Trigger count, evaluation latency/failures, alert-send latency/failures, and alert-save latency/failures. | Diagnose alert pipeline health. |
| `greptime_metric_engine_*` | Metric-engine physical/logical region and column counts, DDL latency, forbidden requests, and Mito operation latency. | Diagnose metric-engine cardinality, DDL, and rejected metric requests. |
| `greptime_grpc_*` | Client-side gRPC operation latency/counters for SDK/client paths. | Useful in client-side dashboards or blackbox-style request-path dashboards. |

## Gaps in the current dashboard

1. **No first-class error overview.** Error counters exist, but they are either absent from the overview or buried in component sections. Add a health row with server errors, HTTP/gRPC non-success rates, auth failures, region request failures, query memory rejections, write rejects, flush/compaction failures, OpenDAL errors, meta migration failures, flow errors, and trigger failures.
2. **No explicit capacity/backpressure overview.** CPU/memory panels exist, but request memory, query memory, scan memory, compaction memory, write stalls, pending-row backlog, and runtime idle threads should be visible together.
3. **Metasrv health is not prominent enough.** Heartbeats, inactive regions, lease-expired regions, metadata KV latency, reconciliation errors, and migration failures should sit before deep storage details because they explain availability and routing incidents.
4. **Ingestion paths are incomplete.** The current ingestion panels emphasize table-operator rows, Prometheus remote write, and HTTP logs. Add OTLP metrics/logs/traces, Loki, Elasticsearch, Prometheus store backlog, flush failures, dropped rows, and bulk-insert message size.
5. **Query troubleshooting lacks stage and memory context.** Add query-stage latency, query memory usage/rejections, merge-scan fan-out/errors, pushdown fallback errors, and PromQL series count near protocol latency panels.
6. **Storage-engine failures should be separated from storage-engine work.** Mito work/latency panels are valuable, but failure/backpressure panels should be in a small "Datanode" group so operators do not miss write rejects, stalls, scan rejections, flush failures, compaction failures, and GC errors.
7. **Flush and index drill-down is still thin.** The dashboard should show flush elapsed time, flush bytes/files, in-flight flushes, index create/apply latency, index memory, and index IO so operators can tell whether flush/compaction latency is caused by storage, indexing, or object-store pressure.
8. **Dashboard organization follows components more than incident questions.** Operators typically start with golden signals, then isolate the failing layer. Reorder the top of the dashboard around health and capacity, then keep component deep-dives below.
9. **Panel quality issues should be fixed while editing JSON.** The `Total Query Rate` PromQL used an incorrect PromQL elapsed counter name and should use the `_count` series. Some descriptions also repeat unrelated text or use generic wording; update them when rebuilding the dashboard.

## Proposed dashboard organization

### 1. Overview

Purpose: answer whether the cluster is alive, serving traffic, and failing users.

| Panel | Query sketch | Unit | Notes |
| --- | --- | --- | --- |
| Uptime through data size | Existing uptime, version, traffic totals, error total, restart, topology, resource, row, and size stat panels. | mixed | Keep the compact summary stats at the beginning of the dashboard. |
| Node availability | `sum by (job) (up)` and `up{instance=~"$frontend|$datanode|$metasrv|$flownode"}` | short | In Kubernetes, prefer `pod`/role labels if available. |
| Recent restarts | `changes(process_start_time_seconds{instance=~"$frontend|$datanode|$metasrv|$flownode"}[$__range])` | short | Stat or table sorted descending. |
| User-facing error rate | `sum by (protocol) (rate(greptime_servers_error{instance=~"$frontend"}[$__rate_interval]))` | eps | First row, red threshold above zero for sustained periods. |
| HTTP non-success rate | `sum by (path, method, code) (rate(greptime_servers_http_requests_elapsed_count{instance=~"$frontend",path!~"/health|/metrics",code!~"2.."}[$__rate_interval]))` | reqps | Lets on-call jump directly to failed API paths. |
| gRPC non-success rate | `sum by (path, code) (rate(greptime_servers_grpc_requests_elapsed_count{instance=~"$frontend",code!~"0|OK"}[$__rate_interval]))` | reqps | Adjust code matcher to actual exported gRPC labels. |
| Request p99 | HTTP request p99 plus gRPC request p99. | s | Keep protocol legends low-cardinality in the overview, and leave path/code detail to drill-down panels. |
| Request average | HTTP request average plus gRPC request average from histogram sum/count. | s | Display beside p99 so operators can distinguish broad latency increases from tail-only spikes. |
| Total ingest rate | `sum(rate(greptime_table_operator_ingest_rows[$__rate_interval]))` | rows/s | Existing panel remains useful. |
| Total query rate | `sum(rate(greptime_servers_mysql_query_elapsed_count{instance=~"$frontend"}[$__rate_interval])) + sum(rate(greptime_servers_postgres_query_elapsed_count{instance=~"$frontend"}[$__rate_interval])) + sum(rate(greptime_servers_http_promql_elapsed_count{instance=~"$frontend"}[$__rate_interval]))` | req/s | Fix the existing typo before generating JSON. |
| Ingestion rate | Table operator, Prometheus remote write, logs, Loki, Elasticsearch, and OTLP rates. | rows/s | Put after summary stats and before node availability/request p99 for immediate traffic context. |
| Query rate | MySQL, PostgreSQL, HTTP SQL, PromQL, and frontend gRPC query rates. | req/s | Put after summary stats and before node availability/request p99 for immediate traffic context. |

### 2. Health

Purpose: put all high-severity counters in one place so an SRE can identify the failing subsystem in seconds.

| Panel | Query sketch | Unit | Notes |
| --- | --- | --- | --- |
| Auth failures | `sum by (code) (rate(greptime_servers_auth_failure_count{instance=~"$frontend"}[$__rate_interval]))` | eps | Often customer/configuration issue rather than storage issue. |
| Frontend request memory rejections | `sum(rate(greptime_servers_request_memory_rejected_total{instance=~"$frontend"}[$__rate_interval]))` | rps | User-visible rejection. |
| Query memory rejections | `sum(rate(greptime_query_memory_pool_rejected_total[$__rate_interval]))` | rps | Query execution saturation. |
| Region request failures | `sum by (instance, pod) (rate(greptime_datanode_region_request_fail_count{instance=~"$datanode"}[$__rate_interval]))` | eps | Backend/dataplane failure after routing. |
| Failed region inserts | `sum by (instance, pod) (rate(greptime_datanode_region_failed_insert_count{instance=~"$datanode"}[$__rate_interval]))` | eps | Write path data-plane error. |
| Pending rows flush failures | `sum(rate(greptime_pending_rows_flush_failures{instance=~"$frontend"}[$__rate_interval]))` | eps | Critical for buffered ingestion. |
| Pending rows dropped | `sum(rate(greptime_pending_rows_flush_dropped_rows{instance=~"$frontend"}[$__rate_interval]))` | rows/s | Treat sustained non-zero as data-loss indicator. |
| Mito write rejects/stalls | `sum(rate(greptime_mito_write_reject_total{instance=~"$datanode"}[$__rate_interval]))` and `sum(rate(greptime_mito_write_stall_total{instance=~"$datanode"}[$__rate_interval]))` | eps | Storage-engine backpressure. |
| Flush and compaction failures | `sum(rate(greptime_mito_flush_failure_total{instance=~"$datanode"}[$__rate_interval]))` and `sum(rate(greptime_mito_compaction_failure_total{instance=~"$datanode"}[$__rate_interval]))` | eps | Durable storage and compaction health. |
| Scan/compaction memory rejects | `sum(rate(greptime_mito_scan_requests_rejected_total{instance=~"$datanode"}[$__rate_interval]))` and `sum(rate(greptime_mito_compaction_memory_rejected_total{instance=~"$datanode"}[$__rate_interval]))` | rps | Memory saturation in datanodes. |
| OpenDAL errors | `sum by (scheme, operation, error) (rate(opendal_operation_errors_total{instance=~"$datanode",error!="NotFound"}[$__rate_interval]))` | eps | Object-store/backend filesystem health. |
| Meta migration/reconciliation failures | `sum(rate(greptime_meta_region_migration_fail[$__rate_interval]))` and `sum(rate(greptime_meta_reconciliation_procedure_error[$__rate_interval]))` | eps | Metasrv failure. |
| Flow/trigger failures | `sum by (code) (rate(greptime_flow_errors[$__rate_interval]))`, `sum(rate(greptime_trigger_evaluate_failure_count[$__rate_interval]))` | eps | Derived data and alerting health. |

### 3. Capacity

Purpose: show whether the system is near limits even before errors begin.

| Panel | Query sketch | Unit | Notes |
| --- | --- | --- | --- |
| CPU versus limits by role | Existing `process_cpu_seconds_total` with `greptime_cpu_limit_in_millicores`. | millicores | Keep role-specific panels, but add a top-row max-over-role stat. |
| Memory versus limits by role | Existing `process_resident_memory_bytes` with `greptime_memory_limit_in_bytes`. | bytes | Add threshold when usage exceeds 80% of limit. |
| Runtime thread starvation | `sum by (instance, pod) (greptime_runtime_threads_idle)` and `sum by (instance, pod) (greptime_runtime_threads_alive)` | short | Low idle threads during latency spikes suggests executor saturation. |
| Request memory utilization | `greptime_servers_request_memory_in_use_bytes / greptime_servers_request_memory_limit_bytes` | percent | Frontend rejection precursor. |
| Query memory utilization | `greptime_query_memory_pool_usage_bytes` | bytes | Use alongside query rejections. |
| Scan memory pressure | `sum by (instance, pod) (greptime_mito_scan_memory_usage_bytes{instance=~"$datanode"})` | bytes | Pair with scan exhausted/rejected counters. |
| Compaction memory utilization | `greptime_mito_compaction_memory_in_use_bytes / greptime_mito_compaction_memory_limit_bytes` | percent | Explains compaction queueing/rejections. |
| Write buffer and active stalling | `greptime_mito_write_buffer_bytes{instance=~"$datanode"}` and `greptime_mito_write_stalling_count{instance=~"$datanode"}` | bytes/short | Data-plane backpressure. |
| Prom store backlog | `greptime_prom_store_pending_rows`, `greptime_prom_store_pending_batches`, `greptime_prom_store_pending_workers` | rows/short | Required for remote-write ingestion observation. |
| Inflight flush/compaction | `greptime_mito_inflight_flush_count`, `greptime_mito_inflight_compaction_count` | short | Distinguish backlog from active work. |

### 4. Queries

Purpose: diagnose user-visible traffic after the first health row identifies latency or errors.

| Panel | Query sketch | Unit | Notes |
| --- | --- | --- | --- |
| Query rate by protocol | MySQL, PostgreSQL, PromQL, HTTP SQL, gRPC query rates. | req/s | Include all main query protocols. |
| Query latency by protocol | p50/p95/p99 for MySQL, PostgreSQL, HTTP SQL, PromQL, gRPC. | s | Use p95 plus p99 to reduce noise. |
| Query stage latency | `histogram_quantile(0.99, sum by (le, stage) (rate(greptime_query_stage_elapsed_bucket[$__rate_interval])))` | s | Localize slow stage. |
| Merge scan fan-out and errors | `greptime_query_merge_scan_regions` and `rate(greptime_query_merge_scan_errors_total[$__rate_interval])` | short/eps | Explains high-latency table scans. |
| Pushdown fallback errors | `rate(greptime_push_down_fallback_errors_total[$__rate_interval])` | eps | Optimization failure can increase scan work. |
| PromQL series count | `greptime_promql_series_count` | short | Correlate PromQL latency with cardinality. |
| Connections/prepared statements | `greptime_servers_mysql_connection_count`, `greptime_servers_postgres_connection_count`, `greptime_servers_mysql_prepared_count`, `greptime_servers_postgres_prepared_count` | short | Detect client storms/leaks. |

### 5. Ingestion

Purpose: let users observe how well GreptimeDB is ingesting metrics, logs, traces, and bulk data.

| Panel | Query sketch | Unit | Notes |
| --- | --- | --- | --- |
| Ingest by protocol | Remote write, HTTP logs, Loki logs, Elasticsearch logs, OTLP metrics/logs/traces, table operator rows. | rows/s or docs/s | Put all observability ingestion protocols together. |
| Ingest latency by protocol | `greptime_servers_http_prometheus_write_elapsed`, `greptime_servers_http_logs_ingestion_elapsed`, `greptime_servers_loki_logs_ingestion_elapsed`, `greptime_servers_elasticsearch_logs_ingestion_elapsed`, `greptime_servers_http_otlp_*_elapsed` | s | Separate transform/codec latency where available. |
| Bulk insert message rows/size | `rate(greptime_table_operator_bulk_insert_message_rows[$__rate_interval])`, `rate(greptime_table_operator_bulk_insert_message_size[$__rate_interval])` | rows/s, bytes/s | Explains batch-size-driven latency. |
| Prom store flush pipeline | `greptime_prom_store_flush_total`, `greptime_prom_store_flush_rows`, `greptime_prom_store_flush_elapsed` | ops/s, rows/s, s | Observe remote-write draining. |
| Pending rows backlog/failures | Backlog panels from the saturation and triage groups. | rows/eps | Duplicate as links or repeated compact panels if necessary. |
| OTLP trace failures | `rate(greptime_frontend_otlp_traces_failure_count[$__rate_interval])` | eps | Trace ingestion has an explicit failure counter. |

### 6. Datanode and Storage

Purpose: inspect the storage layer after triage points to datanodes.

| Panel | Query sketch | Unit | Notes |
| --- | --- | --- | --- |
| Region request QPS/latency/failures | Datanode region request elapsed count/bucket plus fail count. | req/s, s, eps | Keep adjacent. |
| Mito request OPS and p99 | Existing `greptime_mito_handle_request_elapsed` panels. | ops/s, s | Keep by request type. |
| Read/write stage p95/p99 | Existing read/write stage latency panels. | s | Include p95 and p99. |
| Flush work and failures | Requests by reason, bytes, files, in-flight jobs, p95/p99 latency by type, failure counter. | ops/s, bytes/s, s, eps | Build a complete flush row. |
| Compaction work and failures | Requests, in-flight, input/output bytes, latency, failures, memory wait/reject. | ops/s, bytes/s, s, eps | Build a complete compaction row. |
| Cache efficiency | `rate(greptime_mito_cache_hit[$__rate_interval]) / (rate(greptime_mito_cache_hit[$__rate_interval]) + rate(greptime_mito_cache_miss[$__rate_interval]))` | percent | More actionable than hit and miss counters alone. |
| Memtable/cardinality pressure | Active series, field builders, dict bytes. | short/bytes | Explains write memory growth. |
| Mito GC health | GC runs, errors, deleted files, orphaned index files, skipped unparsable files, and duration. | ops/s, s, eps, short | Keep GC failures in Health; keep full GC health and a dedicated duration panel in the Datanode row. |

### 7. Index

Purpose: diagnose index creation, index application, and index IO after storage panels point to indexed read/write pressure.

| Panel | Query sketch | Unit | Notes |
| --- | --- | --- | --- |
| Index apply/create latency | `greptime_index_apply_elapsed`, `greptime_index_create_elapsed` | s | Include p95 and p99 by type/stage. |
| Index create throughput | `greptime_index_create_rows_total`, `greptime_index_create_bytes_total` | rows/s, bytes/s | Shows whether index creation is driving storage work. |
| Index memory and IO | `greptime_index_apply_memory_usage`, `greptime_index_create_memory_usage`, `greptime_index_io_bytes_total`, `greptime_index_io_op_total` | bytes, ops/s | Diagnose indexing memory pressure and puffin/intermediate-file IO. |
| Index cache | Existing Mito cache hit/miss/eviction metrics filtered to index-related cache types. | ops/s | Diagnose index metadata/content/result cache effectiveness. |

### 8. Object Store and WAL

Purpose: identify whether storage dependencies are slowing the database.

| Panel | Query sketch | Unit | Notes |
| --- | --- | --- | --- |
| OpenDAL QPS/latency/errors by operation | Existing OpenDAL request and error panels. | req/s, s, eps | Move errors before detailed latency. |
| OpenDAL traffic | Existing traffic panel. | bytes/s | Split read/write if available. |
| WAL fsync p99 | `raft_engine_sync_log_duration_seconds` p99. | s | Local WAL dependency. |
| Log-store op latency and bytes | Existing `greptime_logstore_op_elapsed` and `greptime_logstore_op_bytes_total`. | s, bytes/s | WAL abstraction health. |
| Kafka produce latency/traffic | `greptime_logstore_kafka_client_produce_elapsed`, Kafka bytes/traffic totals. | s, bytes/s | Remote WAL dependency. |
| Remote WAL checkpoint/flush triggers | Existing meta-triggered counters. | ops/s | Confirm maintenance activity. |

### 9. Metasrv

Purpose: diagnose routing, metadata, region health, and automated balancing.

| Panel | Query sketch | Unit | Notes |
| --- | --- | --- | --- |
| Cluster topology and resource counts | Existing Deployment and Database Resources panels. | short | Keep in overview or metasrv group. |
| Inactive regions | `greptime_meta_inactive_regions` | short | Promote near the top. |
| Heartbeat health | Meta heartbeat receive rate/connection count, datanode/frontend heartbeat send/receive, lease elapsed, expired regions. | ops/s, short, s | Required for node/region liveness. |
| Metadata KV latency and RDS SQL latency | Existing meta KV/RDS panels. | s/ms | Keep with metasrv. |
| Region migration state/errors/stage latency | Existing migration panels plus fail counter. | short/eps/s | Make failures obvious. |
| Reconciliation errors/stats | Existing reconciliation panels plus explicit error rate. | eps/short | Metasrv repair health. |
| Hotspot regions and datanode distribution | Existing Hotspot SQL tables and load distribution. | table/bytes | Put after health signals. |
| Autopilot actions/gate stops | Existing balancer and repartition panels. | short | Keep, but annotate gate stops as "why automation did not act". |
| Metasrv GC | `greptime_metasrv_gc_*` | ops/s, s, short | Add cleanup health for failed/candidate regions. |

## Concrete dashboard editing plan

1. **Keep the generated-dashboard workflow.** Update `grafana/dashboards/metrics/cluster/dashboard.json`, then regenerate JSON/YAML/Markdown with `grafana/scripts/gen-dashboards.sh`.
2. **Use concise row titles.** Prefer Overview, Health, Capacity, Ingestion, Queries, Datanode, Storage, Metasrv, and Object Store and WAL.
3. **Keep summary stats first, then time-series ingestion and query rates.** Operators should see compact cluster totals first, then traffic context before node availability and request p99.
4. **Add a `Health` group** immediately after overview. Use mostly rate panels with red thresholds on non-zero critical counters.
5. **Split the current `Resources` group into `Resources` and `Capacity`.** CPU/memory stay in Resources; request/query/scan/compaction memory, write stalling, runtime threads, and pending-row backlog move to Capacity.
6. **Expand `Ingestion`.** Add OTLP, Loki, Elasticsearch, Prometheus store backlog, flush failures, dropped rows, and bulk insert message rows/size.
7. **Expand `Queries`.** Fix the PromQL `_count` typo, add p95 where useful, and add query-stage, merge-scan, pushdown fallback, PromQL series count, and connection/prepared-count panels.
8. **Create a compact `Datanode` row before deep `Storage`.** Include region failures, failed inserts, write reject/stall, flush/compaction failures, scan/compaction memory rejects, and GC errors.
9. **Deepen `Storage`.** Add flush elapsed/throughput panels near Mito flush and compaction panels.
10. **Create an `Index` row.** Move index apply/create/memory/IO/cache panels into a dedicated row after Storage.
11. **Keep metasrv health above object-store details.** Move heartbeat, inactive region, lease-expiry, metadata KV, migration failures, and reconciliation errors before detailed OpenDAL/WAL panels.
12. **Keep existing deep-dive groups but improve descriptions.** Each panel should answer: symptom shown, likely cause, and next drill-down panel.
13. **Collapse deep-dive rows by default.** Keep Overview, Health, Capacity, Ingestion, Queries, and Datanode open; collapse Resources, Frontend Requests, Frontend to Datanode, Storage, Index, Metasrv, Object Store and WAL, Flownode, and Trigger.
14. **Show average request cost beside p99 where histogram sum/count exists.** Use matching labels for numerator and denominator so request, storage, object-store, Metasrv, flow, and trigger panels can compare average and tail latency directly.
15. **Use repeated panels sparingly.** For cluster dashboards, keep role variables (`$frontend`, `$datanode`, `$metasrv`, `$flownode`). For standalone dashboards, remove instance filters as the current script already does.

## Suggested alert seeds

These are not a replacement for environment-specific alerting, but the dashboard should make each condition easy to see.

| Alert seed | Example condition |
| --- | --- |
| User-visible errors | `sum(rate(greptime_servers_error{instance=~"$frontend"}[5m])) > 0` for a sustained window. |
| HTTP/gRPC failures | Non-success request rate above a small threshold for high-traffic paths. |
| Data-loss or durability risk | Pending rows dropped, pending-row flush failures, storage flush failures, or failed inserts greater than zero. |
| Write path backpressure | Write rejects/stalls or write-stalling gauge above zero. |
| Query rejection | Query/request/scan memory rejections greater than zero. |
| Object-store dependency issue | OpenDAL non-`NotFound` errors or p99 latency above an environment-specific threshold. |
| Metasrv liveness | Inactive regions, lease-expired regions, missing heartbeat receive rate, or migration/reconciliation failures. |
| Automation blocked | Balancer/repartition gate stops increasing while hotspots persist. |

## Implementation notes

- Prefer `rate()` for counters and `histogram_quantile()` over `rate(..._bucket[$__rate_interval])` for latency histograms.
- Use `sum by (...)` deliberately to avoid high-cardinality legends in overview rows. High-cardinality labels such as table, region, path, and error should be reserved for drill-down panels or tables.
- When using error-code labels, verify actual exported values before finalizing matchers. For example, gRPC success may be exported as `0` or `OK` depending on instrumentation.
- For standalone dashboards, query sketches that use role variables should be converted by removing the `instance=~"$role"` matcher, matching the existing generation script behavior.
- Prefer p95 and p99 latency panels for troubleshooting. p99 alone can be noisy on low-traffic paths; p95 helps distinguish systemic degradation from isolated outliers.
