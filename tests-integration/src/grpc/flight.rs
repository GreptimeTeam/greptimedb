// Copyright 2023 Greptime Team
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

#[cfg(test)]
mod test {
    use std::net::SocketAddr;
    use std::sync::Arc;
    use std::time::{Duration, Instant};

    use api::v1::auth_header::AuthScheme;
    use api::v1::{Basic, ColumnDataType, ColumnDef, CreateTableExpr, SemanticType};
    use arrow_flight::FlightDescriptor;
    use auth::user_provider_from_option;
    use client::{Client, Database};
    use common_catalog::consts::{DEFAULT_CATALOG_NAME, DEFAULT_SCHEMA_NAME};
    use common_error::ext::ErrorExt;
    use common_error::status_code::StatusCode;
    use common_grpc::flight::do_put::DoPutMetadata;
    use common_grpc::flight::{FlightEncoder, FlightMessage};
    use common_query::OutputData;
    use common_recordbatch::RecordBatch;
    use common_recordbatch::adapter::{RecordBatchMetrics, RegionWatermarkEntry};
    use datatypes::arrow::array::Array;
    use datatypes::arrow_array::StringArray;
    use datatypes::prelude::{ConcreteDataType, ScalarVector, VectorRef};
    use datatypes::schema::{ColumnSchema, Schema};
    use datatypes::vectors::{Int32Vector, StringVector, TimestampMillisecondVector};
    use futures_util::StreamExt;
    use itertools::Itertools;
    use serde_json::Value;
    use servers::grpc::builder::GrpcServerBuilder;
    use servers::grpc::greptime_handler::GreptimeRequestHandler;
    use servers::grpc::{FlightCompression, GrpcServerConfig};
    use servers::server::Server;
    use store_api::storage::RegionId;

    use crate::cluster::GreptimeDbClusterBuilder;
    use crate::grpc::query_and_expect;
    use crate::test_util::{StorageType, setup_grpc_server};
    use crate::tests::test_util::MockInstance;

    #[tokio::test(flavor = "multi_thread")]
    async fn test_standalone_flight_do_put() {
        common_telemetry::init_default_ut_logging();

        let (db, server) =
            setup_grpc_server(StorageType::File, "test_standalone_flight_do_put").await;
        let addr = server.bind_addr().unwrap().to_string();

        let client = Client::with_urls(vec![addr]);
        let client = Database::new_with_dbname("greptime-public", client);

        create_table(&client).await;

        let record_batches = create_record_batches(1);
        test_put_record_batches(&client, record_batches).await;

        let sql = "select ts, a, `B` from foo order by ts";
        let expected = "\
+-------------------------+----+----+
| ts                      | a  | B  |
+-------------------------+----+----+
| 1970-01-01T00:00:00.001 | -1 | s1 |
| 1970-01-01T00:00:00.002 | -2 | s2 |
| 1970-01-01T00:00:00.003 | -3 | s3 |
| 1970-01-01T00:00:00.004 | -4 | s4 |
| 1970-01-01T00:00:00.005 | -5 | s5 |
| 1970-01-01T00:00:00.006 | -6 | s6 |
| 1970-01-01T00:00:00.007 | -7 | s7 |
| 1970-01-01T00:00:00.008 | -8 | s8 |
| 1970-01-01T00:00:00.009 | -9 | s9 |
+-------------------------+----+----+";
        query_and_expect(db.frontend().as_ref(), sql, expected).await;
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_distributed_flight_do_put() {
        common_telemetry::init_default_ut_logging();

        let db = GreptimeDbClusterBuilder::new("test_distributed_flight_do_put")
            .await
            .build(false)
            .await;

        let runtime = common_runtime::global_runtime().clone();
        let greptime_request_handler = GreptimeRequestHandler::new(
            db.frontend.instance.clone(),
            user_provider_from_option("static_user_provider:cmd:greptime_user=greptime_pwd").ok(),
            Some(runtime.clone()),
            FlightCompression::default(),
        );
        let mut grpc_server = GrpcServerBuilder::new(GrpcServerConfig::default(), runtime)
            .flight_handler(Arc::new(greptime_request_handler))
            .build();
        grpc_server
            .start("127.0.0.1:0".parse::<SocketAddr>().unwrap())
            .await
            .unwrap();
        let addr = grpc_server.bind_addr().unwrap().to_string();

        let client = Client::with_urls(vec![addr]);
        let mut client = Database::new(DEFAULT_CATALOG_NAME, DEFAULT_SCHEMA_NAME, client);
        client.set_auth(AuthScheme::Basic(Basic {
            username: "greptime_user".to_string(),
            password: "greptime_pwd".to_string(),
        }));

        create_table(&client).await;

        let record_batches = create_record_batches(1);
        test_put_record_batches(&client, record_batches).await;

        let sql = "select ts, a, `B` from foo order by ts";
        let expected = "\
+-------------------------+----+----+
| ts                      | a  | B  |
+-------------------------+----+----+
| 1970-01-01T00:00:00.001 | -1 | s1 |
| 1970-01-01T00:00:00.002 | -2 | s2 |
| 1970-01-01T00:00:00.003 | -3 | s3 |
| 1970-01-01T00:00:00.004 | -4 | s4 |
| 1970-01-01T00:00:00.005 | -5 | s5 |
| 1970-01-01T00:00:00.006 | -6 | s6 |
| 1970-01-01T00:00:00.007 | -7 | s7 |
| 1970-01-01T00:00:00.008 | -8 | s8 |
| 1970-01-01T00:00:00.009 | -9 | s9 |
+-------------------------+----+----+";
        query_and_expect(db.fe_instance().as_ref(), sql, expected).await;

        let output = client.sql(sql).await.unwrap();
        let OutputData::Stream(mut stream) = output.data else {
            panic!("expected stream output");
        };
        while let Some(batch) = stream.next().await {
            batch.unwrap();
        }
        let metrics = stream.metrics().expect("expected terminal metrics");
        assert!(metrics.region_watermarks.is_empty());

        let result = client
            .sql_with_terminal_metrics(sql, &[("flow.return_region_seq", "true")])
            .await
            .unwrap();
        let terminal_metrics = result.metrics.clone();
        let OutputData::Stream(mut stream) = result.output.data else {
            panic!("expected stream output");
        };
        while let Some(batch) = stream.next().await {
            batch.unwrap();
        }
        let regions = db.list_all_regions().await;
        assert_eq!(regions.len(), 1);
        let (region_id, region) = regions.into_iter().next().unwrap();
        let expected_watermark = (region_id.as_u64(), region.find_committed_sequence());
        assert_eq!(
            terminal_metrics.region_watermark_map(),
            Some(std::collections::HashMap::from([expected_watermark]))
        );

        let output = client
            .sql_with_hint(sql, &[("flow.return_region_seq", "true")])
            .await
            .unwrap();
        let OutputData::Stream(mut stream) = output.data else {
            panic!("expected stream output");
        };

        let mut row_count = 0;
        while let Some(batch) = stream.next().await {
            let batch = batch.unwrap();
            row_count += batch.num_rows();
        }
        assert_eq!(row_count, 9);

        let metrics = stream.metrics().expect("expected terminal metrics");
        let region_watermarks = metrics.region_watermarks;
        assert_eq!(
            region_watermarks,
            vec![RegionWatermarkEntry {
                region_id: expected_watermark.0,
                watermark: Some(expected_watermark.1),
            }]
        );

        let previous_watermark = expected_watermark;

        create_table_named(&client, "bar").await;
        let result = client
            .sql_with_terminal_metrics("insert into bar select ts, a, `B` from foo", &[])
            .await
            .unwrap();
        let OutputData::AffectedRows(affected_rows) = result.output.data else {
            panic!("expected affected rows output");
        };
        assert_eq!(affected_rows, 9);
        assert!(result.metrics.is_ready());
        assert!(result.region_watermark_map().is_none());

        let err = client
            .sql_with_terminal_metrics(
                "insert into bar select ts, a, `B` from foo",
                &[("flow.return_region_seq", "not-a-bool")],
            )
            .await
            .unwrap_err();
        let err_msg = format!("{err:?}");
        assert!(err_msg.contains("Invalid value for flow.return_region_seq"));

        client.sql("truncate table bar").await.unwrap();

        let result = client
            .sql_with_terminal_metrics(
                "insert into bar select ts, a, `B` from foo",
                &[("flow.return_region_seq", "true")],
            )
            .await
            .unwrap();
        let OutputData::AffectedRows(affected_rows) = result.output.data else {
            panic!("expected affected rows output");
        };
        assert_eq!(affected_rows, 9);
        assert_eq!(
            result.region_watermark_map(),
            Some(std::collections::HashMap::from([previous_watermark]))
        );

        let incremental_batches = create_record_batches(10);
        test_put_record_batches(&client, incremental_batches).await;

        let output = client
            .sql_with_hint(
                sql,
                &[
                    ("flow.incremental_mode", "memtable_only"),
                    (
                        "flow.incremental_after_seqs",
                        &format!(
                            r#"{{"{}":"{}"}}"#,
                            previous_watermark.0, previous_watermark.1
                        ),
                    ),
                    ("flow.return_region_seq", "true"),
                ],
            )
            .await
            .unwrap();
        let OutputData::Stream(mut stream) = output.data else {
            panic!("expected stream output");
        };

        let schema = stream.schema();
        let mut rows = Vec::new();
        while let Some(batch) = stream.next().await {
            rows.push(batch.unwrap());
        }
        let pretty = common_recordbatch::RecordBatches::try_new(schema, rows)
            .unwrap()
            .pretty_print()
            .unwrap();
        assert_eq!(
            pretty,
            "\
+-------------------------+-----+-----+
| ts                      | a   | B   |
+-------------------------+-----+-----+
| 1970-01-01T00:00:00.010 | -10 | s10 |
| 1970-01-01T00:00:00.011 | -11 | s11 |
| 1970-01-01T00:00:00.012 | -12 | s12 |
| 1970-01-01T00:00:00.013 | -13 | s13 |
| 1970-01-01T00:00:00.014 | -14 | s14 |
| 1970-01-01T00:00:00.015 | -15 | s15 |
| 1970-01-01T00:00:00.016 | -16 | s16 |
| 1970-01-01T00:00:00.017 | -17 | s17 |
| 1970-01-01T00:00:00.018 | -18 | s18 |
+-------------------------+-----+-----+"
        );

        let RecordBatchMetrics {
            region_watermarks, ..
        } = stream
            .metrics()
            .expect("expected terminal incremental metrics");
        let regions = db.list_all_regions().await;
        let region = regions
            .get(&RegionId::from_u64(previous_watermark.0))
            .expect("expected source region to exist");
        let expected_incremental_watermark =
            (previous_watermark.0, region.find_committed_sequence());
        assert_eq!(
            region_watermarks,
            vec![RegionWatermarkEntry {
                region_id: expected_incremental_watermark.0,
                watermark: Some(expected_incremental_watermark.1),
            }]
        );

        client.sql("admin flush_table('foo')").await.unwrap();

        let output = client
            .sql_with_hint(
                sql,
                &[
                    ("flow.incremental_mode", "memtable_only"),
                    (
                        "flow.incremental_after_seqs",
                        &format!(
                            r#"{{"{}":"{}"}}"#,
                            previous_watermark.0, previous_watermark.1
                        ),
                    ),
                    ("flow.return_region_seq", "true"),
                ],
            )
            .await
            .unwrap();
        let OutputData::Stream(mut stream) = output.data else {
            panic!("expected stream output");
        };
        let err = loop {
            match stream.next().await {
                Some(Err(err)) => break err,
                Some(Ok(_)) => continue,
                None => panic!("expected stale incremental query to fail while consuming stream"),
            }
        };
        assert_eq!(err.status_code(), StatusCode::EngineExecuteQuery);
        let err_msg = format!("{err:?}");
        assert!(err_msg.contains("STALE_CURSOR"));
        assert!(err_msg.contains(&previous_watermark.0.to_string()));
        assert!(err_msg.contains(&previous_watermark.1.to_string()));
    }

    #[derive(Debug, Clone, Copy)]
    struct ManualIncrementalBenchmarkCase {
        label: &'static str,
        initial_hosts: usize,
        initial_rows_per_host: usize,
        delta_existing_hosts: usize,
        delta_rows_per_host: usize,
        include_new_delta_host: bool,
        iterations: usize,
    }

    #[derive(Debug)]
    struct QueryExecutionSummary {
        pretty: String,
        elapsed: Duration,
        row_count: usize,
        region_watermarks: Vec<RegionWatermarkEntry>,
        region_watermark_map: Option<std::collections::HashMap<u64, u64>>,
    }

    #[derive(Debug)]
    struct ExplainQuerySummary {
        pretty: String,
        row_count: usize,
        plan_texts: Vec<String>,
        json_nodes: Vec<ExplainNodeSummary>,
    }

    #[derive(Debug, Clone)]
    struct ExplainNodeSummary {
        name: String,
        param: String,
        output_rows: usize,
        elapsed_compute: usize,
    }

    #[tokio::test(flavor = "multi_thread")]
    #[ignore = "benchmark-style integration test for Task 4b"]
    async fn test_standalone_flight_manual_incremental_join_benchmark() {
        common_telemetry::init_default_ut_logging();

        let (db, server) = setup_grpc_server(
            StorageType::File,
            "test_standalone_flight_manual_incremental_join_benchmark",
        )
        .await;
        let addr = server.bind_addr().unwrap().to_string();

        let client = Client::with_urls(vec![addr]);
        let client = Database::new_with_dbname("greptime-public", client);

        let cases = [
            ManualIncrementalBenchmarkCase {
                label: "small_delta_large_sink",
                initial_hosts: 128,
                initial_rows_per_host: 6,
                delta_existing_hosts: 4,
                delta_rows_per_host: 2,
                include_new_delta_host: true,
                iterations: 3,
            },
            ManualIncrementalBenchmarkCase {
                label: "medium_delta_large_sink",
                initial_hosts: 128,
                initial_rows_per_host: 6,
                delta_existing_hosts: 32,
                delta_rows_per_host: 2,
                include_new_delta_host: true,
                iterations: 3,
            },
            ManualIncrementalBenchmarkCase {
                label: "small_delta_small_sink",
                initial_hosts: 16,
                initial_rows_per_host: 6,
                delta_existing_hosts: 4,
                delta_rows_per_host: 2,
                include_new_delta_host: true,
                iterations: 3,
            },
            ManualIncrementalBenchmarkCase {
                label: "tiny_delta_huge_sink",
                initial_hosts: 1024,
                initial_rows_per_host: 8,
                delta_existing_hosts: 2,
                delta_rows_per_host: 1,
                include_new_delta_host: true,
                iterations: 2,
            },
            ManualIncrementalBenchmarkCase {
                label: "tiny_delta_src_heavy_sink_small",
                initial_hosts: 16,
                initial_rows_per_host: 1024,
                delta_existing_hosts: 2,
                delta_rows_per_host: 1,
                include_new_delta_host: true,
                iterations: 2,
            },
            ManualIncrementalBenchmarkCase {
                label: "tiny_delta_src_huge_sink_mid",
                initial_hosts: 128,
                initial_rows_per_host: 512,
                delta_existing_hosts: 2,
                delta_rows_per_host: 1,
                include_new_delta_host: true,
                iterations: 2,
            },
            ManualIncrementalBenchmarkCase {
                label: "tiny_delta_src_extreme_sink_mid",
                initial_hosts: 256,
                initial_rows_per_host: 1024,
                delta_existing_hosts: 2,
                delta_rows_per_host: 1,
                include_new_delta_host: true,
                iterations: 2,
            },
            ManualIncrementalBenchmarkCase {
                label: "small_delta_src_ultra_sink_mid",
                initial_hosts: 256,
                initial_rows_per_host: 4096,
                delta_existing_hosts: 20,
                delta_rows_per_host: 100,
                include_new_delta_host: true,
                iterations: 2,
            },
        ];

        for case in cases {
            run_manual_incremental_join_benchmark_case(&db, &client, case).await;
        }
    }

    async fn run_manual_incremental_join_benchmark_case(
        db: &impl MockInstance,
        client: &Database,
        case: ManualIncrementalBenchmarkCase,
    ) {
        let source_table = format!("flow_bench_src_{}", case.label);
        let sink_table = format!("flow_bench_sink_{}", case.label);

        create_flow_bench_source_table(client, &source_table).await;
        create_flow_bench_sink_table(client, &sink_table).await;

        let initial_rows = make_source_rows(
            &source_table,
            0,
            case.initial_hosts,
            case.initial_rows_per_host,
            1,
        );
        insert_source_rows(client, &source_table, &initial_rows).await;

        let source_table_id = db
            .frontend()
            .catalog_manager()
            .table("greptime", "public", &source_table, None)
            .await
            .unwrap()
            .unwrap()
            .table_info()
            .table_id();
        let previous_watermarks = std::collections::HashMap::from([(
            RegionId::new(source_table_id, 0).as_u64(),
            initial_rows.len() as u64,
        )]);

        execute_affected_rows_sql(
            client,
            &format!(
                "insert into {sink_table} select host, cast(sum(v) as bigint), max(ts) from {source_table} group by host"
            ),
        )
        .await;

        let sink_table_id = db
            .frontend()
            .catalog_manager()
            .table("greptime", "public", &sink_table, None)
            .await
            .unwrap()
            .unwrap()
            .table_info()
            .table_id();

        let delta_rows = make_delta_rows(
            case,
            (case.initial_hosts * case.initial_rows_per_host) as i64 + 1,
        );
        insert_source_rows(client, &source_table, &delta_rows).await;

        let expected_summary =
            execute_stream_query_with_hints(client, &full_aggregate_query(&source_table), &[])
                .await;
        let expected = expected_summary.pretty.clone();
        let expected_row_count = expected_summary.row_count;
        let changed_hosts = delta_rows
            .iter()
            .map(|(host, _, _)| host.clone())
            .unique()
            .collect_vec();
        let expected_changed_summary = execute_stream_query_with_hints(
            client,
            &changed_rows_full_baseline_query(&source_table, &changed_hosts),
            &[],
        )
        .await;
        let expected_changed = expected_changed_summary.pretty.clone();
        let expected_changed_row_count = expected_changed_summary.row_count;

        let incremental_after_seqs = format_region_seq_map(&previous_watermarks);
        let sink_table_id_str = sink_table_id.to_string();
        let hint_pairs = [
            ("flow.incremental_mode", "memtable_only".to_string()),
            (
                "flow.incremental_after_seqs",
                incremental_after_seqs.clone(),
            ),
            ("flow.return_region_seq", "true".to_string()),
            ("flow.sink_table_id", sink_table_id_str.clone()),
        ];
        let hints = hint_pairs
            .iter()
            .map(|(k, v)| (*k, v.as_str()))
            .collect::<Vec<_>>();

        let mut full_latencies = Vec::with_capacity(case.iterations);
        let mut delta_only_left_join_latencies = Vec::with_capacity(case.iterations);

        if matches!(
            case.label,
            "tiny_delta_src_extreme_sink_mid" | "small_delta_src_ultra_sink_mid"
        ) {
            let full_verbose = execute_explain_query_with_hints(
                client,
                &format!(
                    "EXPLAIN ANALYZE VERBOSE {}",
                    full_aggregate_query(&source_table)
                ),
                &[],
            )
            .await;
            let delta_only_verbose = execute_explain_query_with_hints(
                client,
                &format!(
                    "EXPLAIN ANALYZE VERBOSE {}",
                    manual_delta_only_left_join_update_query(&source_table, &sink_table)
                ),
                &hints,
            )
            .await;
            let full_json = execute_explain_query_with_hints(
                client,
                &format!(
                    "EXPLAIN ANALYZE FORMAT JSON {}",
                    full_aggregate_query(&source_table)
                ),
                &[],
            )
            .await;
            let delta_only_json = execute_explain_query_with_hints(
                client,
                &format!(
                    "EXPLAIN ANALYZE FORMAT JSON {}",
                    manual_delta_only_left_join_update_query(&source_table, &sink_table)
                ),
                &hints,
            )
            .await;

            assert!(
                full_verbose.pretty.contains("AggregateExec")
                    || full_verbose.pretty.contains("HashAggregateExec")
            );
            assert!(
                delta_only_verbose.pretty.contains("HashJoinExec")
                    || delta_only_verbose.pretty.contains("Left")
            );
            assert!(full_verbose.pretty.contains("SeqScan"));
            assert!(delta_only_verbose.pretty.contains("SeqScan"));
            assert!(full_json.pretty.contains("elapsed_compute"));
            assert!(delta_only_json.pretty.contains("elapsed_compute"));
            assert!(
                full_json
                    .json_nodes
                    .iter()
                    .any(|node| node.name == "AggregateExec"),
                "expected full explain json to include AggregateExec nodes"
            );
            assert!(
                full_json
                    .json_nodes
                    .iter()
                    .any(|node| node.name == "SeqScan"),
                "expected full explain json to include SeqScan nodes"
            );
            assert!(
                delta_only_json
                    .json_nodes
                    .iter()
                    .any(|node| node.name == "HashJoinExec"),
                "expected delta-only explain json to include HashJoinExec nodes"
            );
            assert!(
                delta_only_json
                    .json_nodes
                    .iter()
                    .filter(|node| node.name == "SeqScan")
                    .count()
                    >= 2,
                "expected delta-only explain json to include both source and sink SeqScan nodes"
            );

            common_telemetry::info!(
                "FlowBench profile case={} full_verbose_rows={} delta_only_verbose_rows={} full_json_rows={} delta_only_json_rows={} full_json_plans={} delta_only_json_plans={}\nFULL_VERBOSE\n{}\nDELTA_ONLY_VERBOSE\n{}\nFULL_JSON\n{}\nDELTA_ONLY_JSON\n{}\nFULL_JSON_NODE_SUMMARY={:?}\nDELTA_ONLY_JSON_NODE_SUMMARY={:?}",
                case.label,
                full_verbose.row_count,
                delta_only_verbose.row_count,
                full_json.row_count,
                delta_only_json.row_count,
                full_json.plan_texts.len(),
                delta_only_json.plan_texts.len(),
                full_verbose.pretty,
                delta_only_verbose.pretty,
                full_json.pretty,
                delta_only_json.pretty,
                summarize_explain_nodes(&full_json.json_nodes),
                summarize_explain_nodes(&delta_only_json.json_nodes),
            );
        }

        for _ in 0..case.iterations {
            let full_summary =
                execute_stream_query_with_hints(client, &full_aggregate_query(&source_table), &[])
                    .await;
            let changed_rows_full_summary = execute_stream_query_with_hints(
                client,
                &changed_rows_full_baseline_query(&source_table, &changed_hosts),
                &[],
            )
            .await;
            let delta_only_left_join_summary = execute_stream_query_with_hints(
                client,
                &manual_delta_only_left_join_update_query(&source_table, &sink_table),
                &hints,
            )
            .await;

            assert_eq!(full_summary.pretty, expected);
            assert_eq!(changed_rows_full_summary.pretty, expected_changed);
            assert_eq!(delta_only_left_join_summary.pretty, expected_changed);
            assert_eq!(
                changed_rows_full_summary.row_count,
                expected_changed_row_count
            );
            assert_eq!(
                delta_only_left_join_summary.row_count,
                expected_changed_row_count
            );
            assert!(
                delta_only_left_join_summary
                    .region_watermark_map
                    .as_ref()
                    .map(|watermarks| !watermarks.is_empty())
                    .unwrap_or(false)
                    || !delta_only_left_join_summary.region_watermarks.is_empty(),
                "expected delta-only left-join incremental query to return region watermark metrics"
            );

            full_latencies.push(full_summary.elapsed);
            delta_only_left_join_latencies.push(delta_only_left_join_summary.elapsed);
        }

        common_telemetry::info!(
            "FlowBench case={} full_latencies_ms={:?} delta_only_left_join_latencies_ms={:?} expected_rows={} expected_changed_rows={}",
            case.label,
            full_latencies.iter().map(Duration::as_millis).collect_vec(),
            delta_only_left_join_latencies
                .iter()
                .map(Duration::as_millis)
                .collect_vec(),
            expected_row_count,
            expected_changed_row_count,
        );

        client
            .sql(&format!("admin flush_table('{source_table}')"))
            .await
            .unwrap();

        let output = client
            .sql_with_hint(
                &manual_delta_only_left_join_update_query(&source_table, &sink_table),
                &hints,
            )
            .await
            .unwrap();
        let OutputData::Stream(mut stream) = output.data else {
            panic!("expected stale delta-only benchmark query to return stream output");
        };
        let err = loop {
            match stream.next().await {
                Some(Err(err)) => break err,
                Some(Ok(_)) => continue,
                None => panic!("expected stale delta-only incremental benchmark query to fail"),
            }
        };
        assert_eq!(err.status_code(), StatusCode::EngineExecuteQuery);
        let err_msg = format!("{err:?}");
        assert!(err_msg.contains("STALE_CURSOR"));
    }

    async fn execute_stream_query_with_hints(
        client: &Database,
        sql: &str,
        hints: &[(&str, &str)],
    ) -> QueryExecutionSummary {
        let started = Instant::now();
        let result = client.sql_with_terminal_metrics(sql, hints).await.unwrap();
        let region_watermark_map = result.region_watermark_map();

        let OutputData::Stream(mut stream) = result.output.data else {
            panic!("expected stream output for benchmark query");
        };

        let schema = stream.schema();
        let mut row_count = 0;
        let mut rows = Vec::new();
        while let Some(batch) = stream.next().await {
            let batch = batch.unwrap();
            row_count += batch.num_rows();
            rows.push(batch);
        }
        let pretty = common_recordbatch::RecordBatches::try_new(schema, rows)
            .unwrap()
            .pretty_print()
            .unwrap();
        let elapsed = started.elapsed();
        let region_watermarks = stream
            .metrics()
            .map(|metrics| metrics.region_watermarks)
            .unwrap_or_default();
        let region_watermark_map = region_watermark_map.or_else(|| {
            let region_seq_pairs = region_watermarks
                .iter()
                .map(|entry| {
                    entry
                        .watermark
                        .map(|watermark| (entry.region_id, watermark))
                })
                .collect::<Option<Vec<_>>>()?;
            Some(std::collections::HashMap::from_iter(region_seq_pairs))
        });

        QueryExecutionSummary {
            pretty,
            elapsed,
            row_count,
            region_watermarks,
            region_watermark_map,
        }
    }

    async fn execute_explain_query_with_hints(
        client: &Database,
        sql: &str,
        hints: &[(&str, &str)],
    ) -> ExplainQuerySummary {
        let output = client.sql_with_hint(sql, hints).await.unwrap();
        let OutputData::Stream(mut stream) = output.data else {
            panic!("expected stream output for explain query");
        };

        let schema = stream.schema();
        let mut row_count = 0;
        let mut rows = Vec::new();
        while let Some(batch) = stream.next().await {
            let batch = batch.unwrap();
            row_count += batch.num_rows();
            rows.push(batch);
        }
        let plan_texts = extract_explain_plan_texts(&rows);
        let pretty = common_recordbatch::RecordBatches::try_new(schema, rows)
            .unwrap()
            .pretty_print()
            .unwrap();
        let json_nodes = plan_texts
            .iter()
            .filter_map(|plan| serde_json::from_str::<Value>(plan).ok())
            .flat_map(|value| flatten_explain_json_nodes(&value))
            .collect_vec();

        ExplainQuerySummary {
            pretty,
            row_count,
            plan_texts,
            json_nodes,
        }
    }

    fn extract_explain_plan_texts(rows: &[RecordBatch]) -> Vec<String> {
        let mut plan_texts = Vec::new();
        for batch in rows {
            let Some(plan_column) = batch.columns().get(2) else {
                continue;
            };
            let Some(plan_array) = plan_column.as_any().downcast_ref::<StringArray>() else {
                continue;
            };
            for idx in 0..plan_array.len() {
                if plan_array.is_valid(idx) {
                    plan_texts.push(plan_array.value(idx).to_string());
                }
            }
        }
        plan_texts
    }

    fn flatten_explain_json_nodes(value: &Value) -> Vec<ExplainNodeSummary> {
        let mut nodes = Vec::new();
        flatten_explain_json_nodes_into(value, &mut nodes);
        nodes
    }

    fn flatten_explain_json_nodes_into(value: &Value, nodes: &mut Vec<ExplainNodeSummary>) {
        let name = value
            .get("name")
            .and_then(Value::as_str)
            .unwrap_or_default()
            .to_string();
        let param = value
            .get("param")
            .and_then(Value::as_str)
            .unwrap_or_default()
            .to_string();
        let output_rows = value
            .get("output_rows")
            .and_then(Value::as_u64)
            .unwrap_or_default() as usize;
        let elapsed_compute = value
            .get("elapsed_compute")
            .and_then(Value::as_u64)
            .unwrap_or_default() as usize;

        if !name.is_empty() {
            nodes.push(ExplainNodeSummary {
                name,
                param,
                output_rows,
                elapsed_compute,
            });
        }

        if let Some(children) = value.get("children").and_then(Value::as_array) {
            for child in children {
                flatten_explain_json_nodes_into(child, nodes);
            }
        }
    }

    fn summarize_explain_nodes(nodes: &[ExplainNodeSummary]) -> Vec<String> {
        nodes
            .iter()
            .map(|node| {
                format!(
                    "{} rows={} elapsed_ns={} param={}",
                    node.name, node.output_rows, node.elapsed_compute, node.param
                )
            })
            .collect()
    }

    async fn execute_affected_rows_sql(client: &Database, sql: &str) {
        let output = client.sql(sql).await.unwrap();
        let OutputData::AffectedRows(_) = output.data else {
            panic!("expected affected rows output for sql: {sql}");
        };
    }

    async fn create_flow_bench_source_table(client: &Database, table_name: &str) {
        execute_affected_rows_sql(
            client,
            &format!(
                "create table {table_name}(host string, v bigint, ts timestamp time index, primary key(host))"
            ),
        )
        .await;
    }

    async fn create_flow_bench_sink_table(client: &Database, table_name: &str) {
        execute_affected_rows_sql(
            client,
            &format!(
                "create table {table_name}(host string, total bigint, ts timestamp time index, primary key(host))"
            ),
        )
        .await;
    }

    async fn insert_source_rows(client: &Database, table_name: &str, rows: &[(String, i64, i64)]) {
        for chunk in &rows.iter().chunks(128) {
            let values = chunk
                .map(|(host, value, ts)| format!("('{host}', {value}, {ts})"))
                .join(",");
            execute_affected_rows_sql(
                client,
                &format!("insert into {table_name}(host, v, ts) values {values}"),
            )
            .await;
        }
    }

    fn make_source_rows(
        _table_name: &str,
        ts_start: i64,
        host_count: usize,
        rows_per_host: usize,
        value_bias: i64,
    ) -> Vec<(String, i64, i64)> {
        let mut rows = Vec::with_capacity(host_count * rows_per_host);
        let mut ts = ts_start;
        for host_idx in 0..host_count {
            for row_idx in 0..rows_per_host {
                ts += 1;
                rows.push((
                    format!("host_{host_idx:03}"),
                    value_bias + host_idx as i64 + row_idx as i64,
                    ts,
                ));
            }
        }
        rows
    }

    fn make_delta_rows(
        case: ManualIncrementalBenchmarkCase,
        ts_start: i64,
    ) -> Vec<(String, i64, i64)> {
        let mut rows = Vec::with_capacity(
            case.delta_existing_hosts * case.delta_rows_per_host
                + usize::from(case.include_new_delta_host) * case.delta_rows_per_host,
        );
        let mut ts = ts_start;

        for host_idx in 0..case.delta_existing_hosts {
            for row_idx in 0..case.delta_rows_per_host {
                ts += 1;
                rows.push((
                    format!("host_{host_idx:03}"),
                    1_000 + host_idx as i64 + row_idx as i64,
                    ts,
                ));
            }
        }

        if case.include_new_delta_host {
            for row_idx in 0..case.delta_rows_per_host {
                ts += 1;
                rows.push((
                    format!("host_new_{}", case.label),
                    2_000 + row_idx as i64,
                    ts,
                ));
            }
        }

        rows
    }

    fn full_aggregate_query(source_table: &str) -> String {
        format!(
            "select host, cast(sum(v) as bigint) as total from {source_table} group by host order by host"
        )
    }

    fn changed_rows_full_baseline_query(source_table: &str, changed_hosts: &[String]) -> String {
        let host_list = changed_hosts
            .iter()
            .map(|host| format!("'{}'", host))
            .join(",");
        format!(
            "select host, cast(sum(v) as bigint) as total from {source_table} where host in ({host_list}) group by host order by host"
        )
    }

    fn manual_delta_only_left_join_update_query(source_table: &str, sink_table: &str) -> String {
        format!(
            "with delta as (select host, cast(sum(v) as bigint) as delta_total from {source_table} group by host) \
             select d.host as host, cast(coalesce(s.total, 0) + d.delta_total as bigint) as total \
             from delta d left join {sink_table} s on d.host = s.host \
             order by host"
        )
    }

    fn format_region_seq_map(region_seqs: &std::collections::HashMap<u64, u64>) -> String {
        let mut entries = region_seqs
            .iter()
            .map(|(region_id, seq)| format!(r#""{region_id}":"{seq}""#))
            .collect::<Vec<_>>();
        entries.sort();
        format!("{{{}}}", entries.join(","))
    }

    async fn test_put_record_batches(client: &Database, record_batches: Vec<RecordBatch>) {
        let requests_count = record_batches.len();
        let schema = record_batches[0].schema.arrow_schema().clone();

        let stream = futures::stream::once(async move {
            let mut schema_data = FlightEncoder::default().encode_schema(schema.as_ref());
            let metadata = DoPutMetadata::new(0);
            schema_data.app_metadata = serde_json::to_vec(&metadata).unwrap().into();
            // first message in "DoPut" stream should carry table name in flight descriptor
            schema_data.flight_descriptor = Some(FlightDescriptor {
                r#type: arrow_flight::flight_descriptor::DescriptorType::Path as i32,
                path: vec!["foo".to_string()],
                ..Default::default()
            });
            schema_data
        })
        .chain(
            tokio_stream::iter(record_batches)
                .enumerate()
                .flat_map(|(i, x)| {
                    let mut encoder = FlightEncoder::default();
                    let message = FlightMessage::RecordBatch(x.into_df_record_batch());
                    let mut data = encoder.encode(message);
                    let metadata = DoPutMetadata::new((i + 1) as i64);
                    data.iter_mut().for_each(|x| {
                        x.app_metadata = serde_json::to_vec(&metadata).unwrap().into()
                    });
                    tokio_stream::iter(data)
                })
                .boxed(),
        )
        .boxed();

        let response_stream = client.do_put(stream).await.unwrap();

        let responses = response_stream.collect::<Vec<_>>().await;
        let responses_count = responses.len();
        for (i, response) in responses.into_iter().enumerate() {
            assert!(response.is_ok(), "{}", response.err().unwrap());
            let response = response.unwrap();
            assert_eq!(response.request_id(), i as i64);
            if i == 0 {
                // the first is schema
                assert_eq!(response.affected_rows(), 0);
            } else {
                assert_eq!(response.affected_rows(), 3);
            }
        }
        assert_eq!(requests_count + 1, responses_count);
    }

    fn create_record_batches(start: i64) -> Vec<RecordBatch> {
        let schema = Arc::new(Schema::new(vec![
            ColumnSchema::new(
                "ts",
                ConcreteDataType::timestamp_millisecond_datatype(),
                false,
            )
            .with_time_index(true),
            ColumnSchema::new("a", ConcreteDataType::int32_datatype(), false),
            ColumnSchema::new("B", ConcreteDataType::string_datatype(), true),
        ]));

        let mut record_batches = Vec::with_capacity(3);
        for chunk in &(start..start + 9).chunks(3) {
            let vs = chunk.collect_vec();
            let x1 = vs[0];
            let x2 = vs[1];
            let x3 = vs[2];

            record_batches.push(
                RecordBatch::new(
                    schema.clone(),
                    vec![
                        Arc::new(TimestampMillisecondVector::from_vec(vec![x1, x2, x3]))
                            as VectorRef,
                        Arc::new(Int32Vector::from_vec(vec![
                            -x1 as i32, -x2 as i32, -x3 as i32,
                        ])),
                        Arc::new(StringVector::from_vec(vec![
                            format!("s{x1}"),
                            format!("s{x2}"),
                            format!("s{x3}"),
                        ])),
                    ],
                )
                .unwrap(),
            );
        }
        record_batches
    }

    async fn create_table(client: &Database) {
        create_table_named(client, "foo").await;
    }

    async fn create_table_named(client: &Database, table_name: &str) {
        // create table foo (
        //   ts timestamp time index,
        //   a int primary key,
        //   b string,
        // )
        let output = client
            .create(CreateTableExpr {
                schema_name: "public".to_string(),
                table_name: table_name.to_string(),
                column_defs: vec![
                    ColumnDef {
                        name: "ts".to_string(),
                        data_type: ColumnDataType::TimestampMillisecond as i32,
                        semantic_type: SemanticType::Timestamp as i32,
                        is_nullable: false,
                        ..Default::default()
                    },
                    ColumnDef {
                        name: "a".to_string(),
                        data_type: ColumnDataType::Int32 as i32,
                        semantic_type: SemanticType::Tag as i32,
                        is_nullable: false,
                        ..Default::default()
                    },
                    ColumnDef {
                        name: "B".to_string(),
                        data_type: ColumnDataType::String as i32,
                        semantic_type: SemanticType::Field as i32,
                        is_nullable: true,
                        ..Default::default()
                    },
                ],
                time_index: "ts".to_string(),
                primary_keys: vec!["a".to_string()],
                engine: "mito".to_string(),
                ..Default::default()
            })
            .await
            .unwrap();
        let OutputData::AffectedRows(affected_rows) = output.data else {
            unreachable!()
        };
        assert_eq!(affected_rows, 0);
    }
}
