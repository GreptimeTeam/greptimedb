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
mod tests {
    use std::sync::Arc;

    use api::prom_store::remote::label_matcher::Type as MatcherType;
    use api::prom_store::remote::{
        Label, LabelMatcher, Query, ReadRequest, ReadResponse, Sample, TimeSeries, WriteRequest,
    };
    use api::v1::value::ValueData;
    use api::v1::{
        ColumnDataType, Row, RowInsertRequest, RowInsertRequests, Rows, SemanticType, Value,
    };
    use client::OutputData;
    use common_catalog::consts::DEFAULT_CATALOG_NAME;
    use datatypes::arrow::array::AsArray;
    use datatypes::arrow::datatypes::{
        DataType, TimeUnit, TimestampMicrosecondType, TimestampMillisecondType,
        TimestampNanosecondType, TimestampSecondType,
    };
    use frontend::instance::Instance;
    use prost::Message;
    use servers::http::prom_store::PHYSICAL_TABLE_PARAM;
    use servers::http::test_helpers::{TestClient, TestResponse};
    use servers::http::{HttpOptions, HttpServerBuilder};
    use servers::prom_store;
    use servers::prom_store::to_grpc_row_insert_requests;
    use servers::query_handler::PromStoreProtocolHandler;
    use servers::query_handler::sql::SqlQueryHandler;
    use session::context::QueryContext;

    use crate::standalone::GreptimeDbStandaloneBuilder;
    use crate::tests;

    #[tokio::test(flavor = "multi_thread")]
    async fn test_standalone_prom_store_remote_rw_default_physical_table() {
        common_telemetry::init_default_ut_logging();
        let standalone = GreptimeDbStandaloneBuilder::new(
            "test_standalone_prom_store_remote_rw_default_physical_table",
        )
        .build()
        .await;
        let instance = standalone.fe_instance();

        test_prom_store_remote_rw(instance, None).await;
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_distributed_prom_store_remote_rw_default_physical_table() {
        common_telemetry::init_default_ut_logging();
        let distributed = tests::create_distributed_instance(
            "test_distributed_prom_store_remote_rw_default_physical_table",
        )
        .await;
        test_prom_store_remote_rw(&distributed.frontend(), None).await;
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_standalone_prom_store_remote_rw_custom_physical_table() {
        common_telemetry::init_default_ut_logging();
        let standalone = GreptimeDbStandaloneBuilder::new(
            "test_standalone_prom_store_remote_rw_custom_physical_table",
        )
        .build()
        .await;
        let instance = standalone.fe_instance();

        test_prom_store_remote_rw(instance, Some("my_custom_physical_table".to_string())).await;
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_distributed_prom_store_remote_rw_custom_physical_table() {
        common_telemetry::init_default_ut_logging();
        let distributed = tests::create_distributed_instance(
            "test_distributed_prom_store_remote_rw_custom_physical_table",
        )
        .await;
        test_prom_store_remote_rw(
            &distributed.frontend(),
            Some("my_custom_physical_table".to_string()),
        )
        .await;
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_standalone_prom_store_remote_rw_microsecond_physical_table() {
        common_telemetry::init_default_ut_logging();
        let standalone =
            GreptimeDbStandaloneBuilder::new("test_prom_store_remote_rw_us_physical_table")
                .build()
                .await;
        let instance = standalone.fe_instance();

        test_prom_store_remote_rw_non_millisecond_physical_table(
            instance,
            "TIMESTAMP(6)",
            TimeUnit::Microsecond,
            1_000_000,
            2_000_000,
        )
        .await;
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_distributed_prom_store_remote_rw_microsecond_physical_table() {
        common_telemetry::init_default_ut_logging();
        let distributed =
            tests::create_distributed_instance("test_prom_store_remote_rw_us_physical_table").await;
        test_prom_store_remote_rw_non_millisecond_physical_table(
            &distributed.frontend(),
            "TIMESTAMP(6)",
            TimeUnit::Microsecond,
            1_000_000,
            2_000_000,
        )
        .await;
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_standalone_prom_store_remote_rw_seconds_physical_table() {
        common_telemetry::init_default_ut_logging();
        let standalone =
            GreptimeDbStandaloneBuilder::new("test_prom_store_remote_rw_s_physical_table")
                .build()
                .await;
        let instance = standalone.fe_instance();

        // Narrowing truncates: 1000ms/2000ms -> 1s/2s.
        test_prom_store_remote_rw_non_millisecond_physical_table(
            instance,
            "TIMESTAMP(0)",
            TimeUnit::Second,
            1,
            2,
        )
        .await;
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_standalone_prom_store_remote_rw_nanoseconds_physical_table() {
        common_telemetry::init_default_ut_logging();
        let standalone =
            GreptimeDbStandaloneBuilder::new("test_prom_store_remote_rw_ns_physical_table")
                .build()
                .await;
        let instance = standalone.fe_instance();

        // Lossless widening: 1000ms/2000ms -> 1e9ns/2e9ns.
        test_prom_store_remote_rw_non_millisecond_physical_table(
            instance,
            "TIMESTAMP(9)",
            TimeUnit::Nanosecond,
            1_000_000_000,
            2_000_000_000,
        )
        .await;
    }

    /// Regression test for <https://github.com/GreptimeTeam/greptimedb/issues/9231>:
    /// prometheus remote write/read against a physical metric table
    /// pre-created with a non-millisecond time index. Millisecond samples are
    /// converted to the physical table's unit on write (lossless widening,
    /// truncating narrowing) and narrowed back to milliseconds on remote read.
    async fn test_prom_store_remote_rw_non_millisecond_physical_table(
        instance: &Arc<Instance>,
        sql_ts_type: &str,
        expected_unit: TimeUnit,
        expected_first: i64,
        expected_second: i64,
    ) {
        let db = "prometheus_non_ms";
        let ctx = Arc::new(QueryContext::with(DEFAULT_CATALOG_NAME, db));
        assert!(
            SqlQueryHandler::do_query(
                instance.as_ref(),
                &format!("CREATE DATABASE IF NOT EXISTS {db}"),
                ctx.clone(),
            )
            .await
            .first()
            .unwrap()
            .is_ok()
        );

        let mut output = instance
            .do_query(
                &format!(
                    "CREATE TABLE greptime_physical_table (\
                 greptime_timestamp {sql_ts_type} NOT NULL, \
                 greptime_value DOUBLE NULL, \
                 TIME INDEX (greptime_timestamp)) \
                 ENGINE = metric WITH ('physical_metric_table' = 'true')"
                ),
                ctx.clone(),
            )
            .await;
        assert!(output.remove(0).is_ok());

        let write_request = WriteRequest {
            timeseries: vec![prom_store::mock_timeseries()[0].clone()],
            ..Default::default()
        };
        let (row_inserts, _) = to_grpc_row_insert_requests(&write_request).unwrap();
        instance
            .write(row_inserts, ctx.clone(), true)
            .await
            .unwrap();

        let read_request = ReadRequest {
            queries: vec![Query {
                start_timestamp_ms: 1000,
                end_timestamp_ms: 2000,
                matchers: vec![LabelMatcher {
                    name: prom_store::METRIC_NAME_LABEL.to_string(),
                    value: "metric1".to_string(),
                    r#type: 0,
                }],
                ..Default::default()
            }],
            ..Default::default()
        };
        let resp = instance.read(read_request, ctx.clone()).await.unwrap();
        let body = prom_store::snappy_decompress(&resp.body).unwrap();
        let read_response = ReadResponse::decode(&body[..]).unwrap();
        assert_eq!(1, read_response.results.len());
        assert_eq!(1, read_response.results[0].timeseries.len());
        let timeseries = &read_response.results[0].timeseries[0];
        assert_eq!(
            timeseries.samples,
            vec![
                Sample {
                    value: 1.0,
                    timestamp: 1000,
                },
                Sample {
                    value: 2.0,
                    timestamp: 2000,
                },
            ]
        );

        // The stored timestamps keep the physical table's time index unit.
        let mut output = instance
            .do_query("SELECT greptime_timestamp FROM metric1", ctx.clone())
            .await;
        let OutputData::Stream(stream) = output.remove(0).unwrap().data else {
            unreachable!()
        };
        let batches = common_recordbatch::RecordBatches::try_collect(stream)
            .await
            .unwrap()
            .take();
        assert_eq!(batches[0].num_rows(), 2);
        let ts_column = batches[0].column(0);
        assert_eq!(
            ts_column.data_type(),
            &DataType::Timestamp(expected_unit, None),
            "unexpected time index type"
        );
        let stored = |row: usize| match expected_unit {
            TimeUnit::Second => ts_column.as_primitive::<TimestampSecondType>().value(row),
            TimeUnit::Millisecond => ts_column
                .as_primitive::<TimestampMillisecondType>()
                .value(row),
            TimeUnit::Microsecond => ts_column
                .as_primitive::<TimestampMicrosecondType>()
                .value(row),
            TimeUnit::Nanosecond => ts_column
                .as_primitive::<TimestampNanosecondType>()
                .value(row),
        };
        assert_eq!(stored(0), expected_first);
        assert_eq!(stored(1), expected_second);

        // Negative, non-aligned timestamps must floor towards negative
        // infinity on remote read, matching `Timestamp::convert_to` on the
        // ingestion path: -1001us/-1001000ns -> -2ms, not -1ms.
        if matches!(expected_unit, TimeUnit::Microsecond | TimeUnit::Nanosecond) {
            let (ts_datatype, negative_value) = if expected_unit == TimeUnit::Microsecond {
                (
                    ColumnDataType::TimestampMicrosecond,
                    ValueData::TimestampMicrosecondValue(-1001),
                )
            } else {
                (
                    ColumnDataType::TimestampNanosecond,
                    ValueData::TimestampNanosecondValue(-1_001_000),
                )
            };
            let negative_request = RowInsertRequests {
                inserts: vec![RowInsertRequest {
                    table_name: "metric1".to_string(),
                    rows: Some(Rows {
                        schema: vec![
                            api::v1::ColumnSchema {
                                column_name: "greptime_timestamp".to_string(),
                                datatype: ts_datatype as i32,
                                semantic_type: SemanticType::Timestamp as i32,
                                datatype_extension: None,
                                options: None,
                            },
                            api::v1::ColumnSchema {
                                column_name: "greptime_value".to_string(),
                                datatype: ColumnDataType::Float64 as i32,
                                semantic_type: SemanticType::Field as i32,
                                datatype_extension: None,
                                options: None,
                            },
                            api::v1::ColumnSchema {
                                column_name: "job".to_string(),
                                datatype: ColumnDataType::String as i32,
                                semantic_type: SemanticType::Tag as i32,
                                datatype_extension: None,
                                options: None,
                            },
                        ],
                        rows: vec![Row {
                            values: vec![
                                Value {
                                    value_data: Some(negative_value),
                                },
                                Value {
                                    value_data: Some(ValueData::F64Value(3.0)),
                                },
                                Value {
                                    value_data: Some(ValueData::StringValue("spark".to_string())),
                                },
                            ],
                        }],
                    }),
                }],
            };
            instance
                .write(negative_request, ctx.clone(), true)
                .await
                .unwrap();

            let read_request = ReadRequest {
                queries: vec![Query {
                    start_timestamp_ms: -2,
                    end_timestamp_ms: 0,
                    matchers: vec![LabelMatcher {
                        name: prom_store::METRIC_NAME_LABEL.to_string(),
                        value: "metric1".to_string(),
                        r#type: 0,
                    }],
                    ..Default::default()
                }],
                ..Default::default()
            };
            let resp = instance.read(read_request, ctx.clone()).await.unwrap();
            let body = prom_store::snappy_decompress(&resp.body).unwrap();
            let read_response = ReadResponse::decode(&body[..]).unwrap();
            assert_eq!(1, read_response.results.len());
            assert_eq!(
                read_response.results[0].timeseries[0].samples,
                vec![Sample {
                    value: 3.0,
                    timestamp: -2,
                }],
                "remote read must floor negative non-aligned timestamps"
            );
        }
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_standalone_prom_store_write_existing_logical_table_of_other_physical_unit() {
        common_telemetry::init_default_ut_logging();
        let standalone = GreptimeDbStandaloneBuilder::new(
            "test_prom_store_write_existing_logical_other_physical_unit",
        )
        .build()
        .await;
        let instance = standalone.fe_instance();

        // Two physical metric tables with different time index units.
        let db = "prometheus_mixed_units";
        let ctx = Arc::new(QueryContext::with(DEFAULT_CATALOG_NAME, db));
        assert!(
            SqlQueryHandler::do_query(
                instance.as_ref(),
                &format!("CREATE DATABASE IF NOT EXISTS {db}"),
                ctx.clone(),
            )
            .await
            .first()
            .unwrap()
            .is_ok()
        );
        let mut output = instance
            .do_query(
                "CREATE TABLE phy_us (greptime_timestamp TIMESTAMP(6) NOT NULL, \
                 greptime_value DOUBLE NULL, TIME INDEX (greptime_timestamp)) \
                 ENGINE = metric WITH ('physical_metric_table' = 'true')",
                ctx.clone(),
            )
            .await;
        assert!(output.remove(0).is_ok());

        // First create the logical table `shared_metric` on the default
        // (millisecond) physical table.
        let shared_series = TimeSeries {
            labels: vec![
                Label {
                    name: prom_store::METRIC_NAME_LABEL.to_string(),
                    value: "shared_metric".to_string(),
                },
                Label {
                    name: "job".to_string(),
                    value: "demo".to_string(),
                },
            ],
            samples: vec![Sample {
                value: 1.0,
                timestamp: 1000,
            }],
            ..Default::default()
        };
        let (row_inserts, _) = to_grpc_row_insert_requests(&WriteRequest {
            timeseries: vec![shared_series.clone()],
            ..Default::default()
        })
        .unwrap();
        instance
            .write(row_inserts, ctx.clone(), true)
            .await
            .unwrap();

        // Now select the microsecond physical table while writing to BOTH the
        // existing millisecond `shared_metric` (bound to the default physical
        // table) and a new table: the existing table's request must keep the
        // millisecond unit, and only the new table uses microsecond.
        let mut hint_ctx = QueryContext::with(DEFAULT_CATALOG_NAME, db);
        hint_ctx.set_extension(PHYSICAL_TABLE_PARAM, "phy_us".to_string());
        let hint_ctx = Arc::new(hint_ctx);
        let (row_inserts, _) = to_grpc_row_insert_requests(&WriteRequest {
            timeseries: vec![
                TimeSeries {
                    samples: vec![Sample {
                        value: 1.5,
                        timestamp: 1500,
                    }],
                    ..shared_series
                },
                TimeSeries {
                    labels: vec![
                        Label {
                            name: prom_store::METRIC_NAME_LABEL.to_string(),
                            value: "fresh_us_metric".to_string(),
                        },
                        Label {
                            name: "job".to_string(),
                            value: "demo".to_string(),
                        },
                    ],
                    samples: vec![Sample {
                        value: 2.5,
                        timestamp: 1500,
                    }],
                    ..Default::default()
                },
            ],
            ..Default::default()
        })
        .unwrap();
        instance
            .write(row_inserts, hint_ctx.clone(), true)
            .await
            .unwrap();

        // The existing table keeps its millisecond unit and precision.
        let mut output = instance
            .do_query(
                "SELECT greptime_timestamp FROM shared_metric ORDER BY greptime_timestamp",
                ctx.clone(),
            )
            .await;
        let OutputData::Stream(stream) = output.remove(0).unwrap().data else {
            unreachable!()
        };
        let batches = common_recordbatch::RecordBatches::try_collect(stream)
            .await
            .unwrap()
            .take();
        let shared_ts = batches[0]
            .column(0)
            .as_primitive::<TimestampMillisecondType>();
        assert_eq!((shared_ts.value(0), shared_ts.value(1)), (1000, 1500));

        // The new table is created on the selected physical table with the
        // microsecond unit.
        let mut output = instance
            .do_query(
                "SELECT greptime_timestamp FROM fresh_us_metric",
                ctx.clone(),
            )
            .await;
        let OutputData::Stream(stream) = output.remove(0).unwrap().data else {
            unreachable!()
        };
        let batches = common_recordbatch::RecordBatches::try_collect(stream)
            .await
            .unwrap()
            .take();
        let fresh_ts = batches[0]
            .column(0)
            .as_primitive::<TimestampMicrosecondType>();
        assert_eq!(fresh_ts.value(0), 1_500_000);
    }

    async fn test_prom_store_remote_rw(instance: &Arc<Instance>, physical_table: Option<String>) {
        let write_request = WriteRequest {
            timeseries: prom_store::mock_timeseries(),
            ..Default::default()
        };

        let db = "prometheus";
        let mut ctx = Arc::into_inner(QueryContext::with(DEFAULT_CATALOG_NAME, db).into()).unwrap();

        // set physical table if provided
        if let Some(physical_table) = &physical_table {
            ctx.set_extension(PHYSICAL_TABLE_PARAM.to_string(), physical_table.clone());
        }
        let ctx = Arc::new(ctx);

        assert!(
            SqlQueryHandler::do_query(
                instance.as_ref(),
                "CREATE DATABASE IF NOT EXISTS prometheus",
                ctx.clone(),
            )
            .await
            .first()
            .unwrap()
            .is_ok()
        );

        let (row_inserts, _) = to_grpc_row_insert_requests(&write_request).unwrap();
        instance
            .write(row_inserts, ctx.clone(), true)
            .await
            .unwrap();

        let read_request = ReadRequest {
            queries: vec![
                Query {
                    start_timestamp_ms: 1000,
                    end_timestamp_ms: 2000,
                    matchers: vec![LabelMatcher {
                        name: prom_store::METRIC_NAME_LABEL.to_string(),
                        value: "metric1".to_string(),
                        r#type: 0,
                    }],
                    ..Default::default()
                },
                Query {
                    start_timestamp_ms: 1000,
                    end_timestamp_ms: 3000,
                    matchers: vec![
                        LabelMatcher {
                            name: prom_store::METRIC_NAME_LABEL.to_string(),
                            value: "metric3".to_string(),
                            r#type: 0,
                        },
                        LabelMatcher {
                            name: "app".to_string(),
                            value: "biz".to_string(),
                            r#type: MatcherType::Eq as i32,
                        },
                    ],
                    ..Default::default()
                },
            ],
            ..Default::default()
        };

        let resp = instance.read(read_request, ctx.clone()).await.unwrap();
        assert_eq!(resp.content_type, "application/x-protobuf");
        assert_eq!(resp.content_encoding, "snappy");
        let body = prom_store::snappy_decompress(&resp.body).unwrap();
        let read_response = ReadResponse::decode(&body[..]).unwrap();
        let query_results = read_response.results;
        assert_eq!(2, query_results.len());

        assert_eq!(1, query_results[0].timeseries.len());
        let timeseries = &query_results[0].timeseries[0];

        assert_eq!(
            vec![
                Label {
                    name: prom_store::METRIC_NAME_LABEL.to_string(),
                    value: "metric1".to_string(),
                },
                Label {
                    name: "job".to_string(),
                    value: "spark".to_string(),
                },
            ],
            timeseries.labels
        );

        assert_eq!(
            timeseries.samples,
            vec![
                Sample {
                    value: 1.0,
                    timestamp: 1000,
                },
                Sample {
                    value: 2.0,
                    timestamp: 2000,
                }
            ]
        );

        assert_eq!(1, query_results[1].timeseries.len());
        let timeseries = &query_results[1].timeseries[0];

        assert_eq!(
            vec![
                Label {
                    name: prom_store::METRIC_NAME_LABEL.to_string(),
                    value: "metric3".to_string(),
                },
                Label {
                    name: "app".to_string(),
                    value: "biz".to_string(),
                },
                Label {
                    name: "idc".to_string(),
                    value: "z002".to_string(),
                },
            ],
            timeseries.labels
        );

        assert_eq!(
            timeseries.samples,
            vec![
                Sample {
                    value: 5.0,
                    timestamp: 1000,
                },
                Sample {
                    value: 6.0,
                    timestamp: 2000,
                },
                Sample {
                    value: 7.0,
                    timestamp: 3000,
                }
            ]
        );

        instance
            .do_query(
                "CREATE TABLE invalid_metric (ts TIMESTAMP TIME INDEX, a DOUBLE, b DOUBLE)",
                ctx.clone(),
            )
            .await[0]
            .as_ref()
            .unwrap();
        let server = HttpServerBuilder::new(HttpOptions::default())
            .with_prom_handler(instance.clone(), None, true, Default::default(), None)
            .build();
        let client = TestClient::new(server.build(server.make_app()).unwrap()).await;

        test_remote_read_metric_names(instance, &client).await;

        // Cover missing schemas/catalogs and each database label alias.
        for (selector, database) in [
            ("__schema__", "unknown_schema"),
            ("__database__", "unknown_catalog-prometheus"),
            ("x_greptime_database", "unknown_catalog-information_schema"),
        ] {
            let mut query = remote_query("missing_metric");
            query.matchers.push(LabelMatcher {
                name: selector.to_string(),
                value: database.to_string(),
                r#type: MatcherType::Eq as i32,
            });
            let response = post_remote_read(&client, vec![query]).await;
            assert_eq!(response.status(), 200, "{selector}={database}");
            let body = prom_store::snappy_decompress(&response.bytes().await).unwrap();
            let response = ReadResponse::decode(&body[..]).unwrap();
            assert_eq!(response.results.len(), 1);
            assert!(response.results[0].timeseries.is_empty());
        }

        // Matcher syntax must be checked even when the metric does not exist.
        for metric in ["metric1", "missing_metric"] {
            for (matcher_type, value, expected_status) in [
                (99, "spark", 400),
                (MatcherType::Re as i32, "[", 400),
                (MatcherType::Nre as i32, "[", 400),
                (MatcherType::Eq as i32, "[", 200),
                (MatcherType::Neq as i32, "[", 200),
                (MatcherType::Re as i32, "spark", 200),
                (MatcherType::Nre as i32, "api.*", 200),
                (MatcherType::Re as i32, "(?x) spark", 200),
                (MatcherType::Nre as i32, "(?x) spark", 200),
                (MatcherType::Re as i32, "(?x) spark # comment", 400),
                (MatcherType::Nre as i32, "(?x) spark # comment", 400),
                (MatcherType::Re as i32, "a)|(?:b", 400),
                (MatcherType::Nre as i32, "a)|(?:b", 400),
            ] {
                let mut query = remote_query(metric);
                query.matchers.push(LabelMatcher {
                    name: "job".to_string(),
                    value: value.to_string(),
                    r#type: matcher_type,
                });
                let response = post_remote_read(&client, vec![query]).await;
                assert_eq!(
                    response.status().as_u16(),
                    expected_status,
                    "metric={metric}, type={matcher_type}, value={value}"
                );
            }
        }

        // Missing metrics must retain their result slots in multi-query responses.
        for metrics in [
            vec!["missing_metric"],
            vec!["missing_metric", "another_missing_metric"],
            vec![
                "missing_metric",
                "metric1",
                "missing_metric",
                "metric3",
                "missing_metric",
            ],
            vec!["missing_metric", "invalid_metric"],
        ] {
            let queries = metrics.iter().map(|metric| remote_query(metric)).collect();
            let response = post_remote_read(&client, queries).await;
            if metrics.contains(&"invalid_metric") {
                assert!(!response.status().is_success());
                assert!(response.text().await.contains("Ambiguous value column"));
                continue;
            }
            assert_eq!(response.status(), 200, "{}", response.text().await);
            assert_eq!(response.headers()["content-type"], "application/x-protobuf");
            assert_eq!(response.headers()["content-encoding"], "snappy");
            let body = prom_store::snappy_decompress(&response.bytes().await).unwrap();
            let response = ReadResponse::decode(&body[..]).unwrap();
            assert_eq!(response.results.len(), metrics.len());
            for (result, metric) in response.results.iter().zip(metrics) {
                if metric.ends_with("missing_metric") {
                    assert!(result.timeseries.is_empty());
                    continue;
                }
                assert!(!result.timeseries.is_empty());
                for series in &result.timeseries {
                    assert!(series.labels.iter().any(|label| {
                        label.name == prom_store::METRIC_NAME_LABEL && label.value == metric
                    }));
                    assert_eq!(series.samples.len(), 2);
                }
            }
        }

        // check physical table if provided
        if let Some(physical_table) = physical_table {
            let sql = format!("DESC TABLE {physical_table};");
            instance.do_query(&sql, ctx).await[0].as_ref().unwrap();
        }
    }

    async fn test_remote_read_metric_names(instance: &Arc<Instance>, client: &TestClient) {
        let ctx = Arc::new(QueryContext::with(DEFAULT_CATALOG_NAME, "name_matches"));
        for sql in [
            "CREATE DATABASE name_matches",
            "CREATE TABLE cpu (ts TIMESTAMP TIME INDEX, sample_value DOUBLE, job STRING, PRIMARY KEY(job)) ENGINE=mito",
            "CREATE TABLE memory (ts TIMESTAMP TIME INDEX, sample_value DOUBLE, label_zone STRING, PRIMARY KEY(label_zone)) ENGINE=mito",
            "INSERT INTO cpu VALUES (999, 0, 'api'), (1000, 1, 'api'), (2000, 2, 'api'), (2001, 3, 'api')",
            "INSERT INTO memory VALUES (1000, 10, 'us'), (2000, 20, 'us')",
        ] {
            instance.do_query(sql, ctx.clone()).await[0]
                .as_ref()
                .unwrap();
        }
        let matcher = |name: &str, op: MatcherType, value: &str| LabelMatcher {
            name: name.into(),
            value: value.into(),
            r#type: op as i32,
        };
        let name = |op, value| matcher(prom_store::METRIC_NAME_LABEL, op, value);
        let cases = vec![
            (
                vec![name(MatcherType::Re, "cpu|memory")],
                vec!["cpu", "memory"],
            ),
            (vec![name(MatcherType::Eq, "")], vec![]),
            (vec![name(MatcherType::Eq, "cpu")], vec!["cpu"]),
            (vec![], vec!["cpu", "memory"]),
            (vec![matcher("job", MatcherType::Eq, "api")], vec!["cpu"]),
            (vec![matcher("job", MatcherType::Eq, "")], vec!["memory"]),
        ];
        let queries = cases
            .iter()
            .map(|(matchers, _)| {
                let mut matchers = matchers.clone();
                // The HTTP request defaults to another database; discovery must use this selector.
                matchers.push(matcher("__database__", MatcherType::Eq, "name_matches"));
                Query {
                    start_timestamp_ms: 1000,
                    end_timestamp_ms: 2000,
                    matchers,
                    ..Default::default()
                }
            })
            .collect();
        let response = post_remote_read(client, queries).await;
        assert_eq!(response.status(), 200, "{}", response.text().await);
        let body = prom_store::snappy_decompress(&response.bytes().await).unwrap();
        let response = ReadResponse::decode(&body[..]).unwrap();
        assert_eq!(cases.len(), response.results.len());
        for ((matchers, expected), result) in cases.iter().zip(&response.results) {
            let mut names = Vec::new();
            for series in &result.timeseries {
                let metric = &series
                    .labels
                    .iter()
                    .find(|label| label.name == prom_store::METRIC_NAME_LABEL)
                    .unwrap()
                    .value;
                names.push(metric.as_str());
                let factor = if metric == "cpu" { 1.0 } else { 10.0 };
                assert_eq!(
                    series.samples,
                    vec![
                        Sample {
                            timestamp: 1000,
                            value: factor
                        },
                        Sample {
                            timestamp: 2000,
                            value: 2.0 * factor
                        },
                    ]
                );
            }
            names.sort_unstable();
            assert_eq!(*expected, names, "{matchers:?}");
        }

        // Discovery includes logical metrics but excludes their internal physical table.
        let mut query = remote_query("metric1");
        query.matchers = vec![name(MatcherType::Nre, "invalid_metric")];
        let response = post_remote_read(client, vec![query]).await;
        assert_eq!(response.status(), 200, "{}", response.text().await);
        let body = prom_store::snappy_decompress(&response.bytes().await).unwrap();
        let response = ReadResponse::decode(&body[..]).unwrap();
        assert_eq!(response.results.len(), 1);
        assert_eq!(response.results[0].timeseries.len(), 3);

        // An empty EQ must not bypass matcher validation.
        let mut query = remote_query("");
        query.matchers.push(name(MatcherType::Re, "["));
        assert_eq!(post_remote_read(client, vec![query]).await.status(), 400);
        for database in ["unknown_schema", "unknown_catalog-name_matches"] {
            let query = Query {
                matchers: vec![
                    name(MatcherType::Re, ".*"),
                    matcher("__database__", MatcherType::Eq, database),
                ],
                ..Default::default()
            };
            let response = post_remote_read(client, vec![query]).await;
            assert_eq!(response.status(), 200, "{}", response.text().await);
            let body = prom_store::snappy_decompress(&response.bytes().await).unwrap();
            let response = ReadResponse::decode(&body[..]).unwrap();
            assert!(response.results[0].timeseries.is_empty());
        }

        // Broad selectors must propagate incompatible-table errors, not return partial success.
        instance
            .do_query(
                "CREATE TABLE invalid (ts TIMESTAMP TIME INDEX, a DOUBLE, b DOUBLE)",
                ctx.clone(),
            )
            .await[0]
            .as_ref()
            .unwrap();
        let query = Query {
            matchers: vec![
                name(MatcherType::Re, "cpu|invalid"),
                matcher("__database__", MatcherType::Eq, "name_matches"),
            ],
            ..Default::default()
        };
        let response = post_remote_read(client, vec![query]).await;
        assert!(!response.status().is_success());
        assert!(response.text().await.contains("Ambiguous value column"));
    }

    fn remote_query(metric: &str) -> Query {
        Query {
            start_timestamp_ms: 1000,
            end_timestamp_ms: 2000,
            matchers: vec![LabelMatcher {
                name: prom_store::METRIC_NAME_LABEL.to_string(),
                value: metric.to_string(),
                r#type: MatcherType::Eq as i32,
            }],
            ..Default::default()
        }
    }

    async fn post_remote_read(client: &TestClient, queries: Vec<Query>) -> TestResponse {
        let request = ReadRequest {
            queries,
            ..Default::default()
        };
        client
            .post("/v1/prometheus/read?db=prometheus")
            .body(prom_store::snappy_compress(&request.encode_to_vec()).unwrap())
            .send()
            .await
    }
}
