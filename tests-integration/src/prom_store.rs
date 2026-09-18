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
        Label, LabelMatcher, Query, ReadRequest, ReadResponse, Sample, WriteRequest,
    };
    use client::OutputData;
    use common_catalog::consts::DEFAULT_CATALOG_NAME;
    use datatypes::arrow::array::AsArray;
    use datatypes::arrow::datatypes::TimestampMicrosecondType;
    use frontend::instance::Instance;
    use prost::Message;
    use servers::http::prom_store::PHYSICAL_TABLE_PARAM;
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

        test_prom_store_remote_rw_microsecond_physical_table(instance).await;
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_distributed_prom_store_remote_rw_microsecond_physical_table() {
        common_telemetry::init_default_ut_logging();
        let distributed =
            tests::create_distributed_instance("test_prom_store_remote_rw_us_physical_table").await;
        test_prom_store_remote_rw_microsecond_physical_table(&distributed.frontend()).await;
    }

    /// Regression test for <https://github.com/GreptimeTeam/greptimedb/issues/9231>:
    /// prometheus remote write/read against a physical metric table
    /// pre-created with a TIMESTAMP(6) (microsecond) time index. Millisecond
    /// samples are widened to the physical table's unit on write and narrowed
    /// back to milliseconds on remote read.
    async fn test_prom_store_remote_rw_microsecond_physical_table(instance: &Arc<Instance>) {
        let db = "prometheus_us";
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
                "CREATE TABLE greptime_physical_table (\
                 greptime_timestamp TIMESTAMP(6) NOT NULL, \
                 greptime_value DOUBLE NULL, \
                 TIME INDEX (greptime_timestamp)) \
                 ENGINE = metric WITH ('physical_metric_table' = 'true')",
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

        // The stored timestamps keep the physical table's microsecond unit.
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
        let timestamps = batches[0]
            .column(0)
            .as_primitive::<TimestampMicrosecondType>();
        // Millisecond samples are widened to the microsecond time index.
        assert_eq!(timestamps.value(0), 1_000_000);
        assert_eq!(timestamps.value(1), 2_000_000);
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

        // check physical table if provided
        if let Some(physical_table) = physical_table {
            let sql = format!("DESC TABLE {physical_table};");
            instance.do_query(&sql, ctx).await[0].as_ref().unwrap();
        }
    }
}
