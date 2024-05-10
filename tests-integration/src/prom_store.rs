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
    use common_catalog::consts::DEFAULT_CATALOG_NAME;
    use frontend::instance::Instance;
    use prost::Message;
    use servers::http::prom_store::PHYSICAL_TABLE_PARAM;
    use servers::prom_store;
    use servers::prom_store::to_grpc_row_insert_requests;
    use servers::query_handler::sql::SqlQueryHandler;
    use servers::query_handler::PromStoreProtocolHandler;
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
        let instance = &standalone.instance;

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
        let instance = &standalone.instance;

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

        assert!(SqlQueryHandler::do_query(
            instance.as_ref(),
            "CREATE DATABASE IF NOT EXISTS prometheus",
            ctx.clone(),
        )
        .await
        .first()
        .unwrap()
        .is_ok());

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
