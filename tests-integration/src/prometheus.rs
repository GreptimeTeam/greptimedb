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

    use api::prometheus::remote::label_matcher::Type as MatcherType;
    use api::prometheus::remote::{
        Label, LabelMatcher, Query, ReadRequest, ReadResponse, Sample, WriteRequest,
    };
    use common_catalog::consts::DEFAULT_CATALOG_NAME;
    use frontend::instance::Instance;
    use prost::Message;
    use servers::prometheus;
    use servers::query_handler::sql::SqlQueryHandler;
    use servers::query_handler::PrometheusProtocolHandler;
    use session::context::QueryContext;

    use crate::tests;

    #[tokio::test(flavor = "multi_thread")]
    async fn test_standalone_prometheus_remote_rw() {
        let standalone =
            tests::create_standalone_instance("test_standalone_prometheus_remote_rw").await;
        let instance = &standalone.instance;

        test_prometheus_remote_rw(instance).await;
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_distributed_prometheus_remote_rw() {
        let distributed =
            tests::create_distributed_instance("test_distributed_prometheus_remote_rw").await;
        let instance = &distributed.frontend;

        test_prometheus_remote_rw(instance).await;
    }

    async fn test_prometheus_remote_rw(instance: &Arc<Instance>) {
        let write_request = WriteRequest {
            timeseries: prometheus::mock_timeseries(),
            ..Default::default()
        };

        let db = "prometheus";
        let ctx = Arc::new(QueryContext::with(DEFAULT_CATALOG_NAME, db));

        assert!(SqlQueryHandler::do_query(
            instance.as_ref(),
            "CREATE DATABASE IF NOT EXISTS prometheus",
            ctx.clone(),
        )
        .await
        .get(0)
        .unwrap()
        .is_ok());

        instance.write(write_request, ctx.clone()).await.unwrap();

        let read_request = ReadRequest {
            queries: vec![
                Query {
                    start_timestamp_ms: 1000,
                    end_timestamp_ms: 2000,
                    matchers: vec![LabelMatcher {
                        name: prometheus::METRIC_NAME_LABEL.to_string(),
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
                            name: prometheus::METRIC_NAME_LABEL.to_string(),
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

        let resp = instance.read(read_request, ctx).await.unwrap();
        assert_eq!(resp.content_type, "application/x-protobuf");
        assert_eq!(resp.content_encoding, "snappy");
        let body = prometheus::snappy_decompress(&resp.body).unwrap();
        let read_response = ReadResponse::decode(&body[..]).unwrap();
        let query_results = read_response.results;
        assert_eq!(2, query_results.len());

        assert_eq!(1, query_results[0].timeseries.len());
        let timeseries = &query_results[0].timeseries[0];

        assert_eq!(
            vec![
                Label {
                    name: prometheus::METRIC_NAME_LABEL.to_string(),
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
                    name: prometheus::METRIC_NAME_LABEL.to_string(),
                    value: "metric3".to_string(),
                },
                Label {
                    name: "idc".to_string(),
                    value: "z002".to_string(),
                },
                Label {
                    name: "app".to_string(),
                    value: "biz".to_string(),
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
    }
}
