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
    use std::sync::Arc;

    use client::DEFAULT_CATALOG_NAME;
    use common_query::Output;
    use common_recordbatch::RecordBatches;
    use frontend::instance::Instance;
    use opentelemetry_proto::tonic::collector::metrics::v1::ExportMetricsServiceRequest;
    use opentelemetry_proto::tonic::common::v1::any_value::Value as Val;
    use opentelemetry_proto::tonic::common::v1::{AnyValue, KeyValue};
    use opentelemetry_proto::tonic::metrics::v1::number_data_point::Value;
    use opentelemetry_proto::tonic::metrics::v1::{metric, NumberDataPoint, *};
    use servers::query_handler::sql::SqlQueryHandler;
    use servers::query_handler::OpenTelemetryProtocolHandler;
    use session::context::QueryContext;

    use crate::tests;

    #[tokio::test(flavor = "multi_thread")]
    pub async fn test_otlp_on_standalone() {
        let standalone = tests::create_standalone_instance("test_standalone_otlp").await;
        let instance = &standalone.instance;

        test_otlp(instance).await;
    }

    #[tokio::test(flavor = "multi_thread")]
    pub async fn test_otlp_on_distributed() {
        let instance = tests::create_distributed_instance("test_standalone_otlp").await;

        test_otlp(&instance.frontend()).await;
    }

    async fn test_otlp(instance: &Arc<Instance>) {
        let req = build_request();
        let db = "otlp";
        let ctx = Arc::new(QueryContext::with(DEFAULT_CATALOG_NAME, db));

        assert!(SqlQueryHandler::do_query(
            instance.as_ref(),
            &format!("CREATE DATABASE IF NOT EXISTS {db}"),
            ctx.clone(),
        )
        .await
        .get(0)
        .unwrap()
        .is_ok());

        let resp = instance.metrics(req, ctx.clone()).await.unwrap();
        assert!(resp.partial_success.is_none());

        let mut output = instance
            .do_query(
                "SELECT * FROM my_test_metric ORDER BY greptime_timestamp",
                ctx.clone(),
            )
            .await;
        let output = output.remove(0).unwrap();
        let Output::Stream(stream) = output else { unreachable!() };
        let recordbatches = RecordBatches::try_collect(stream).await.unwrap();
        assert_eq!(
            recordbatches.pretty_print().unwrap(),
            "\
+------------+---------------------+----------------+
| host       | greptime_timestamp  | greptime_value |
+------------+---------------------+----------------+
| testserver | 1970-01-01T00:00:00 | 105.0          |
| testsevrer | 1970-01-01T00:00:00 | 100.0          |
+------------+---------------------+----------------+"
        );
    }

    fn build_request() -> ExportMetricsServiceRequest {
        let data_points = vec![
            NumberDataPoint {
                attributes: vec![keyvalue("host", "testsevrer")],
                time_unix_nano: 100,
                value: Some(Value::AsInt(100)),
                ..Default::default()
            },
            NumberDataPoint {
                attributes: vec![keyvalue("host", "testserver")],
                time_unix_nano: 105,
                value: Some(Value::AsInt(105)),
                ..Default::default()
            },
        ];
        let gauge = Gauge { data_points };

        ExportMetricsServiceRequest {
            resource_metrics: vec![ResourceMetrics {
                scope_metrics: vec![ScopeMetrics {
                    metrics: vec![Metric {
                        name: "my.test.metric".into(),
                        description: "my ignored desc".into(),
                        unit: "my ignored unit".into(),
                        data: Some(metric::Data::Gauge(gauge)),
                    }],
                    ..Default::default()
                }],
                ..Default::default()
            }],
        }
    }

    fn keyvalue(key: &str, value: &str) -> KeyValue {
        KeyValue {
            key: key.into(),
            value: Some(AnyValue {
                value: Some(Val::StringValue(value.into())),
            }),
        }
    }
}
