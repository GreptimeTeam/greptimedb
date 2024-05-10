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

    use client::{OutputData, DEFAULT_CATALOG_NAME};
    use common_recordbatch::RecordBatches;
    use frontend::instance::Instance;
    use opentelemetry_proto::tonic::collector::metrics::v1::ExportMetricsServiceRequest;
    use opentelemetry_proto::tonic::common::v1::any_value::Value as Val;
    use opentelemetry_proto::tonic::common::v1::{AnyValue, InstrumentationScope, KeyValue};
    use opentelemetry_proto::tonic::metrics::v1::number_data_point::Value;
    use opentelemetry_proto::tonic::metrics::v1::{metric, NumberDataPoint, *};
    use opentelemetry_proto::tonic::resource::v1::Resource;
    use servers::query_handler::sql::SqlQueryHandler;
    use servers::query_handler::OpenTelemetryProtocolHandler;
    use session::context::QueryContext;

    use crate::standalone::GreptimeDbStandaloneBuilder;
    use crate::tests;

    #[tokio::test(flavor = "multi_thread")]
    pub async fn test_otlp_on_standalone() {
        let standalone = GreptimeDbStandaloneBuilder::new("test_standalone_otlp")
            .build()
            .await;
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
        .first()
        .unwrap()
        .is_ok());

        let resp = instance.metrics(req, ctx.clone()).await;
        assert!(resp.is_ok());

        let mut output = instance
            .do_query(
                "SELECT * FROM my_test_metric ORDER BY greptime_timestamp",
                ctx.clone(),
            )
            .await;
        let output = output.remove(0).unwrap();
        let OutputData::Stream(stream) = output.data else {
            unreachable!()
        };
        let recordbatches = RecordBatches::try_collect(stream).await.unwrap();
        assert_eq!(
            recordbatches.pretty_print().unwrap(),
            "\
+------------+-------+--------------------+------------+-------------------------------+----------------+
| resource   | scope | telemetry_sdk_name | host       | greptime_timestamp            | greptime_value |
+------------+-------+--------------------+------------+-------------------------------+----------------+
| greptimedb | otel  | java               | testsevrer | 1970-01-01T00:00:00.000000100 | 100.0          |
| greptimedb | otel  | java               | testserver | 1970-01-01T00:00:00.000000105 | 105.0          |
+------------+-------+--------------------+------------+-------------------------------+----------------+",
        );

        let mut output = instance
            .do_query(
                "SELECT le, greptime_value FROM my_test_histo_bucket order by le",
                ctx.clone(),
            )
            .await;
        let output = output.remove(0).unwrap();
        let OutputData::Stream(stream) = output.data else {
            unreachable!()
        };
        let recordbatches = RecordBatches::try_collect(stream).await.unwrap();
        assert_eq!(
            recordbatches.pretty_print().unwrap(),
            "\
+-----+----------------+
| le  | greptime_value |
+-----+----------------+
| 1   | 1.0            |
| 5   | 3.0            |
| inf | 4.0            |
+-----+----------------+",
        );

        let mut output = instance
            .do_query("SELECT * FROM my_test_histo_sum", ctx.clone())
            .await;
        let output = output.remove(0).unwrap();
        let OutputData::Stream(stream) = output.data else {
            unreachable!()
        };
        let recordbatches = RecordBatches::try_collect(stream).await.unwrap();
        assert_eq!(
            recordbatches.pretty_print().unwrap(),
            "\
+------------+-------+--------------------+------------+-------------------------------+----------------+
| resource   | scope | telemetry_sdk_name | host       | greptime_timestamp            | greptime_value |
+------------+-------+--------------------+------------+-------------------------------+----------------+
| greptimedb | otel  | java               | testserver | 1970-01-01T00:00:00.000000100 | 51.0           |
+------------+-------+--------------------+------------+-------------------------------+----------------+",
        );

        let mut output = instance
            .do_query("SELECT * FROM my_test_histo_count", ctx.clone())
            .await;
        let output = output.remove(0).unwrap();
        let OutputData::Stream(stream) = output.data else {
            unreachable!()
        };
        let recordbatches = RecordBatches::try_collect(stream).await.unwrap();
        assert_eq!(
            recordbatches.pretty_print().unwrap(),
            "\
+------------+-------+--------------------+------------+-------------------------------+----------------+
| resource   | scope | telemetry_sdk_name | host       | greptime_timestamp            | greptime_value |
+------------+-------+--------------------+------------+-------------------------------+----------------+
| greptimedb | otel  | java               | testserver | 1970-01-01T00:00:00.000000100 | 4.0            |
+------------+-------+--------------------+------------+-------------------------------+----------------+"
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

        let histo_data_points = vec![HistogramDataPoint {
            attributes: vec![keyvalue("host", "testserver")],
            time_unix_nano: 100,
            count: 4,
            bucket_counts: vec![1, 2, 1],
            explicit_bounds: vec![1.0f64, 5.0f64],
            sum: Some(51f64),
            ..Default::default()
        }];

        let histo = Histogram {
            data_points: histo_data_points,
            aggregation_temporality: 0,
        };

        ExportMetricsServiceRequest {
            resource_metrics: vec![ResourceMetrics {
                scope_metrics: vec![ScopeMetrics {
                    metrics: vec![
                        Metric {
                            name: "my.test.metric".into(),
                            description: "my ignored desc".into(),
                            unit: "my ignored unit".into(),
                            data: Some(metric::Data::Gauge(gauge)),
                        },
                        Metric {
                            name: "my.test.histo".into(),
                            description: "my ignored desc".into(),
                            unit: "my ignored unit".into(),
                            data: Some(metric::Data::Histogram(histo)),
                        },
                    ],
                    scope: Some(InstrumentationScope {
                        attributes: vec![
                            keyvalue("scope", "otel"),
                            keyvalue("telemetry.sdk.name", "java"),
                        ],
                        ..Default::default()
                    }),
                    ..Default::default()
                }],
                resource: Some(Resource {
                    attributes: vec![keyvalue("resource", "greptimedb")],
                    dropped_attributes_count: 0,
                }),
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
