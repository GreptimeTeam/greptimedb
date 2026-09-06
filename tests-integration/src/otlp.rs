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

    use client::{DEFAULT_CATALOG_NAME, OutputData};
    use common_recordbatch::RecordBatches;
    use frontend::instance::Instance;
    use otel_arrow_rust::proto::opentelemetry::collector::metrics::v1::ExportMetricsServiceRequest;
    use otel_arrow_rust::proto::opentelemetry::common::v1::any_value::Value as Val;
    use otel_arrow_rust::proto::opentelemetry::common::v1::{
        AnyValue, InstrumentationScope, KeyValue,
    };
    use otel_arrow_rust::proto::opentelemetry::metrics::v1::number_data_point::Value;
    use otel_arrow_rust::proto::opentelemetry::metrics::v1::{
        AggregationTemporality, DataPointFlags, Gauge, Histogram, HistogramDataPoint, Metric,
        NumberDataPoint, ResourceMetrics, ScopeMetrics, Sum, metric,
    };
    use otel_arrow_rust::proto::opentelemetry::resource::v1::Resource;
    use servers::query_handler::OpenTelemetryProtocolHandler;
    use servers::query_handler::sql::SqlQueryHandler;
    use session::context::QueryContext;

    use crate::standalone::GreptimeDbStandaloneBuilder;
    use crate::tests;

    #[tokio::test(flavor = "multi_thread")]
    pub async fn test_otlp_on_standalone() {
        let standalone = GreptimeDbStandaloneBuilder::new("test_standalone_otlp")
            .build()
            .await;
        let instance = standalone.fe_instance();

        test_otlp(instance).await;
    }

    #[tokio::test(flavor = "multi_thread")]
    pub async fn test_otlp_on_distributed() {
        let instance = tests::create_distributed_instance("test_standalone_otlp").await;

        test_otlp(&instance.frontend()).await;
    }

    #[tokio::test(flavor = "multi_thread")]
    pub async fn test_otlp_fixed_schema_rejects_missing_temporality_tag() {
        let standalone = GreptimeDbStandaloneBuilder::new("test_otlp_fixed_schema")
            .with_auto_create_table(false)
            .build()
            .await;
        let instance = standalone.fe_instance();
        let ctx = Arc::new(QueryContext::with(DEFAULT_CATALOG_NAME, "public"));
        let mut output = instance
            .do_query(
                "CREATE TABLE fixed_delta_total (\
                 \"stream\" STRING, greptime_timestamp TIMESTAMP(3) NOT NULL, \
                 greptime_value DOUBLE, TIME INDEX (greptime_timestamp), \
                 PRIMARY KEY (\"stream\")) ENGINE=mito",
                ctx.clone(),
            )
            .await;
        let result = output.remove(0);
        assert!(result.is_ok(), "{result:?}");

        let error = instance
            .metrics(
                build_sum_request("fixed.delta", AggregationTemporality::Delta, &[(60, 10)]),
                ctx.clone(),
            )
            .await
            .unwrap_err();
        assert!(format!("{error:?}").contains("otlp_aggregation_temporality"));

        let mut output = instance
            .do_query("SELECT COUNT(*) FROM fixed_delta_total", ctx)
            .await;
        let OutputData::Stream(stream) = output.remove(0).unwrap().data else {
            unreachable!()
        };
        assert!(
            RecordBatches::try_collect(stream)
                .await
                .unwrap()
                .pretty_print()
                .unwrap()
                .contains("| 0        |")
        );
    }

    async fn test_otlp(instance: &Arc<Instance>) {
        let req = build_request();
        let db = "otlp";
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

        let resp = instance.metrics(req, ctx.clone()).await;
        assert!(resp.is_ok());

        let mut output = instance
            .do_query(
                "CREATE TABLE raw_delta_mito_total (\
                 \"stream\" STRING, greptime_timestamp TIMESTAMP(3) NOT NULL, \
                 greptime_value DOUBLE, TIME INDEX (greptime_timestamp), \
                 PRIMARY KEY (\"stream\")) ENGINE=mito",
                ctx.clone(),
            )
            .await;
        let result = output.remove(0);
        assert!(result.is_ok(), "{result:?}");

        for (metric, table) in [
            ("raw.delta", "raw_delta_total"),
            ("raw.delta.mito", "raw_delta_mito_total"),
        ] {
            for request in [
                build_sum_request(metric, AggregationTemporality::Cumulative, &[(60, 10)]),
                build_sum_request(
                    metric,
                    AggregationTemporality::Delta,
                    &[(60, 10), (120, 20), (180, 15)],
                ),
                build_sum_request(metric, AggregationTemporality::Cumulative, &[(180, 30)]),
            ] {
                let result = instance.metrics(request, ctx.clone()).await;
                assert!(result.is_ok(), "{metric}: {result:?}");
            }

            let mut output = instance
                .do_query(
                    &format!(
                        "SELECT COALESCE(otlp_aggregation_temporality, '') AS temporality, \
                         COUNT(*) AS samples, SUM(greptime_value) AS total \
                         FROM {table} GROUP BY otlp_aggregation_temporality ORDER BY temporality"
                    ),
                    ctx.clone(),
                )
                .await;
            let OutputData::Stream(stream) = output.remove(0).unwrap().data else {
                unreachable!()
            };
            assert_eq!(
                RecordBatches::try_collect(stream)
                    .await
                    .unwrap()
                    .pretty_print()
                    .unwrap(),
                "\
+-------------+---------+-------+
| temporality | samples | total |
+-------------+---------+-------+
|             | 2       | 40.0  |
| delta       | 3       | 45.0  |
+-------------+---------+-------+"
            );

            for (function, expected) in [("increase", "45.0"), ("rate", "0.25")] {
                let mut output = instance
                    .do_query(
                        &format!("TQL EVAL (180, 180, '1m') {function}({table}[3m])"),
                        ctx.clone(),
                    )
                    .await;
                let OutputData::Stream(stream) = output.remove(0).unwrap().data else {
                    unreachable!()
                };
                let rendered = RecordBatches::try_collect(stream)
                    .await
                    .unwrap()
                    .pretty_print()
                    .unwrap();
                assert!(rendered.contains("delta"), "{rendered}");
                assert!(rendered.contains(expected), "{rendered}");
            }

            let mut stale = build_sum_request(metric, AggregationTemporality::Delta, &[(240, 99)]);
            let Some(metric::Data::Sum(sum)) = stale.resource_metrics[0].scope_metrics[0].metrics
                [0]
            .data
            .as_mut() else {
                unreachable!()
            };
            sum.data_points[0].flags = DataPointFlags::NoRecordedValueMask as u32;
            assert!(instance.metrics(stale, ctx.clone()).await.is_ok());

            for (matcher, expected_rows) in [
                ("otlp_aggregation_temporality=\"delta\"", 0),
                ("otlp_aggregation_temporality!=\"delta\"", 1),
            ] {
                let mut output = instance
                    .do_query(
                        &format!("TQL EVAL (240, 240, '1m') {table}{{{matcher}}}"),
                        ctx.clone(),
                    )
                    .await;
                let OutputData::Stream(stream) = output.remove(0).unwrap().data else {
                    unreachable!()
                };
                let batches = RecordBatches::try_collect(stream).await.unwrap();
                assert_eq!(
                    expected_rows,
                    batches.iter().map(|batch| batch.num_rows()).sum::<usize>()
                );
                if expected_rows == 1 {
                    assert!(batches.pretty_print().unwrap().contains("30.0"));
                }
            }
        }

        let malformed = ExportMetricsServiceRequest {
            resource_metrics: vec![ResourceMetrics {
                scope_metrics: vec![ScopeMetrics {
                    metrics: vec![Metric {
                        name: "rejected.delta.histogram".to_string(),
                        data: Some(metric::Data::Histogram(Histogram {
                            data_points: vec![HistogramDataPoint {
                                count: 1,
                                bucket_counts: vec![1],
                                explicit_bounds: vec![1.0, 2.0],
                                ..Default::default()
                            }],
                            aggregation_temporality: AggregationTemporality::Delta as i32,
                        })),
                        ..Default::default()
                    }],
                    ..Default::default()
                }],
                ..Default::default()
            }],
        };
        let outcome = instance.metrics(malformed, ctx.clone()).await.unwrap();
        assert_eq!(0, outcome.accepted_data_points);
        assert_eq!(1, outcome.rejected_data_points);
        assert!(
            outcome
                .error_message
                .as_deref()
                .unwrap()
                .contains("bucket_counts length")
        );
        let mut output = instance
            .do_query(
                "SELECT COUNT(*) FROM information_schema.tables WHERE table_name IN \
                 ('rejected_delta_histogram_bucket', 'rejected_delta_histogram_sum', \
                  'rejected_delta_histogram_count')",
                ctx.clone(),
            )
            .await;
        let OutputData::Stream(stream) = output.remove(0).unwrap().data else {
            unreachable!()
        };
        assert!(
            RecordBatches::try_collect(stream)
                .await
                .unwrap()
                .pretty_print()
                .unwrap()
                .contains("| 0        |")
        );

        let point = |stream: &str, seconds: u64, bounds: Vec<f64>, sum| HistogramDataPoint {
            attributes: vec![keyvalue("stream", stream)],
            time_unix_nano: seconds * 1_000_000_000,
            count: u64::try_from(bounds.len() + 1).unwrap(),
            sum,
            bucket_counts: vec![1; bounds.len() + 1],
            explicit_bounds: bounds,
            ..Default::default()
        };
        let tombstone = |stream: &str, seconds: u64, bounds: Vec<f64>, sum| HistogramDataPoint {
            flags: DataPointFlags::NoRecordedValueMask as u32,
            ..point(stream, seconds, bounds, sum)
        };
        for points in [
            vec![
                point("same", 300, vec![1.0, 2.0], Some(4.0)),
                tombstone("same", 360, vec![1.0, 2.0], None),
            ],
            vec![
                point("changed", 420, vec![3.0, 5.0], Some(8.0)),
                tombstone("changed", 480, vec![1.0, 2.0], Some(99.0)),
            ],
            vec![
                point("boundless", 420, vec![3.0, 5.0], Some(8.0)),
                tombstone("boundless", 480, vec![], None),
            ],
            vec![tombstone("new", 480, vec![], None)],
        ] {
            let outcome = instance
                .metrics(
                    build_histogram_request("raw.delta.histogram", points),
                    ctx.clone(),
                )
                .await
                .unwrap();
            assert_eq!(0, outcome.rejected_data_points);
        }

        let mut output = instance
            .do_query(
                "TQL EVAL (300, 300, '1m') \
                 histogram_quantile(0.5, rate(raw_delta_histogram_bucket[2m]))",
                ctx.clone(),
            )
            .await;
        let OutputData::Stream(stream) = output.remove(0).unwrap().data else {
            unreachable!()
        };
        let rendered = RecordBatches::try_collect(stream)
            .await
            .unwrap()
            .pretty_print()
            .unwrap();
        assert!(rendered.contains("1.5"), "{rendered}");

        for (query, expected_rows) in [
            ("raw_delta_histogram_bucket{stream=\"same\",le=~\"1|2\"}", 0),
            ("raw_delta_histogram_sum{stream=\"same\"}", 1),
            (
                "raw_delta_histogram_bucket{stream=\"changed\",le=~\"3|5\"}",
                2,
            ),
            ("raw_delta_histogram_sum{stream=\"changed\"}", 0),
            (
                "raw_delta_histogram_bucket{stream=\"boundless\",le=~\"3|5\"}",
                2,
            ),
        ] {
            let mut output = instance
                .do_query(&format!("TQL EVAL (480, 480, '1m') {query}"), ctx.clone())
                .await;
            let OutputData::Stream(stream) = output.remove(0).unwrap().data else {
                unreachable!()
            };
            let batches = RecordBatches::try_collect(stream).await.unwrap();
            assert_eq!(
                expected_rows,
                batches.iter().map(|batch| batch.num_rows()).sum::<usize>(),
                "{query}: {}",
                batches.pretty_print().unwrap()
            );
        }

        let mut output = instance
            .do_query(
                "SELECT \
                   (SELECT COUNT(*) FROM raw_delta_histogram_bucket WHERE \"stream\" = 'new') AS buckets, \
                   (SELECT COUNT(*) FROM raw_delta_histogram_count WHERE \"stream\" = 'new') AS counts, \
                   (SELECT COUNT(*) FROM raw_delta_histogram_sum WHERE \"stream\" = 'new') AS sums",
                ctx.clone(),
            )
            .await;
        let OutputData::Stream(stream) = output.remove(0).unwrap().data else {
            unreachable!()
        };
        let rendered = RecordBatches::try_collect(stream)
            .await
            .unwrap()
            .pretty_print()
            .unwrap();
        assert!(
            rendered.contains("| 1       | 1      | 0    |"),
            "{rendered}"
        );

        let mut output = instance
            .do_query(
                "SELECT * FROM my_test_metric_my_ignored_unit ORDER BY greptime_timestamp",
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
+----------------+---------------------+----------------+
| container_name | greptime_timestamp  | greptime_value |
+----------------+---------------------+----------------+
| testserver     | 1970-01-01T00:00:00 | 105.0          |
| testsevrer     | 1970-01-01T00:00:00 | 100.0          |
+----------------+---------------------+----------------+",
        );

        let mut output = instance
            .do_query(
                "SELECT le, greptime_value FROM my_test_histo_my_ignored_unit_bucket order by le",
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
            .do_query(
                "SELECT * FROM my_test_histo_my_ignored_unit_sum",
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
+------------+---------------------+----------------+
| host       | greptime_timestamp  | greptime_value |
+------------+---------------------+----------------+
| testserver | 1970-01-01T00:00:00 | 51.0           |
+------------+---------------------+----------------+",
        );

        let mut output = instance
            .do_query(
                "SELECT * FROM my_test_histo_my_ignored_unit_count",
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
+------------+---------------------+----------------+
| host       | greptime_timestamp  | greptime_value |
+------------+---------------------+----------------+
| testserver | 1970-01-01T00:00:00 | 4.0            |
+------------+---------------------+----------------+",
        );
    }

    fn build_request() -> ExportMetricsServiceRequest {
        let data_points = vec![
            NumberDataPoint {
                attributes: vec![keyvalue("container.name", "testsevrer")],
                time_unix_nano: 100,
                value: Some(Value::AsInt(100)),
                ..Default::default()
            },
            NumberDataPoint {
                attributes: vec![keyvalue("container.name", "testserver")],
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
                            metadata: vec![],
                            data: Some(metric::Data::Gauge(gauge)),
                        },
                        Metric {
                            name: "my.test.histo".into(),
                            description: "my ignored desc".into(),
                            unit: "my ignored unit".into(),
                            metadata: vec![],
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
                    entity_refs: vec![],
                }),
                ..Default::default()
            }],
        }
    }

    fn build_sum_request(
        name: &str,
        temporality: AggregationTemporality,
        points: &[(u64, i64)],
    ) -> ExportMetricsServiceRequest {
        let data_points = points
            .iter()
            .map(|(seconds, value)| NumberDataPoint {
                attributes: vec![keyvalue("stream", "same")],
                time_unix_nano: *seconds * 1_000_000_000,
                value: Some(Value::AsInt(*value)),
                ..Default::default()
            })
            .collect();
        ExportMetricsServiceRequest {
            resource_metrics: vec![ResourceMetrics {
                scope_metrics: vec![ScopeMetrics {
                    metrics: vec![Metric {
                        name: name.to_string(),
                        data: Some(metric::Data::Sum(Sum {
                            data_points,
                            aggregation_temporality: temporality as i32,
                            is_monotonic: true,
                        })),
                        ..Default::default()
                    }],
                    ..Default::default()
                }],
                ..Default::default()
            }],
        }
    }

    fn build_histogram_request(
        name: &str,
        points: Vec<HistogramDataPoint>,
    ) -> ExportMetricsServiceRequest {
        ExportMetricsServiceRequest {
            resource_metrics: vec![ResourceMetrics {
                scope_metrics: vec![ScopeMetrics {
                    metrics: vec![Metric {
                        name: name.to_string(),
                        data: Some(metric::Data::Histogram(Histogram {
                            data_points: points,
                            aggregation_temporality: AggregationTemporality::Delta as i32,
                        })),
                        ..Default::default()
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
