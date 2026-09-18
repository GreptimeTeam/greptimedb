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
    use datatypes::arrow::array::AsArray;
    use datatypes::arrow::datatypes::TimestampMicrosecondType;
    use frontend::instance::Instance;
    use opentelemetry_proto::tonic::collector::trace::v1::ExportTraceServiceRequest;
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
    use pipeline::{GreptimePipelineParams, PipelineWay};
    use serde_json::json;
    use servers::query_handler::OpenTelemetryProtocolHandler;
    use servers::query_handler::sql::SqlQueryHandler;
    use session::context::QueryContext;
    use session::protocol_ctx::{OtlpMetricCtx, ProtocolCtx};

    use crate::standalone::GreptimeDbStandaloneBuilder;
    use crate::tests;

    #[tokio::test(flavor = "multi_thread")]
    async fn test_trace_v2_on_standalone() -> Result<(), Box<dyn std::error::Error>> {
        let standalone = GreptimeDbStandaloneBuilder::new("trace_v2_standalone")
            .build()
            .await;
        test_trace_v2(standalone.fe_instance()).await?;
        Ok(())
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_trace_v2_on_distributed() -> Result<(), Box<dyn std::error::Error>> {
        let distributed = tests::create_distributed_instance("trace_v2_distributed").await;
        test_trace_v2(&distributed.frontend()).await?;
        Ok(())
    }

    async fn test_trace_v2(instance: &Arc<Instance>) -> Result<(), Box<dyn std::error::Error>> {
        let mut context = QueryContext::with(DEFAULT_CATALOG_NAME, "public");
        context.set_extension(
            common_catalog::consts::TRACE_TABLE_NAME_SESSION_KEY,
            "trace_v2",
        );
        let ctx = Arc::new(context);
        let request: ExportTraceServiceRequest = serde_json::from_value(json!({
            "resourceSpans": [{
                "resource": {"attributes": [
                    {"key": "deployment.environment", "value": {"stringValue": "prod"}},
                    {"key": "cloud.region", "value": {"stringValue": "us-west"}},
                    {"key": "capacity", "value": {"intValue": "8"}}
                ]},
                "scopeSpans": [{
                    "scope": {"attributes": [
                        {"key": "sample.rate", "value": {"doubleValue": 0.5}},
                        {"key": "enabled", "value": {"boolValue": true}},
                        {"key": "config", "value": {"kvlistValue": {"values": [
                            {"key": "batch_size", "value": {"intValue": "4"}}
                        ]}}}
                    ]},
                    "spans": [{
                        "traceId": "c05d7a4ec8e1f231f02ed6e8da8655b4",
                        "spanId": "9630f2916e2f7909", "name": "op", "kind": 2,
                        "startTimeUnixNano": "1736480942444376000",
                        "endTimeUnixNano": "1736480942444499000",
                        "attributes": [
                            {"key": "http.status_code", "value": {"intValue": "200"}},
                            {"key": "latency", "value": {"doubleValue": 12.5}},
                            {"key": "empty"},
                            {"key": "bytes", "value": {"bytesValue": "AQID"}},
                            {"key": "nested", "value": {"kvlistValue": {"values": [
                                {"key": "a.b", "value": {"arrayValue": {"values": [{"boolValue": true}, {"stringValue": "ok"}]}}}
                            ]}}}
                        ]
                    }]
                }]
            }]
        }))?;
        let result = instance
            .traces(
                instance.clone(),
                request.clone(),
                PipelineWay::OtlpTraceDirectV2,
                GreptimePipelineParams::default(),
                "trace_v2".to_string(),
                ctx.clone(),
            )
            .await?;
        assert_eq!((result.accepted_spans, result.rejected_spans), (1, 0));

        // Check the complete schema created by the first export, before later writes.
        let output = instance
            .do_query("SHOW CREATE TABLE trace_v2", ctx.clone())
            .await;
        let batches = match output
            .into_iter()
            .next()
            .ok_or("Expected non-empty SQL output")??
            .data
        {
            OutputData::Stream(stream) => RecordBatches::try_collect(stream).await?,
            OutputData::RecordBatches(batches) => batches,
            OutputData::AffectedRows(_) => return Err("Expected SHOW CREATE TABLE rows".into()),
        };
        let expected = r#"CREATE TABLE IF NOT EXISTS "trace_v2" (
  "timestamp" TIMESTAMP(9) NOT NULL,
  "timestamp_end" TIMESTAMP(9) NULL,
  "duration_nano" BIGINT NULL,
  "parent_span_id" STRING NULL SKIPPING INDEX WITH(false_positive_rate = '0.01', granularity = '10240', type = 'BLOOM'),
  "trace_id" STRING NULL SKIPPING INDEX WITH(false_positive_rate = '0.01', granularity = '10240', type = 'BLOOM'),
  "span_id" STRING NULL,
  "span_kind" STRING NULL,
  "span_name" STRING NULL,
  "span_status_code" STRING NULL,
  "span_status_message" STRING NULL,
  "trace_state" STRING NULL,
  "scope_name" STRING NULL,
  "scope_version" STRING NULL,
  "service_name" STRING NULL SKIPPING INDEX WITH(false_positive_rate = '0.01', granularity = '10240', type = 'BLOOM'),
  "span_attributes" JSON2(
    max_auto_expanded_paths = 100
  ) NULL,
  "scope_attributes" JSON2(
    max_auto_expanded_paths = 100
  ) NULL,
  "resource_attributes" JSON2(
    max_auto_expanded_paths = 100
  ) NULL,
  TIME INDEX ("timestamp"),
  PRIMARY KEY ("service_name")
)
PARTITION ON COLUMNS ("trace_id") (
  trace_id < '1',
  trace_id >= '1' AND trace_id < '2',
  trace_id >= '2' AND trace_id < '3',
  trace_id >= '3' AND trace_id < '4',
  trace_id >= '4' AND trace_id < '5',
  trace_id >= '5' AND trace_id < '6',
  trace_id >= '6' AND trace_id < '7',
  trace_id >= '7' AND trace_id < '8',
  trace_id >= '8' AND trace_id < '9',
  trace_id >= '9' AND trace_id < 'a',
  trace_id >= 'a' AND trace_id < 'b',
  trace_id >= 'b' AND trace_id < 'c',
  trace_id >= 'c' AND trace_id < 'd',
  trace_id >= 'd' AND trace_id < 'e',
  trace_id >= 'e' AND trace_id < 'f',
  trace_id >= 'f'
)
ENGINE=mito
WITH(
  'comment' = 'Created on insertion',
  append_mode = 'true',
  'greptime.semantic.entity.service.id' = 'service_name',
  'greptime.semantic.pipeline' = 'greptime_trace_v2',
  'greptime.semantic.signal_type' = 'trace',
  'greptime.semantic.source' = 'opentelemetry',
  'greptime.semantic.trace.conventions' = 'unknown',
  table_data_model = 'greptime_trace_v2'
)"#;
        let batches = batches.take();
        assert_eq!(batches.len(), 1);
        assert_eq!(batches[0].num_rows(), 1);
        assert_eq!(batches[0].num_columns(), 2);
        assert_eq!(
            batches[0].iter_column_as_string(1).collect::<Vec<_>>(),
            vec![Some(expected.to_string())],
        );

        // First write has no service.name: it must still create the tag, but no auxiliary rows.
        assert_trace_v2_query(
            instance,
            "SELECT COUNT(*) = 1
             FROM trace_v2
             WHERE service_name IS NULL",
            ctx.clone(),
        )
        .await?;
        assert_trace_v2_query(
            instance,
            "SELECT COUNT(*) = 17
             FROM information_schema.columns
             WHERE table_name = 'trace_v2'",
            ctx.clone(),
        )
        .await?;
        assert_trace_v2_query(
            instance,
            "SELECT COUNT(*) = 0
             FROM information_schema.tables
             WHERE table_name IN ('trace_v2_services', 'trace_v2_operations')",
            ctx.clone(),
        )
        .await?;
        assert_trace_v2_query(
            instance,
            r#"SELECT COUNT(*) = 1
             FROM trace_v2
             WHERE span_attributes."http.status_code"::BIGINT = 200
               AND span_attributes.empty::STRING IS NULL
               AND span_attributes.bytes[1]::BIGINT = 2
               AND span_attributes.nested."a.b"[0]::BOOLEAN = true"#,
            ctx.clone(),
        )
        .await?;

        // Exercise each JSON2 column and expressions combining columns, before and after flush.
        let json_queries = [
            r#"SELECT COUNT(*) > 0 AND COUNT(*) = COUNT(CASE
                 WHEN span_attributes.latency::DOUBLE * 2 = 25
                   AND upper(span_attributes.nested."a.b"[1]::STRING) = 'OK'
                   AND span_attributes.missing::STRING IS NULL
                 THEN 1 END)
             FROM trace_v2"#,
            r#"SELECT COUNT(*) > 0 AND COUNT(*) = COUNT(CASE
                 WHEN scope_attributes."sample.rate"::DOUBLE + 0.25 = 0.75
                   AND scope_attributes.enabled::BOOLEAN
                   AND scope_attributes.config.batch_size::BIGINT * 2 = 8
                 THEN 1 END)
             FROM trace_v2"#,
            r#"SELECT COUNT(*) > 0 AND COUNT(*) = COUNT(CASE
                 WHEN upper(resource_attributes."cloud.region"::STRING) = 'US-WEST'
                   AND resource_attributes.capacity::BIGINT - 2 = 6
                 THEN 1 END)
             FROM trace_v2"#,
            r#"SELECT AVG(span_attributes.latency::DOUBLE
                        * scope_attributes."sample.rate"::DOUBLE) = 6.25
                   AND SUM(resource_attributes.capacity::BIGINT)
                       = SUM(scope_attributes.config.batch_size::BIGINT) * 2
             FROM trace_v2"#,
        ];
        for sql in json_queries {
            assert_trace_v2_query(instance, sql, ctx.clone()).await?;
        }

        let mut mixed = request.clone();
        mixed.resource_spans[0]
            .resource
            .as_mut()
            .ok_or("Expected non-empty resource in trace fixture")?
            .attributes[0]
            .key = "service.name".to_string();
        let spans = &mut mixed.resource_spans[0].scope_spans[0].spans;
        // Reject one span in the first chunk, merge its healthy rows, then write the next chunk.
        spans.resize(514, spans[0].clone());
        for (index, span) in spans.iter_mut().enumerate() {
            span.span_id = (index as u64).to_be_bytes().to_vec();
        }
        spans[0].attributes[0]
            .value
            .as_mut()
            .ok_or("Expected non-empty attribute value in trace fixture")?
            .value = Some(
            opentelemetry_proto::tonic::common::v1::any_value::Value::StringValue("ok".to_string()),
        );
        spans[511].end_time_unix_nano = u64::MAX;
        let result = instance
            .traces(
                instance.clone(),
                mixed,
                PipelineWay::OtlpTraceDirectV2,
                GreptimePipelineParams::default(),
                "trace_v2".to_string(),
                ctx.clone(),
            )
            .await?;
        assert_eq!((result.accepted_spans, result.rejected_spans), (513, 1));
        assert!(
            result
                .error_message
                .as_deref()
                .is_some_and(|s| s.contains("Timestamp overflow"))
        );
        assert_trace_v2_query(
            instance,
            "SELECT COUNT(*) = 514
             FROM trace_v2",
            ctx.clone(),
        )
        .await?;
        assert_trace_v2_query(
            instance,
            r#"SELECT COUNT(*) = 1
             FROM trace_v2
             WHERE span_attributes."http.status_code"::STRING = 'ok'"#,
            ctx.clone(),
        )
        .await?;
        assert_trace_v2_query(
            instance,
            "SELECT COUNT(*) = 1
             FROM trace_v2_services
             WHERE service_name = 'prod'",
            ctx.clone(),
        )
        .await?;
        assert_trace_v2_query(
            instance,
            "SELECT COUNT(*) = 1
             FROM trace_v2_operations
             WHERE service_name = 'prod'",
            ctx.clone(),
        )
        .await?;

        let mut invalid = request.clone();
        invalid.resource_spans[0].scope_spans[0].spans[0].start_time_unix_nano = u64::MAX;
        let result = instance
            .traces(
                instance.clone(),
                invalid,
                PipelineWay::OtlpTraceDirectV2,
                GreptimePipelineParams::default(),
                "trace_v2".to_string(),
                ctx.clone(),
            )
            .await?;
        assert_eq!((result.accepted_spans, result.rejected_spans), (0, 1));

        // A v1 request must fail before auto-ALTER can flatten attributes into this table.
        let error = instance
            .traces(
                instance.clone(),
                request.clone(),
                PipelineWay::OtlpTraceDirectV1,
                GreptimePipelineParams::default(),
                "trace_v2".to_string(),
                ctx.clone(),
            )
            .await
            .err()
            .ok_or("Expected mixed trace models to be rejected")?;
        let error = format!("{error:?}");
        assert!(
            error.contains("Trace table `trace_v2` uses greptime_trace_v2, but the request uses greptime_trace_v1"),
            "{error}",
        );
        assert_trace_v2_query(
            instance,
            "SELECT COUNT(*) = 17
             FROM information_schema.columns
             WHERE table_name = 'trace_v2'",
            ctx.clone(),
        )
        .await?;

        let mut context = (*ctx).clone();
        context.set_extension(
            common_catalog::consts::TRACE_TABLE_NAME_SESSION_KEY,
            "trace_v1",
        );
        let v1_ctx = Arc::new(context);
        // Create the v1 table with an explicit service identity for the isolation check.
        let mut v1_request = request.clone();
        v1_request.resource_spans[0]
            .resource
            .as_mut()
            .ok_or("Expected non-empty resource in trace fixture")?
            .attributes[0]
            .key = "service.name".to_string();
        let result = instance
            .traces(
                instance.clone(),
                v1_request,
                PipelineWay::OtlpTraceDirectV1,
                GreptimePipelineParams::default(),
                "trace_v1".to_string(),
                v1_ctx.clone(),
            )
            .await?;
        assert_eq!(result.accepted_spans, 1);
        let error = instance
            .traces(
                instance.clone(),
                request,
                PipelineWay::OtlpTraceDirectV2,
                GreptimePipelineParams::default(),
                "trace_v1".to_string(),
                v1_ctx,
            )
            .await
            .err()
            .ok_or("Expected mixed trace models to be rejected")?;
        let error = format!("{error:?}");
        assert!(
            error.contains("Trace table `trace_v1` uses greptime_trace_v1, but the request uses greptime_trace_v2"),
            "{error}",
        );

        let output = instance
            .do_query("ADMIN FLUSH_TABLE('trace_v2')", ctx.clone())
            .await;
        assert!(output.into_iter().all(|r| r.is_ok()));
        assert_trace_v2_query(
            instance,
            r#"SELECT COUNT(*) = 513
             FROM trace_v2
             WHERE span_attributes."http.status_code"::BIGINT = 200"#,
            ctx.clone(),
        )
        .await?;
        for sql in json_queries {
            assert_trace_v2_query(instance, sql, ctx.clone()).await?;
        }
        Ok(())
    }

    async fn assert_trace_v2_query(
        instance: &Arc<Instance>,
        sql: &str,
        ctx: Arc<QueryContext>,
    ) -> Result<(), Box<dyn std::error::Error>> {
        let output = instance.do_query(sql, ctx).await;
        let OutputData::Stream(stream) = output
            .into_iter()
            .next()
            .ok_or("Expected non-empty SQL output")??
            .data
        else {
            return Err(format!("Expected a query stream: {sql}").into());
        };
        let batches = RecordBatches::try_collect(stream).await?;
        let batches = batches.take();
        assert_eq!(batches.len(), 1, "{sql}");
        assert_eq!(batches[0].num_rows(), 1, "{sql}");
        assert_eq!(
            batches[0].column(0).as_boolean().iter().collect::<Vec<_>>(),
            vec![Some(true)],
            "{sql}"
        );
        Ok(())
    }

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

    #[tokio::test(flavor = "multi_thread")]
    pub async fn test_otlp_metrics_into_microsecond_physical_table_on_standalone() {
        let standalone = GreptimeDbStandaloneBuilder::new("test_otlp_us_physical")
            .build()
            .await;

        test_otlp_metrics_into_microsecond_physical_table(standalone.fe_instance()).await;
    }

    #[tokio::test(flavor = "multi_thread")]
    pub async fn test_otlp_metrics_into_microsecond_physical_table_on_distributed() {
        let instance = tests::create_distributed_instance("test_otlp_us_physical_dist").await;

        test_otlp_metrics_into_microsecond_physical_table(&instance.frontend()).await;
    }

    /// Regression test for <https://github.com/GreptimeTeam/greptimedb/issues/9231>:
    /// a physical metric table pre-created with a TIMESTAMP(6) (microsecond)
    /// time index accepts OTLP ingestion; nanosecond samples are converted to
    /// the physical table's unit, keeping microsecond precision.
    async fn test_otlp_metrics_into_microsecond_physical_table(instance: &Arc<Instance>) {
        let db = "otlp_us_physical";
        let mut ctx = QueryContext::with(DEFAULT_CATALOG_NAME, db);
        // Route the request to the metric engine, like the OTLP HTTP handler
        // does with the default `prom_store.with_metric_engine = true`.
        ctx.set_protocol_ctx(ProtocolCtx::OtlpMetric(OtlpMetricCtx {
            with_metric_engine: true,
            ..Default::default()
        }));
        let ctx = Arc::new(ctx);
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

        let request = ExportMetricsServiceRequest {
            resource_metrics: vec![ResourceMetrics {
                scope_metrics: vec![ScopeMetrics {
                    metrics: vec![Metric {
                        name: "my_gauge".to_string(),
                        data: Some(metric::Data::Gauge(Gauge {
                            data_points: vec![NumberDataPoint {
                                attributes: vec![keyvalue("host", "h1")],
                                time_unix_nano: 1_704_067_200_123_456_789,
                                value: Some(Value::AsDouble(1.0)),
                                ..Default::default()
                            }],
                        })),
                        ..Default::default()
                    }],
                    ..Default::default()
                }],
                ..Default::default()
            }],
        };
        instance.metrics(request, ctx.clone()).await.unwrap();

        let mut output = instance
            .do_query("SELECT greptime_timestamp FROM my_gauge", ctx.clone())
            .await;
        let OutputData::Stream(stream) = output.remove(0).unwrap().data else {
            unreachable!()
        };
        let batches = RecordBatches::try_collect(stream).await.unwrap().take();
        assert_eq!(batches[0].num_rows(), 1);
        // The logical table's time index keeps the physical table's
        // microsecond unit and precision: 1704067200123456789ns ->
        // 1704067200123456us.
        let timestamps = batches[0]
            .column(0)
            .as_primitive::<TimestampMicrosecondType>();
        assert_eq!(timestamps.value(0), 1_704_067_200_123_456);
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
