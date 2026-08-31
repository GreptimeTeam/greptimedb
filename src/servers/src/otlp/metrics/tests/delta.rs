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

use otel_arrow_rust::proto::opentelemetry::common::v1::InstrumentationScope;

use super::*;

fn table_rows<'a>(request: &'a RowInsertRequests, table: &str) -> &'a api::v1::Rows {
    request
        .inserts
        .iter()
        .find(|insert| insert.table_name == table)
        .unwrap_or_else(|| panic!("missing table {table}"))
        .rows
        .as_ref()
        .unwrap()
}

fn column_index(rows: &api::v1::Rows, column: &str) -> usize {
    rows.schema
        .iter()
        .position(|schema| schema.column_name == column)
        .unwrap_or_else(|| panic!("missing column {column}"))
}

#[test]
fn test_raw_delta_sum_identity_and_stale_marker() {
    set_default_prefix(Some("custom")).unwrap();
    assert_eq!(greptime_temporality_label(), "__custom_temporality__");

    let points = vec![
        NumberDataPoint {
            attributes: vec![keyvalue("host", "a")],
            time_unix_nano: 1_000_000,
            value: Some(Value::AsInt(10)),
            ..Default::default()
        },
        NumberDataPoint {
            attributes: vec![keyvalue("host", "a")],
            time_unix_nano: 2_000_000,
            value: Some(Value::AsDouble(20.5)),
            ..Default::default()
        },
        NumberDataPoint {
            attributes: vec![keyvalue("host", "a")],
            time_unix_nano: 3_000_000,
            value: Some(Value::AsDouble(99.0)),
            flags: DataPointFlags::NoRecordedValueMask as u32,
            ..Default::default()
        },
    ];
    let metric = Metric {
        name: "requests".to_string(),
        data: Some(metric::Data::Sum(Sum {
            data_points: points,
            aggregation_temporality: AggregationTemporality::Delta as i32,
            is_monotonic: true,
        })),
        ..Default::default()
    };
    let conversion =
        to_grpc_insert_requests(metrics_request(vec![metric]), &mut OtlpMetricCtx::default())
            .unwrap();

    assert_eq!(3, conversion.outcome.accepted_data_points);
    assert_eq!(0, conversion.outcome.rejected_data_points);
    let rows = table_rows(&conversion.requests, "requests_total");
    let value = column_index(rows, greptime_value());
    let temporality = column_index(rows, greptime_temporality_label());
    let host = column_index(rows, "host");
    let values = rows
        .rows
        .iter()
        .map(|row| row.values[value].value_data.as_ref().unwrap())
        .collect::<Vec<_>>();
    assert_eq!(Some(&ValueData::F64Value(10.0)), values.first().copied());
    assert_eq!(Some(&ValueData::F64Value(20.5)), values.get(1).copied());
    let ValueData::F64Value(stale) = values[2] else {
        panic!("expected stale float")
    };
    assert_eq!(PROMETHEUS_STALE_NAN_BITS, stale.to_bits());
    for row in &rows.rows {
        assert_eq!(
            Some(&ValueData::StringValue(
                GREPTIME_TEMPORALITY_DELTA.to_string()
            )),
            row.values[temporality].value_data.as_ref()
        );
        assert_eq!(
            Some(&ValueData::StringValue("a".to_string())),
            row.values[host].value_data.as_ref()
        );
    }

    for temporality in [
        AggregationTemporality::Cumulative as i32,
        AggregationTemporality::Unspecified as i32,
    ] {
        let metric = Metric {
            name: format!("sum_{temporality}"),
            data: Some(metric::Data::Sum(Sum {
                data_points: vec![NumberDataPoint::default()],
                aggregation_temporality: temporality,
                ..Default::default()
            })),
            ..Default::default()
        };
        let conversion =
            to_grpc_insert_requests(metrics_request(vec![metric]), &mut OtlpMetricCtx::default())
                .unwrap();
        assert!(
            conversion.requests.inserts[0]
                .rows
                .as_ref()
                .unwrap()
                .schema
                .iter()
                .all(|column| column.column_name != greptime_temporality_label())
        );
    }
}

#[test]
fn test_delta_histogram_partial_rejection_and_inline_tombstone() {
    let valid = HistogramDataPoint {
        time_unix_nano: 1_000_000,
        count: 10,
        sum: Some(12.0),
        bucket_counts: vec![2, 3, 5],
        explicit_bounds: vec![1.0, 2.0],
        ..Default::default()
    };
    let malformed = HistogramDataPoint {
        time_unix_nano: 2_000_000,
        count: 10,
        bucket_counts: vec![10],
        explicit_bounds: vec![1.0, 2.0],
        ..Default::default()
    };
    let tombstone = HistogramDataPoint {
        time_unix_nano: 3_000_000,
        count: u64::MAX,
        sum: Some(99.0),
        bucket_counts: vec![u64::MAX, 1],
        explicit_bounds: vec![3.0, 2.0],
        flags: DataPointFlags::NoRecordedValueMask as u32,
        ..Default::default()
    };
    let metric = Metric {
        name: "latency".to_string(),
        data: Some(metric::Data::Histogram(Histogram {
            data_points: vec![valid, malformed, tombstone],
            aggregation_temporality: AggregationTemporality::Delta as i32,
        })),
        ..Default::default()
    };
    let conversion =
        to_grpc_insert_requests(metrics_request(vec![metric]), &mut OtlpMetricCtx::default())
            .unwrap();

    assert_eq!(2, conversion.outcome.accepted_data_points);
    assert_eq!(1, conversion.outcome.rejected_data_points);
    assert!(
        conversion
            .outcome
            .error_message
            .unwrap()
            .contains("bucket_counts length")
    );
    let buckets = table_rows(&conversion.requests, "latency_bucket");
    assert_eq!(6, buckets.rows.len());
    let value = column_index(buckets, greptime_value());
    let le = column_index(buckets, HISTOGRAM_LE_COLUMN);
    let temporality = column_index(buckets, greptime_temporality_label());
    let ordinary = buckets.rows[..3]
        .iter()
        .map(|row| row.values[value].value_data.clone())
        .collect::<Vec<_>>();
    assert_eq!(
        vec![
            Some(ValueData::F64Value(2.0)),
            Some(ValueData::F64Value(5.0)),
            Some(ValueData::F64Value(10.0)),
        ],
        ordinary
    );
    let tombstone_bounds = buckets.rows[3..]
        .iter()
        .map(|row| row.values[le].value_data.clone())
        .collect::<Vec<_>>();
    assert_eq!(
        vec![
            Some(ValueData::StringValue("3".to_string())),
            Some(ValueData::StringValue("2".to_string())),
            Some(ValueData::StringValue("inf".to_string())),
        ],
        tombstone_bounds
    );
    for row in &buckets.rows {
        assert_eq!(
            Some(&ValueData::StringValue(
                GREPTIME_TEMPORALITY_DELTA.to_string()
            )),
            row.values[temporality].value_data.as_ref()
        );
    }
    for row in &buckets.rows[3..] {
        let Some(ValueData::F64Value(value)) = row.values[value].value_data else {
            panic!("expected stale float")
        };
        assert_eq!(PROMETHEUS_STALE_NAN_BITS, value.to_bits());
    }
    for table in ["latency_sum", "latency_count"] {
        let rows = table_rows(&conversion.requests, table);
        assert_eq!(2, rows.rows.len());
        let value = column_index(rows, greptime_value());
        let Some(ValueData::F64Value(stale)) = rows.rows[1].values[value].value_data else {
            panic!("expected stale float")
        };
        assert_eq!(PROMETHEUS_STALE_NAN_BITS, stale.to_bits());
    }
}

#[test]
fn test_histogram_validation_preserves_supported_siblings() {
    for temporality in [
        AggregationTemporality::Delta,
        AggregationTemporality::Cumulative,
    ] {
        let overflow = HistogramDataPoint {
            count: u64::MAX,
            bucket_counts: vec![u64::MAX, 1],
            explicit_bounds: vec![1.0],
            ..Default::default()
        };
        let count_only = HistogramDataPoint {
            count: 2,
            sum: Some(3.0),
            ..Default::default()
        };
        let metric = Metric {
            name: format!("hist_{temporality:?}"),
            data: Some(metric::Data::Histogram(Histogram {
                data_points: vec![overflow, count_only],
                aggregation_temporality: temporality as i32,
            })),
            ..Default::default()
        };
        let conversion =
            to_grpc_insert_requests(metrics_request(vec![metric]), &mut OtlpMetricCtx::default())
                .unwrap();
        assert_eq!(1, conversion.outcome.accepted_data_points);
        assert_eq!(1, conversion.outcome.rejected_data_points);
        assert!(
            conversion
                .outcome
                .error_message
                .unwrap()
                .contains("overflows u64")
        );
        assert_eq!(
            1,
            conversion
                .requests
                .inserts
                .iter()
                .find(|insert| { insert.table_name.ends_with(COUNT_TABLE_SUFFIX) })
                .unwrap()
                .rows
                .as_ref()
                .unwrap()
                .rows
                .len()
        );
    }
}

#[test]
fn test_reserved_temporality_label_uses_final_persisted_key() {
    set_default_prefix(Some("custom")).unwrap();

    let gauge = |attributes: Vec<KeyValue>| Metric {
        name: "gauge".to_string(),
        data: Some(metric::Data::Gauge(Gauge {
            data_points: vec![NumberDataPoint {
                attributes,
                ..Default::default()
            }],
        })),
        ..Default::default()
    };
    let error = to_grpc_insert_requests(
        metrics_request(vec![gauge(vec![keyvalue(
            greptime_temporality_label(),
            "user",
        )])]),
        &mut OtlpMetricCtx::default(),
    )
    .unwrap_err();
    assert!(matches!(error, error::Error::InvalidOtlpMetricInput { .. }));

    let mut request = metrics_request(vec![gauge(vec![])]);
    request.resource_metrics[0].scope_metrics[0].scope = Some(InstrumentationScope {
        attributes: vec![keyvalue(greptime_temporality_label(), "safe")],
        ..Default::default()
    });
    let mut ctx = OtlpMetricCtx {
        promote_scope_attrs: true,
        ..Default::default()
    };
    let conversion = to_grpc_insert_requests(request, &mut ctx).unwrap();
    assert!(
        column_names(&conversion.requests, "gauge")
            .contains(&format!("otel_scope_{}", greptime_temporality_label()))
    );
}

#[test]
fn test_histogram_semantics_follow_only_emitted_rows() {
    let cumulative = histogram_metric("latency");
    let mut delta = histogram_metric("latency");
    let Some(metric::Data::Histogram(histogram)) = delta.data.as_mut() else {
        unreachable!()
    };
    histogram.aggregation_temporality = AggregationTemporality::Delta as i32;

    let conversion = to_grpc_insert_requests(
        metrics_request(vec![cumulative.clone(), delta.clone()]),
        &mut OtlpMetricCtx::default(),
    )
    .unwrap();
    let semantics = decode(&conversion.semantic_index);
    for table in ["latency_bucket", "latency_sum", "latency_count"] {
        assert_eq!(
            Some("mixed"),
            semantics[table]
                .get(SEMANTIC_METRIC_TEMPORALITY)
                .map(String::as_str)
        );
    }

    let Some(metric::Data::Histogram(histogram)) = delta.data.as_mut() else {
        unreachable!()
    };
    histogram.data_points[0].explicit_bounds = vec![1.0];
    let conversion = to_grpc_insert_requests(
        metrics_request(vec![cumulative, delta]),
        &mut OtlpMetricCtx::default(),
    )
    .unwrap();
    assert_eq!(1, conversion.outcome.rejected_data_points);
    let semantics = decode(&conversion.semantic_index);
    for table in ["latency_bucket", "latency_sum", "latency_count"] {
        assert_eq!(
            Some(METRIC_TEMPORALITY_CUMULATIVE),
            semantics[table]
                .get(SEMANTIC_METRIC_TEMPORALITY)
                .map(String::as_str)
        );
    }
}
