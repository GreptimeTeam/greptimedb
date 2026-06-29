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

use api::greptime_proto::io::prometheus::write::v2::histogram::{Count, ZeroCount};
use api::greptime_proto::io::prometheus::write::v2::{BucketSpan, Histogram, Metadata};
use api::v1::value::ValueData;
use api::v1::{ColumnSchema, Row};
use bytes::Bytes;
use servers::prom_remote_write::v2::native_histogram::{
    PrometheusFloatHistogram, PrometheusHistogram, PrometheusHistogramSpan,
    PrometheusNativeHistogram, convert_native_histogram, to_float_histogram, to_int_histogram,
};
use servers::prom_remote_write::v2::test_util as remote_write_v2;

#[test]
fn test_decode_remote_write_v2_native_histogram_dump() {
    const BODY: &[u8] = include_bytes!(
        "testdata/prom_remote_write/remote_write_v2_native_hist_1782358162264510000.raw.snappy"
    );
    const TEXT: &str = include_str!(
        "testdata/prom_remote_write/remote_write_v2_native_hist_1782358162264510000.txt"
    );

    let decoded = remote_write_v2::decode_request(false, Bytes::from_static(BODY)).unwrap();

    assert_text_dump_shape(TEXT, 1, 0, 1, 0, 1);
    assert!(TEXT.contains("received_from_a_http_request_duration_seconds"));
    assert!(TEXT.contains("positive_spans: <"));
    assert_eq!(
        decoded.symbols,
        vec![
            "",
            "__name__",
            "received_from_a_http_request_duration_seconds",
            "handler",
            "/api/v1/write",
            "instance",
            "localhost:9090",
            "job",
            "prometheus",
            "source_prometheus",
            "A",
        ]
    );
    assert_eq!(decoded.timeseries.len(), 1);

    let series = &decoded.timeseries[0];
    assert_eq!(series.labels_refs, vec![1, 2, 3, 4, 5, 6, 7, 8, 9, 10]);
    assert!(series.samples.is_empty());
    assert!(series.exemplars.is_empty());
    assert_eq!(series.metadata, Some(Metadata::default()));
    assert_eq!(series.histograms.len(), 1);

    let histogram = &series.histograms[0];
    assert_eq!(histogram.count, Some(Count::CountInt(24)));
    assert_eq!(histogram.sum, 0.205418879);
    assert_eq!(histogram.schema, 3);
    assert_eq!(histogram.zero_threshold, 2.938735877055719e-39);
    assert_eq!(histogram.zero_count, Some(ZeroCount::ZeroCountInt(0)));
    assert_eq!(histogram.positive_spans.len(), 3);
    assert_eq!(histogram.positive_spans[0].offset, -63);
    assert_eq!(histogram.positive_spans[0].length, 1);
    assert_eq!(histogram.positive_spans[1].offset, 1);
    assert_eq!(histogram.positive_spans[1].length, 14);
    assert_eq!(histogram.positive_spans[2].offset, 5);
    assert_eq!(histogram.positive_spans[2].length, 1);
    assert_eq!(
        histogram.positive_deltas,
        vec![2, -1, 3, -3, 3, -3, 0, 0, 0, 0, 0, 0, 0, 1, -1, 0]
    );
    assert_eq!(histogram.timestamp, 1782358160412);

    assert_eq!(
        convert_native_histogram(histogram),
        PrometheusNativeHistogram::Int(PrometheusHistogram {
            reset_hint: 0,
            schema: 3,
            zero_threshold: 2.938735877055719e-39,
            zero_count: 0,
            count: 24,
            sum: 0.205418879,
            positive_spans: vec![
                PrometheusHistogramSpan {
                    offset: -63,
                    length: 1,
                },
                PrometheusHistogramSpan {
                    offset: 1,
                    length: 14,
                },
                PrometheusHistogramSpan {
                    offset: 5,
                    length: 1,
                },
            ],
            positive_buckets: vec![2, -1, 3, -3, 3, -3, 0, 0, 0, 0, 0, 0, 0, 1, -1, 0],
            negative_spans: Vec::new(),
            negative_buckets: Vec::new(),
            custom_values: Vec::new(),
        })
    );
    assert_eq!(
        to_float_histogram(histogram).positive_buckets,
        vec![
            2.0, 1.0, 4.0, 1.0, 4.0, 1.0, 1.0, 1.0, 1.0, 1.0, 1.0, 1.0, 1.0, 2.0, 1.0, 1.0
        ]
    );

    let (sample_inserts, histogram_inserts, sample_count, histogram_count) =
        remote_write_v2::write_requests(decoded).unwrap();
    assert!(sample_inserts.is_empty());
    assert_eq!(sample_count, 0);
    assert_eq!(histogram_count, 1);
    assert_eq!(histogram_inserts.len(), 1);

    let insert = &histogram_inserts[0];
    assert_eq!(
        insert.table_name,
        "received_from_a_http_request_duration_seconds_native_histogram"
    );
    let rows = insert.rows.as_ref().unwrap();
    assert_eq!(rows.rows.len(), 1);
    let row = &rows.rows[0];
    assert_eq!(
        row.values[column_index(&rows.schema, "greptime_timestamp")].value_data,
        Some(ValueData::TimestampMillisecondValue(1782358160412))
    );
    assert_eq!(
        row.values[column_index(&rows.schema, "schema")].value_data,
        Some(ValueData::I32Value(3))
    );
    assert_eq!(
        row.values[column_index(&rows.schema, "count_u64")].value_data,
        Some(ValueData::U64Value(24))
    );
    assert_eq!(
        row.values[column_index(&rows.schema, "greptime_histogram_type")].value_data,
        Some(ValueData::StringValue("int".to_string()))
    );
    assert_eq!(
        list_i32_values(row, column_index(&rows.schema, "positive_span_offsets")),
        vec![-63, 1, 5]
    );
    assert_eq!(
        list_i64_values(row, column_index(&rows.schema, "positive_buckets_i64")),
        vec![2, -1, 3, -3, 3, -3, 0, 0, 0, 0, 0, 0, 0, 1, -1, 0]
    );
    assert_eq!(
        row.values[column_index(&rows.schema, "positive_buckets_f64")].value_data,
        None
    );
}

#[test]
fn test_convert_remote_write_v2_float_native_histogram() {
    let histogram = Histogram {
        count: Some(Count::CountFloat(3.5)),
        sum: 10.0,
        schema: 2,
        zero_threshold: 1e-128,
        zero_count: Some(ZeroCount::ZeroCountFloat(0.5)),
        negative_spans: vec![BucketSpan {
            offset: -2,
            length: 1,
        }],
        negative_counts: vec![1.25],
        positive_spans: vec![BucketSpan {
            offset: 3,
            length: 2,
        }],
        positive_counts: vec![2.0, 3.5],
        reset_hint: 3,
        timestamp: 1234,
        custom_values: vec![0.5, 1.5],
        ..Default::default()
    };

    assert!(to_int_histogram(&histogram).is_none());
    assert_eq!(
        convert_native_histogram(&histogram),
        PrometheusNativeHistogram::Float(PrometheusFloatHistogram {
            reset_hint: 3,
            schema: 2,
            zero_threshold: 1e-128,
            zero_count: 0.5,
            count: 3.5,
            sum: 10.0,
            positive_spans: vec![PrometheusHistogramSpan {
                offset: 3,
                length: 2,
            }],
            positive_buckets: vec![2.0, 3.5],
            negative_spans: vec![PrometheusHistogramSpan {
                offset: -2,
                length: 1,
            }],
            negative_buckets: vec![1.25],
            custom_values: vec![0.5, 1.5],
        })
    );
}

fn assert_text_dump_shape(
    text: &str,
    timeseries: usize,
    samples: usize,
    histograms: usize,
    exemplars: usize,
    metadata: usize,
) {
    assert_eq!(count_textproto_block(text, "timeseries: <"), timeseries);
    assert_eq!(count_textproto_block(text, "samples: <"), samples);
    assert_eq!(count_textproto_block(text, "histograms: <"), histograms);
    assert_eq!(count_textproto_block(text, "exemplars: <"), exemplars);
    assert_eq!(count_textproto_block(text, "metadata: <"), metadata);
}

fn count_textproto_block(text: &str, block: &str) -> usize {
    text.lines().filter(|line| line.trim() == block).count()
}

fn column_index(schema: &[ColumnSchema], column_name: &str) -> usize {
    schema
        .iter()
        .position(|column| column.column_name == column_name)
        .unwrap()
}

fn list_i32_values(row: &Row, column_idx: usize) -> Vec<i32> {
    let Some(ValueData::ListValue(list)) = &row.values[column_idx].value_data else {
        panic!("expected list value");
    };
    list.items
        .iter()
        .map(|value| {
            let Some(ValueData::I32Value(value)) = value.value_data.as_ref() else {
                panic!("expected i32 list item");
            };
            *value
        })
        .collect()
}

fn list_i64_values(row: &Row, column_idx: usize) -> Vec<i64> {
    let Some(ValueData::ListValue(list)) = &row.values[column_idx].value_data else {
        panic!("expected list value");
    };
    list.items
        .iter()
        .map(|value| {
            let Some(ValueData::I64Value(value)) = value.value_data.as_ref() else {
                panic!("expected i64 list item");
            };
            *value
        })
        .collect()
}
