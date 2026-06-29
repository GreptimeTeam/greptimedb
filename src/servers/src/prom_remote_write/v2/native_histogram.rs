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

//! Prometheus-shaped native histogram conversion for remote write v2.

use api::greptime_proto::io::prometheus::write::v2::histogram::{Count, ZeroCount};
use api::greptime_proto::io::prometheus::write::v2::{BucketSpan, Histogram};

/// Native histogram representation after converting from the remote write v2 protobuf.
#[derive(Debug, Clone, PartialEq)]
pub enum PrometheusNativeHistogram {
    /// Integer native histogram with delta-encoded bucket counts.
    Int(PrometheusHistogram),
    /// Float native histogram with absolute bucket counts.
    Float(PrometheusFloatHistogram),
}

/// Prometheus integer native histogram shape.
///
/// This mirrors Prometheus `histogram.Histogram`: bucket values are sparse and
/// delta-style, where the first bucket value is absolute and subsequent bucket
/// values are deltas from the previous bucket.
#[derive(Debug, Clone, PartialEq)]
pub struct PrometheusHistogram {
    /// Counter reset hint from the remote write histogram.
    pub reset_hint: i32,
    /// Bucket schema. Standard exponential schemas use `-4..=8`; custom buckets use `-53`.
    pub schema: i32,
    /// Width of the zero bucket.
    pub zero_threshold: f64,
    /// Count of observations in the zero bucket.
    pub zero_count: u64,
    /// Total count of observations.
    pub count: u64,
    /// Sum of observations.
    pub sum: f64,
    /// Sparse spans for positive buckets.
    pub positive_spans: Vec<PrometheusHistogramSpan>,
    /// Positive bucket counts in Prometheus integer delta form.
    pub positive_buckets: Vec<i64>,
    /// Sparse spans for negative buckets.
    pub negative_spans: Vec<PrometheusHistogramSpan>,
    /// Negative bucket counts in Prometheus integer delta form.
    pub negative_buckets: Vec<i64>,
    /// Custom bucket upper bounds for custom-bucket schema histograms.
    pub custom_values: Vec<f64>,
}

/// Prometheus float native histogram shape.
///
/// This mirrors Prometheus `histogram.FloatHistogram`: bucket values are sparse
/// and absolute counts, not deltas.
#[derive(Debug, Clone, PartialEq)]
pub struct PrometheusFloatHistogram {
    /// Counter reset hint from the remote write histogram.
    pub reset_hint: i32,
    /// Bucket schema. Standard exponential schemas use `-4..=8`; custom buckets use `-53`.
    pub schema: i32,
    /// Width of the zero bucket.
    pub zero_threshold: f64,
    /// Count of observations in the zero bucket.
    pub zero_count: f64,
    /// Total count of observations.
    pub count: f64,
    /// Sum of observations.
    pub sum: f64,
    /// Sparse spans for positive buckets.
    pub positive_spans: Vec<PrometheusHistogramSpan>,
    /// Positive bucket counts in absolute-count form.
    pub positive_buckets: Vec<f64>,
    /// Sparse spans for negative buckets.
    pub negative_spans: Vec<PrometheusHistogramSpan>,
    /// Negative bucket counts in absolute-count form.
    pub negative_buckets: Vec<f64>,
    /// Custom bucket upper bounds for custom-bucket schema histograms.
    pub custom_values: Vec<f64>,
}

/// A sparse bucket span.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct PrometheusHistogramSpan {
    /// Gap to the previous span, or starting bucket index for the first span.
    pub offset: i32,
    /// Number of consecutive buckets covered by this span.
    pub length: u32,
}

/// Converts a remote write v2 histogram into the matching Prometheus histogram shape.
pub fn convert_native_histogram(histogram: &Histogram) -> PrometheusNativeHistogram {
    match to_int_histogram(histogram) {
        Some(histogram) => PrometheusNativeHistogram::Int(histogram),
        None => PrometheusNativeHistogram::Float(to_float_histogram(histogram)),
    }
}

/// Converts an integer remote write v2 histogram to Prometheus integer histogram shape.
///
/// Returns `None` for float histograms, matching Prometheus `ToIntHistogram()`.
pub fn to_int_histogram(histogram: &Histogram) -> Option<PrometheusHistogram> {
    if is_float_histogram(histogram) {
        return None;
    }

    Some(PrometheusHistogram {
        reset_hint: histogram.reset_hint,
        schema: histogram.schema,
        zero_threshold: histogram.zero_threshold,
        zero_count: zero_count_int(histogram),
        count: count_int(histogram),
        sum: histogram.sum,
        positive_spans: spans_proto_to_spans(&histogram.positive_spans),
        positive_buckets: histogram.positive_deltas.clone(),
        negative_spans: spans_proto_to_spans(&histogram.negative_spans),
        negative_buckets: histogram.negative_deltas.clone(),
        custom_values: histogram.custom_values.clone(),
    })
}

/// Converts a remote write v2 histogram to Prometheus float histogram shape.
///
/// Float histograms keep their absolute bucket counts. Integer histograms are
/// converted by accumulating bucket deltas into absolute counts, as Prometheus
/// does in `ToFloatHistogram()`.
pub fn to_float_histogram(histogram: &Histogram) -> PrometheusFloatHistogram {
    let (zero_count, count, positive_buckets, negative_buckets) = if is_float_histogram(histogram) {
        (
            zero_count_float(histogram),
            count_float(histogram),
            histogram.positive_counts.clone(),
            histogram.negative_counts.clone(),
        )
    } else {
        (
            zero_count_int(histogram) as f64,
            count_int(histogram) as f64,
            deltas_to_counts(&histogram.positive_deltas),
            deltas_to_counts(&histogram.negative_deltas),
        )
    };

    PrometheusFloatHistogram {
        reset_hint: histogram.reset_hint,
        schema: histogram.schema,
        zero_threshold: histogram.zero_threshold,
        zero_count,
        count,
        sum: histogram.sum,
        positive_spans: spans_proto_to_spans(&histogram.positive_spans),
        positive_buckets,
        negative_spans: spans_proto_to_spans(&histogram.negative_spans),
        negative_buckets,
        custom_values: histogram.custom_values.clone(),
    }
}

fn is_float_histogram(histogram: &Histogram) -> bool {
    matches!(histogram.count, Some(Count::CountFloat(_)))
}

fn count_int(histogram: &Histogram) -> u64 {
    match histogram.count {
        Some(Count::CountInt(count)) => count,
        _ => 0,
    }
}

fn count_float(histogram: &Histogram) -> f64 {
    match histogram.count {
        Some(Count::CountFloat(count)) => count,
        _ => 0.0,
    }
}

fn zero_count_int(histogram: &Histogram) -> u64 {
    match histogram.zero_count {
        Some(ZeroCount::ZeroCountInt(count)) => count,
        _ => 0,
    }
}

fn zero_count_float(histogram: &Histogram) -> f64 {
    match histogram.zero_count {
        Some(ZeroCount::ZeroCountFloat(count)) => count,
        _ => 0.0,
    }
}

fn spans_proto_to_spans(spans: &[BucketSpan]) -> Vec<PrometheusHistogramSpan> {
    spans
        .iter()
        .map(|span| PrometheusHistogramSpan {
            offset: span.offset,
            length: span.length,
        })
        .collect()
}

fn deltas_to_counts(deltas: &[i64]) -> Vec<f64> {
    let mut current = 0.0;
    deltas
        .iter()
        .map(|delta| {
            current += *delta as f64;
            current
        })
        .collect()
}
