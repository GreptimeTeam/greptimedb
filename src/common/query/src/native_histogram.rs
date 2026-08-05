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

//! Shared native histogram field contract.
//!
//! Prom remote-write v2 stores these names as children of one Struct field, while
//! metric-engine uses the same contract to recognize native histogram tables.
//! [`NativeHistogram`] is the query-time representation and therefore normalizes
//! integer and floating-point payloads to absolute `f64` counts.

use std::collections::BTreeMap;
use std::sync::Arc;

use datafusion::arrow::array::{
    Array, ArrayRef, Float64Array, Int32Array, ListArray, PrimitiveArray, StructArray,
    TimestampMillisecondArray, UInt64Array,
};
use datafusion::arrow::buffer::NullBuffer;
use datafusion::arrow::datatypes::{
    ArrowPrimitiveType, DataType as ArrowDataType, Field, Float64Type, Int32Type, Int64Type,
    TimestampMillisecondType, UInt32Type, UInt64Type,
};
use datafusion_common::{DataFusionError, Result as DfResult};
use datatypes::data_type::{ConcreteDataType, DataType};
use datatypes::types::{StructField, StructType};
use once_cell::sync::Lazy;

use crate::prelude::greptime_native_histogram;
use crate::prometheus::format_prometheus_float;

pub const NATIVE_HISTOGRAM_FIELD: &str = "greptime_native_histogram";
pub const SCHEMA_FIELD: &str = "schema";
pub const ZERO_THRESHOLD_FIELD: &str = "zero_threshold";
pub const SUM_FIELD: &str = "sum";
pub const RESET_HINT_FIELD: &str = "reset_hint";
pub const START_TIMESTAMP_FIELD: &str = "start_timestamp";
pub const CUSTOM_VALUES_FIELD: &str = "custom_values";
pub const POSITIVE_SPAN_OFFSETS_FIELD: &str = "positive_span_offsets";
pub const POSITIVE_SPAN_LENGTHS_FIELD: &str = "positive_span_lengths";
pub const NEGATIVE_SPAN_OFFSETS_FIELD: &str = "negative_span_offsets";
pub const NEGATIVE_SPAN_LENGTHS_FIELD: &str = "negative_span_lengths";
pub const COUNT_U64_FIELD: &str = "count_u64";
pub const ZERO_COUNT_U64_FIELD: &str = "zero_count_u64";
pub const POSITIVE_BUCKETS_I64_FIELD: &str = "positive_buckets_i64";
pub const NEGATIVE_BUCKETS_I64_FIELD: &str = "negative_buckets_i64";
pub const COUNT_F64_FIELD: &str = "count_f64";
pub const ZERO_COUNT_F64_FIELD: &str = "zero_count_f64";
pub const POSITIVE_BUCKETS_F64_FIELD: &str = "positive_buckets_f64";
pub const NEGATIVE_BUCKETS_F64_FIELD: &str = "negative_buckets_f64";

// Keep int and float payloads in separate columns. The populated family is the
// type signal, so we don't need to persist an extra histogram-type tag.
pub const NATIVE_HISTOGRAM_FIELD_NAMES: &[&str] = &[
    SCHEMA_FIELD,
    ZERO_THRESHOLD_FIELD,
    SUM_FIELD,
    RESET_HINT_FIELD,
    START_TIMESTAMP_FIELD,
    CUSTOM_VALUES_FIELD,
    POSITIVE_SPAN_OFFSETS_FIELD,
    POSITIVE_SPAN_LENGTHS_FIELD,
    NEGATIVE_SPAN_OFFSETS_FIELD,
    NEGATIVE_SPAN_LENGTHS_FIELD,
    COUNT_U64_FIELD,
    ZERO_COUNT_U64_FIELD,
    POSITIVE_BUCKETS_I64_FIELD,
    NEGATIVE_BUCKETS_I64_FIELD,
    COUNT_F64_FIELD,
    ZERO_COUNT_F64_FIELD,
    POSITIVE_BUCKETS_F64_FIELD,
    NEGATIVE_BUCKETS_F64_FIELD,
];

static NATIVE_HISTOGRAM_VALUE_TYPE: Lazy<ConcreteDataType> = Lazy::new(|| {
    let fields = NATIVE_HISTOGRAM_FIELD_NAMES
        .iter()
        .filter_map(|name| {
            let data_type = native_histogram_field_type(name)?;
            Some(StructField::new((*name).to_string(), data_type, true))
        })
        .collect();
    ConcreteDataType::struct_datatype(StructType::new(Arc::new(fields)))
});

/// Returns the exact Greptime type for a persisted native histogram field.
pub fn native_histogram_field_type(name: &str) -> Option<ConcreteDataType> {
    match name {
        SCHEMA_FIELD | RESET_HINT_FIELD => Some(ConcreteDataType::int32_datatype()),
        ZERO_THRESHOLD_FIELD | SUM_FIELD | COUNT_F64_FIELD | ZERO_COUNT_F64_FIELD => {
            Some(ConcreteDataType::float64_datatype())
        }
        START_TIMESTAMP_FIELD => Some(ConcreteDataType::timestamp_millisecond_datatype()),
        CUSTOM_VALUES_FIELD | POSITIVE_BUCKETS_F64_FIELD | NEGATIVE_BUCKETS_F64_FIELD => Some(
            ConcreteDataType::list_datatype(Arc::new(ConcreteDataType::float64_datatype())),
        ),
        POSITIVE_SPAN_OFFSETS_FIELD | NEGATIVE_SPAN_OFFSETS_FIELD => Some(
            ConcreteDataType::list_datatype(Arc::new(ConcreteDataType::int32_datatype())),
        ),
        POSITIVE_SPAN_LENGTHS_FIELD | NEGATIVE_SPAN_LENGTHS_FIELD => Some(
            ConcreteDataType::list_datatype(Arc::new(ConcreteDataType::uint32_datatype())),
        ),
        COUNT_U64_FIELD | ZERO_COUNT_U64_FIELD => Some(ConcreteDataType::uint64_datatype()),
        POSITIVE_BUCKETS_I64_FIELD | NEGATIVE_BUCKETS_I64_FIELD => Some(
            ConcreteDataType::list_datatype(Arc::new(ConcreteDataType::int64_datatype())),
        ),
        _ => None,
    }
}

/// Returns the exact Greptime type for a complete native histogram value.
pub fn native_histogram_value_type() -> &'static ConcreteDataType {
    &NATIVE_HISTOGRAM_VALUE_TYPE
}

/// Returns whether `data_type` matches the native histogram value contract.
pub fn is_native_histogram_value_type(data_type: &ConcreteDataType) -> bool {
    data_type == native_histogram_value_type()
}

/// Returns whether a named column matches the configured native histogram contract.
pub fn is_native_histogram_value_schema(name: &str, data_type: &ConcreteDataType) -> bool {
    name == greptime_native_histogram() && is_native_histogram_value_type(data_type)
}

/// Schema identifier for native histograms with explicit custom bucket bounds.
pub const CUSTOM_BUCKETS_SCHEMA: i32 = -53;
const MIN_EXPONENTIAL_SCHEMA: i32 = -4;
const MAX_EXPONENTIAL_SCHEMA: i32 = 8;

/// Information carried by a native histogram about a possible counter reset.
///
/// The persisted and protobuf representation remains an `i32`. Unrecognized
/// values are retained so data written by a newer producer can still round-trip.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum CounterResetHint {
    /// The payload must be inspected to determine whether a reset occurred.
    Unknown,
    /// This is the first histogram after a counter reset.
    CounterReset,
    /// No counter reset occurred since the previous histogram.
    NotCounterReset,
    /// This is a gauge histogram, so counter resets do not apply.
    Gauge,
    /// A value not understood by this version.
    Unrecognized(i32),
}

impl From<i32> for CounterResetHint {
    fn from(value: i32) -> Self {
        match value {
            0 => Self::Unknown,
            1 => Self::CounterReset,
            2 => Self::NotCounterReset,
            3 => Self::Gauge,
            value => Self::Unrecognized(value),
        }
    }
}

impl From<CounterResetHint> for i32 {
    fn from(value: CounterResetHint) -> Self {
        match value {
            CounterResetHint::Unknown => 0,
            CounterResetHint::CounterReset => 1,
            CounterResetHint::NotCounterReset => 2,
            CounterResetHint::Gauge => 3,
            CounterResetHint::Unrecognized(value) => value,
        }
    }
}

/// Reset hint indicating that the payload must be inspected.
pub const UNKNOWN_COUNTER_RESET_HINT: CounterResetHint = CounterResetHint::Unknown;
/// Reset hint indicating the first histogram after a counter reset.
pub const COUNTER_RESET_HINT: CounterResetHint = CounterResetHint::CounterReset;
/// Reset hint indicating that no counter reset occurred.
pub const NOT_COUNTER_RESET_HINT: CounterResetHint = CounterResetHint::NotCounterReset;
/// Reset hint identifying a gauge histogram.
pub const GAUGE_RESET_HINT: CounterResetHint = CounterResetHint::Gauge;

/// A contiguous run of populated sparse buckets.
#[derive(Clone, Debug, PartialEq)]
pub struct Span {
    /// The first bucket index, or the gap after the preceding span.
    pub offset: i32,
    /// Number of consecutive buckets in the span.
    pub length: u32,
}

/// Inclusion rules for a materialized bucket's lower and upper bounds.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum BoundaryRule {
    OpenLeft = 0,
    OpenRight = 1,
    ClosedBoth = 3,
}

/// A materialized bucket used by query-time estimators and renderers.
#[derive(Clone, Debug, PartialEq)]
struct Bucket {
    lower: f64,
    upper: f64,
    count: f64,
    boundary_rule: BoundaryRule,
}

/// Query-time representation of a Prometheus native histogram.
///
/// Bucket counts are absolute `f64` values even when the persisted payload used
/// integer counts or deltas.
#[derive(Clone, Debug, PartialEq)]
pub struct NativeHistogram {
    /// Exponential bucket schema, or [`CUSTOM_BUCKETS_SCHEMA`].
    pub schema: i32,
    /// Absolute width of the zero bucket.
    pub zero_threshold: f64,
    /// Sum of all observations.
    pub sum: f64,
    /// Counter reset information associated with this sample.
    pub reset_hint: CounterResetHint,
    /// Optional time in milliseconds when this histogram started counting.
    pub start_timestamp: Option<i64>,
    /// Inclusive upper bounds used when `schema` is [`CUSTOM_BUCKETS_SCHEMA`].
    pub custom_values: Vec<f64>,
    /// Sparse spans describing positive or custom buckets.
    pub positive_spans: Vec<Span>,
    /// Sparse spans describing negative buckets.
    pub negative_spans: Vec<Span>,
    /// Total number of observations.
    pub count: f64,
    /// Number of observations in the zero bucket.
    pub zero_count: f64,
    /// Absolute counts for positive or custom buckets.
    pub positive_buckets: Vec<f64>,
    /// Absolute counts for negative buckets.
    pub negative_buckets: Vec<f64>,
}

impl NativeHistogram {
    fn uses_custom_buckets(&self) -> bool {
        self.schema == CUSTOM_BUCKETS_SCHEMA
    }

    fn compatible_with(&self, other: &Self) -> bool {
        self.schema == other.schema
            && self.zero_threshold == other.zero_threshold
            && self.custom_values == other.custom_values
    }

    /// Returns a histogram with the same layout and metadata but zeroed payload values.
    pub fn zero_like(&self) -> Self {
        let mut result = self.clone();
        result.count = 0.0;
        result.zero_count = 0.0;
        result.sum = 0.0;
        result.positive_buckets.fill(0.0);
        result.negative_buckets.fill(0.0);
        result
    }

    fn combine_exact(
        &self,
        other: &Self,
        reset_hint: CounterResetHint,
        op: impl Fn(f64, f64) -> f64 + Copy,
    ) -> Option<Self> {
        if !self.compatible_with(other) {
            return None;
        }

        let mut result = self.clone();
        result.count = op(result.count, other.count);
        result.zero_count = op(result.zero_count, other.zero_count);
        result.sum = op(result.sum, other.sum);
        result.reset_hint = reset_hint;
        (result.positive_spans, result.positive_buckets) = merge_side(
            &self.positive_spans,
            &self.positive_buckets,
            &other.positive_spans,
            &other.positive_buckets,
            op,
        )?;
        (result.negative_spans, result.negative_buckets) = merge_side(
            &self.negative_spans,
            &self.negative_buckets,
            &other.negative_spans,
            &other.negative_buckets,
            op,
        )?;
        Some(result)
    }

    /// Adds two histograms after reconciling compatible bucket layouts.
    ///
    /// Returns `None` when the layouts are invalid or cannot be reconciled.
    pub fn add(&self, other: &Self) -> Option<Self> {
        let reset_hint = add_reset_hint(self.reset_hint, other.reset_hint);
        if self.is_empty_payload() {
            let mut result = other.clone();
            result.reset_hint = reset_hint;
            return result.compact();
        }
        if other.is_empty_payload() {
            let mut result = self.clone();
            result.reset_hint = reset_hint;
            return result.compact();
        }

        let (left, right) = self.reconcile(other)?;
        left.combine_exact(&right, reset_hint, |left, right| left + right)?
            .compact()
    }

    /// Subtracts `other` after reconciling compatible bucket layouts.
    ///
    /// Returns `None` when the layouts are invalid or cannot be reconciled.
    pub fn sub(&self, other: &Self) -> Option<Self> {
        if self.is_empty_payload() {
            return other.clone().negated().compact();
        }
        if other.is_empty_payload() {
            return self.clone().into_gauge().compact();
        }

        let (left, right) = self.reconcile(other)?;
        left.combine_exact(&right, CounterResetHint::Gauge, |left, right| left - right)?
            .compact()
    }

    /// Negates every payload value and marks the result as a gauge histogram.
    pub fn negated(self) -> Self {
        self.scale(-1.0)
    }

    /// Marks this histogram as a gauge without changing its payload.
    pub fn into_gauge(mut self) -> Self {
        self.reset_hint = CounterResetHint::Gauge;
        self
    }

    /// Returns whether the histograms carry explicit, contradictory reset hints.
    pub fn counter_reset_hints_contradict(&self, other: &Self) -> bool {
        matches!(
            (self.reset_hint, other.reset_hint),
            (
                CounterResetHint::CounterReset,
                CounterResetHint::NotCounterReset
            ) | (
                CounterResetHint::NotCounterReset,
                CounterResetHint::CounterReset
            )
        )
    }

    /// Returns whether two custom histograms use different bucket bounds.
    pub fn needs_custom_reconciliation(&self, other: &Self) -> bool {
        self.uses_custom_buckets()
            && other.uses_custom_buckets()
            && self.custom_values != other.custom_values
    }

    /// Compares PromQL-visible payload values by their exact bit patterns.
    ///
    /// Reset hints, start timestamps, and sparse zero placement are ignored.
    pub fn promql_eq(&self, other: &Self) -> bool {
        self.schema == other.schema
            && self.zero_threshold == other.zero_threshold
            && self.custom_values == other.custom_values
            && self.count.to_bits() == other.count.to_bits()
            && self.zero_count.to_bits() == other.zero_count.to_bits()
            && self.sum.to_bits() == other.sum.to_bits()
            && side_counts_equal(
                &self.positive_spans,
                &self.positive_buckets,
                &other.positive_spans,
                &other.positive_buckets,
            )
            && side_counts_equal(
                &self.negative_spans,
                &self.negative_buckets,
                &other.negative_spans,
                &other.negative_buckets,
            )
    }

    /// Formats this histogram using PromQL native histogram sample notation.
    pub fn promql_string(&self) -> String {
        let mut parts = vec![format!("count:{}", self.count), format!("sum:{}", self.sum)];
        if let Some(buckets) = self.all_buckets() {
            parts.extend(
                buckets
                    .into_iter()
                    .filter(|bucket| bucket.count != 0.0)
                    .map(|bucket| {
                        let (left, right) = match bucket.boundary_rule {
                            BoundaryRule::OpenLeft => ("(", "]"),
                            BoundaryRule::OpenRight => ("[", ")"),
                            BoundaryRule::ClosedBoth => ("[", "]"),
                        };
                        format!(
                            "{}{},{}{}:{}",
                            left, bucket.lower, bucket.upper, right, bucket.count
                        )
                    }),
            );
        }
        format!("{{{}}}", parts.join(", "))
    }

    /// Estimates the population variance using each populated bucket's midpoint.
    pub fn estimated_stdvar(&self) -> f64 {
        if self.count == 0.0 {
            return f64::NAN;
        }
        let mean = self.sum / self.count;
        let Some(buckets) = self.all_buckets() else {
            return f64::NAN;
        };
        buckets
            .into_iter()
            .map(|bucket| {
                let midpoint = self.bucket_midpoint(&bucket);
                bucket.count * (midpoint - mean).powi(2)
            })
            .sum::<f64>()
            / self.count
    }

    /// Estimates the population standard deviation from [`Self::estimated_stdvar`].
    pub fn estimated_stddev(&self) -> f64 {
        self.estimated_stdvar().sqrt()
    }

    /// Multiplies every payload value by `factor`.
    ///
    /// A negative factor marks the result as a gauge histogram.
    pub fn scale(mut self, factor: f64) -> Self {
        self.count *= factor;
        self.zero_count *= factor;
        self.sum *= factor;
        for count in &mut self.positive_buckets {
            *count *= factor;
        }
        for count in &mut self.negative_buckets {
            *count *= factor;
        }
        if factor < 0.0 {
            self.reset_hint = CounterResetHint::Gauge;
        }
        self
    }

    /// Divides every payload value by `divisor`.
    ///
    /// A negative divisor marks the result as a gauge histogram.
    pub fn divide_by(mut self, divisor: f64) -> Self {
        self.count /= divisor;
        self.zero_count /= divisor;
        self.sum /= divisor;
        if divisor == 0.0 {
            self.positive_spans.clear();
            self.positive_buckets.clear();
            self.negative_spans.clear();
            self.negative_buckets.clear();
        } else {
            for count in &mut self.positive_buckets {
                *count /= divisor;
            }
            for count in &mut self.negative_buckets {
                *count /= divisor;
            }
        }
        if divisor < 0.0 {
            self.reset_hint = CounterResetHint::Gauge;
        }
        self
    }

    fn compact(mut self) -> Option<Self> {
        let (spans, buckets) = compact_side(&self.positive_spans, &self.positive_buckets)?;
        self.positive_spans = spans;
        self.positive_buckets = buckets;
        let (spans, buckets) = compact_side(&self.negative_spans, &self.negative_buckets)?;
        self.negative_spans = spans;
        self.negative_buckets = buckets;
        Some(self)
    }

    /// Detects a reset from the explicit hint and histogram payload.
    pub fn detect_reset(&self, previous: &Self) -> bool {
        match self.reset_hint {
            CounterResetHint::CounterReset => return true,
            CounterResetHint::NotCounterReset => return false,
            CounterResetHint::Unknown
            | CounterResetHint::Gauge
            | CounterResetHint::Unrecognized(_) => {}
        }

        if self.count < previous.count {
            return true;
        }

        match (self.uses_custom_buckets(), previous.uses_custom_buckets()) {
            (true, true) => {
                let Some((current, previous)) = reconcile_custom(self, previous) else {
                    return true;
                };
                current.zero_count < previous.zero_count
                    || current.side_has_reset(true, &previous)
                    || current.side_has_reset(false, &previous)
            }
            (true, false) | (false, true) => true,
            (false, false) => {
                if self.schema > previous.schema || self.zero_threshold < previous.zero_threshold {
                    return true;
                }

                let mut previous = previous.clone();
                if self.zero_threshold > previous.zero_threshold {
                    let Some(expanded) = previous.expanded_zero_threshold(self.zero_threshold)
                    else {
                        return true;
                    };
                    if expanded != self.zero_threshold
                        || previous.grow_zero_threshold(self.zero_threshold).is_none()
                    {
                        return true;
                    }
                }
                let Some(previous) = previous.copy_to_schema(self.schema) else {
                    return true;
                };

                self.zero_count < previous.zero_count
                    || self.side_has_reset(true, &previous)
                    || self.side_has_reset(false, &previous)
            }
        }
    }

    fn detect_start_timestamp_reset(
        &self,
        previous: &Self,
        previous_ts: i64,
        current_ts: i64,
    ) -> bool {
        let current_start = self.start_timestamp.unwrap_or_default();
        if current_start == 0 || current_start >= current_ts || current_start < previous_ts {
            return false;
        }
        if current_start > previous_ts {
            return true;
        }

        let previous_start = previous.start_timestamp.unwrap_or_default();
        previous_start <= previous_ts && previous_start != 0 && previous_start != previous_ts
    }

    /// Detects a counter reset using start timestamps, hints, and payload values.
    pub fn detect_counter_reset(&self, previous: &Self, previous_ts: i64, current_ts: i64) -> bool {
        self.detect_start_timestamp_reset(previous, previous_ts, current_ts)
            || self.detect_reset(previous)
    }

    fn all_buckets(&self) -> Option<Vec<Bucket>> {
        let mut buckets = self.side_buckets(false)?;
        buckets.reverse();
        if self.zero_count != 0.0 {
            buckets.push(Bucket {
                lower: -self.zero_threshold,
                upper: self.zero_threshold,
                count: self.zero_count,
                boundary_rule: BoundaryRule::ClosedBoth,
            });
        }
        buckets.extend(self.side_buckets(true)?);
        Some(buckets)
    }

    fn side_buckets(&self, positive: bool) -> Option<Vec<Bucket>> {
        let (spans, counts) = if positive {
            (&self.positive_spans, &self.positive_buckets)
        } else {
            (&self.negative_spans, &self.negative_buckets)
        };

        let mut result = Vec::with_capacity(counts.len());
        for (idx, count) in side_bucket_indices(spans)?.into_iter().zip(counts) {
            let upper = get_bound(idx, self.schema, &self.custom_values)?;
            let lower = get_bound(idx.checked_sub(1)?, self.schema, &self.custom_values)?;
            if positive {
                result.push(Bucket {
                    lower,
                    upper,
                    count: *count,
                    boundary_rule: if self.uses_custom_buckets() && idx == 0 {
                        BoundaryRule::ClosedBoth
                    } else {
                        BoundaryRule::OpenLeft
                    },
                });
            } else {
                result.push(Bucket {
                    lower: -upper,
                    upper: -lower,
                    count: *count,
                    boundary_rule: BoundaryRule::OpenRight,
                });
            }
        }
        Some(result)
    }

    /// Estimates the value at quantile `q`.
    ///
    /// Returns negative or positive infinity outside `[0, 1]`, and `NaN` when
    /// the quantile cannot be estimated.
    pub fn quantile(&self, q: f64) -> f64 {
        if q < 0.0 {
            return f64::NEG_INFINITY;
        }
        if q > 1.0 {
            return f64::INFINITY;
        }
        if self.count == 0.0 || q.is_nan() {
            return f64::NAN;
        }

        let Some(mut buckets) = self.all_buckets() else {
            return f64::NAN;
        };
        let rank = q * self.count;
        let mut count = 0.0;
        for bucket in &mut buckets {
            if bucket.count == 0.0 {
                continue;
            }
            count += bucket.count;
            if count < rank {
                continue;
            }

            if !self.uses_custom_buckets() && bucket.lower < 0.0 && bucket.upper > 0.0 {
                if self.negative_buckets.is_empty() && !self.positive_buckets.is_empty() {
                    bucket.lower = 0.0;
                } else if self.positive_buckets.is_empty() && !self.negative_buckets.is_empty() {
                    bucket.upper = 0.0;
                }
            } else if self.uses_custom_buckets() {
                if bucket.lower == f64::NEG_INFINITY {
                    if bucket.upper <= 0.0 {
                        return bucket.upper;
                    }
                    bucket.lower = 0.0;
                } else if bucket.upper == f64::INFINITY {
                    return bucket.lower;
                }
            }

            let rank_in_bucket = rank - (count - bucket.count);
            let fraction = rank_in_bucket / bucket.count;
            if self.uses_custom_buckets() || (bucket.lower <= 0.0 && bucket.upper >= 0.0) {
                return bucket.lower + (bucket.upper - bucket.lower) * fraction;
            }

            let log_lower = bucket.lower.abs().log2();
            let log_upper = bucket.upper.abs().log2();
            if bucket.lower > 0.0 {
                return 2.0_f64.powf(log_lower + (log_upper - log_lower) * fraction);
            }
            return -2.0_f64.powf(log_upper + (log_lower - log_upper) * (1.0 - fraction));
        }

        f64::NAN
    }

    /// Estimates the fraction of observations between `lower` and `upper`.
    pub fn fraction(&self, lower: f64, upper: f64) -> f64 {
        if self.count == 0.0 || lower.is_nan() || upper.is_nan() {
            return f64::NAN;
        }
        if lower >= upper {
            return 0.0;
        }

        let Some(mut buckets) = self.all_buckets() else {
            return f64::NAN;
        };
        let count = if self.sum.is_nan() {
            buckets.iter().map(|bucket| bucket.count).sum()
        } else {
            self.count
        };

        let mut rank = 0.0;
        let mut lower_rank = 0.0;
        let mut upper_rank = 0.0;
        let mut lower_set = false;
        let mut upper_set = false;

        for bucket in &mut buckets {
            let zero_bucket = bucket.lower <= 0.0 && bucket.upper >= 0.0;
            if zero_bucket {
                if self.negative_buckets.is_empty() && !self.positive_buckets.is_empty() {
                    bucket.lower = 0.0;
                } else if self.positive_buckets.is_empty() && !self.negative_buckets.is_empty() {
                    bucket.upper = 0.0;
                }
            }

            if !lower_set && bucket.lower >= lower {
                lower_rank = rank;
                lower_set = true;
            }
            if !upper_set && bucket.lower >= upper {
                upper_rank = rank;
                upper_set = true;
            }
            if lower_set && upper_set {
                break;
            }
            if !lower_set && bucket.lower < lower && bucket.upper > lower {
                lower_rank = self.interpolate_rank(bucket, rank, lower, zero_bucket);
                lower_set = true;
            }
            if !upper_set && bucket.lower < upper && bucket.upper > upper {
                upper_rank = self.interpolate_rank(bucket, rank, upper, zero_bucket);
                upper_set = true;
            }
            if lower_set && upper_set {
                break;
            }
            rank += bucket.count;
        }

        if !lower_set || lower_rank > count {
            lower_rank = count;
        }
        if !upper_set || upper_rank > count {
            upper_rank = count;
        }

        (upper_rank - lower_rank) / self.count
    }

    /// Converts populated buckets to `(boundary rule, lower, upper, count)` strings.
    pub fn to_prometheus_buckets(&self) -> Option<Vec<(u8, String, String, String)>> {
        Some(
            self.all_buckets()?
                .into_iter()
                .filter(|bucket| bucket.count != 0.0)
                .map(|bucket| {
                    (
                        bucket.boundary_rule as u8,
                        format_prometheus_float(bucket.lower),
                        format_prometheus_float(bucket.upper),
                        format_prometheus_float(bucket.count),
                    )
                })
                .collect(),
        )
    }

    fn interpolate_rank(&self, bucket: &Bucket, rank: f64, value: f64, zero_bucket: bool) -> f64 {
        if self.uses_custom_buckets() || zero_bucket {
            if bucket.lower == f64::NEG_INFINITY {
                return bucket.count;
            }
            return rank + bucket.count * (value - bucket.lower) / (bucket.upper - bucket.lower);
        }

        let log_lower = bucket.lower.abs().log2();
        let log_upper = bucket.upper.abs().log2();
        let log_value = value.abs().log2();
        let fraction = if value > 0.0 {
            (log_value - log_lower) / (log_upper - log_lower)
        } else {
            1.0 - ((log_value - log_upper) / (log_lower - log_upper))
        };
        rank + bucket.count * fraction
    }

    fn bucket_midpoint(&self, bucket: &Bucket) -> f64 {
        if bucket.lower == f64::NEG_INFINITY && bucket.upper == f64::INFINITY {
            return 0.0;
        }
        if bucket.lower == f64::NEG_INFINITY {
            return bucket.upper;
        }
        if bucket.upper == f64::INFINITY {
            return bucket.lower;
        }
        if self.uses_custom_buckets() || (bucket.lower <= 0.0 && bucket.upper >= 0.0) {
            return (bucket.lower + bucket.upper) / 2.0;
        }
        if bucket.upper < 0.0 {
            -((bucket.lower.abs() * bucket.upper.abs()).sqrt())
        } else {
            (bucket.lower * bucket.upper).sqrt()
        }
    }

    fn side_has_reset(&self, positive: bool, previous: &Self) -> bool {
        let (current_spans, current_buckets, previous_spans, previous_buckets) = if positive {
            (
                &self.positive_spans,
                &self.positive_buckets,
                &previous.positive_spans,
                &previous.positive_buckets,
            )
        } else {
            (
                &self.negative_spans,
                &self.negative_buckets,
                &previous.negative_spans,
                &previous.negative_buckets,
            )
        };
        let Some(current) = side_counts(current_spans, current_buckets) else {
            return true;
        };
        let Some(previous) = side_counts(previous_spans, previous_buckets) else {
            return true;
        };
        previous.keys().chain(current.keys()).any(|idx| {
            current.get(idx).copied().unwrap_or_default()
                < previous.get(idx).copied().unwrap_or_default()
        })
    }

    fn reconcile(&self, other: &Self) -> Option<(Self, Self)> {
        match (self.uses_custom_buckets(), other.uses_custom_buckets()) {
            (true, true) => reconcile_custom(self, other),
            (false, false) => reconcile_exponential(self, other),
            _ => None,
        }
    }

    fn is_empty_payload(&self) -> bool {
        self.count == 0.0
            && self.zero_count == 0.0
            && self.sum == 0.0
            && self.positive_buckets.iter().all(|count| *count == 0.0)
            && self.negative_buckets.iter().all(|count| *count == 0.0)
    }
}

fn reconcile_exponential(
    left: &NativeHistogram,
    right: &NativeHistogram,
) -> Option<(NativeHistogram, NativeHistogram)> {
    let schema = left.schema.min(right.schema);
    let mut left = left.copy_to_schema(schema)?;
    let mut right = right.copy_to_schema(schema)?;
    let mut zero_threshold = left.zero_threshold.max(right.zero_threshold);
    loop {
        let expanded = left
            .expanded_zero_threshold(zero_threshold)?
            .max(right.expanded_zero_threshold(zero_threshold)?);
        if expanded == zero_threshold {
            break;
        }
        zero_threshold = expanded;
    }
    left.grow_zero_threshold(zero_threshold)?;
    right.grow_zero_threshold(zero_threshold)?;
    Some((left.compact()?, right.compact()?))
}

fn reconcile_custom(
    left: &NativeHistogram,
    right: &NativeHistogram,
) -> Option<(NativeHistogram, NativeHistogram)> {
    let custom_values = if left.custom_values == right.custom_values {
        left.custom_values.clone()
    } else {
        left.custom_values
            .iter()
            .copied()
            .filter(|value| right.custom_values.contains(value))
            .collect()
    };

    Some((
        left.copy_to_custom_values(custom_values.clone())?
            .compact()?,
        right.copy_to_custom_values(custom_values)?.compact()?,
    ))
}

impl NativeHistogram {
    fn copy_to_schema(&self, target_schema: i32) -> Option<Self> {
        if self.uses_custom_buckets()
            || !(MIN_EXPONENTIAL_SCHEMA..=MAX_EXPONENTIAL_SCHEMA).contains(&target_schema)
            || target_schema > self.schema
        {
            return None;
        }
        if target_schema == self.schema {
            return Some(self.clone());
        }

        let mut result = self.clone();
        result.schema = target_schema;
        (result.positive_spans, result.positive_buckets) = reduce_side(
            &self.positive_spans,
            &self.positive_buckets,
            self.schema,
            target_schema,
        )?;
        (result.negative_spans, result.negative_buckets) = reduce_side(
            &self.negative_spans,
            &self.negative_buckets,
            self.schema,
            target_schema,
        )?;
        Some(result)
    }

    fn copy_to_custom_values(&self, custom_values: Vec<f64>) -> Option<Self> {
        if !self.uses_custom_buckets() {
            return None;
        }
        if self.custom_values == custom_values {
            return Some(self.clone());
        }

        let mut result = self.clone();
        result.custom_values = custom_values.clone();
        result.negative_spans.clear();
        result.negative_buckets.clear();
        (result.positive_spans, result.positive_buckets) = map_custom_side(
            &self.positive_spans,
            &self.positive_buckets,
            &self.custom_values,
            &custom_values,
        )?;
        Some(result)
    }

    fn grow_zero_threshold(&mut self, zero_threshold: f64) -> Option<()> {
        if self.uses_custom_buckets() || zero_threshold == self.zero_threshold {
            self.zero_threshold = zero_threshold;
            return Some(());
        }

        let (spans, buckets, zero_count) = fold_zero_side(
            &self.positive_spans,
            &self.positive_buckets,
            self.schema,
            zero_threshold,
        )?;
        self.positive_spans = spans;
        self.positive_buckets = buckets;
        self.zero_count += zero_count;

        let (spans, buckets, zero_count) = fold_zero_side(
            &self.negative_spans,
            &self.negative_buckets,
            self.schema,
            zero_threshold,
        )?;
        self.negative_spans = spans;
        self.negative_buckets = buckets;
        self.zero_count += zero_count;
        self.zero_threshold = zero_threshold;
        Some(())
    }

    fn expanded_zero_threshold(&self, mut zero_threshold: f64) -> Option<f64> {
        if self.uses_custom_buckets() {
            return Some(zero_threshold);
        }
        zero_threshold = expand_zero_threshold_side(
            &self.positive_spans,
            &self.positive_buckets,
            self.schema,
            zero_threshold,
        )?;
        expand_zero_threshold_side(
            &self.negative_spans,
            &self.negative_buckets,
            self.schema,
            zero_threshold,
        )
    }
}

fn add_reset_hint(left: CounterResetHint, right: CounterResetHint) -> CounterResetHint {
    if left == CounterResetHint::Gauge || right == CounterResetHint::Gauge {
        CounterResetHint::Gauge
    } else if left == right {
        left
    } else {
        CounterResetHint::Unknown
    }
}

fn side_bucket_indices(spans: &[Span]) -> Option<Vec<i32>> {
    let mut indices = Vec::new();
    let mut current_index = 0i32;
    let mut first = true;
    for span in spans {
        if first {
            current_index = span.offset;
            first = false;
        } else {
            current_index = current_index.checked_add(span.offset)?;
        }
        for _ in 0..span.length {
            indices.push(current_index);
            current_index = current_index.checked_add(1)?;
        }
    }
    Some(indices)
}

fn spans_from_indices_counts(values: Vec<(i32, f64)>) -> Option<(Vec<Span>, Vec<f64>)> {
    let mut spans = Vec::<Span>::new();
    let mut buckets = Vec::new();
    let mut previous_index = None::<i32>;

    for (idx, count) in values {
        if count == 0.0 {
            continue;
        }
        match (spans.last_mut(), previous_index) {
            (Some(span), Some(previous)) if previous.checked_add(1) == Some(idx) => {
                span.length = span.length.checked_add(1)?;
            }
            (_, Some(previous)) => {
                spans.push(Span {
                    offset: idx.checked_sub(previous)?.checked_sub(1)?,
                    length: 1,
                });
            }
            (_, None) => {
                spans.push(Span {
                    offset: idx,
                    length: 1,
                });
            }
        }
        buckets.push(count);
        previous_index = Some(idx);
    }

    Some((spans, buckets))
}

fn side_counts(spans: &[Span], buckets: &[f64]) -> Option<BTreeMap<i32, f64>> {
    let mut values = BTreeMap::new();
    for (idx, count) in side_bucket_indices(spans)?.into_iter().zip(buckets) {
        if *count != 0.0 {
            values.insert(idx, *count);
        }
    }
    Some(values)
}

fn side_counts_equal(
    left_spans: &[Span],
    left_buckets: &[f64],
    right_spans: &[Span],
    right_buckets: &[f64],
) -> bool {
    match (
        side_counts(left_spans, left_buckets),
        side_counts(right_spans, right_buckets),
    ) {
        (Some(left), Some(right)) => {
            left.len() == right.len()
                && left.iter().all(|(idx, count)| {
                    right
                        .get(idx)
                        .is_some_and(|other| count.to_bits() == other.to_bits())
                })
        }
        (None, None) => true,
        _ => false,
    }
}

fn merge_side(
    left_spans: &[Span],
    left_buckets: &[f64],
    right_spans: &[Span],
    right_buckets: &[f64],
    op: impl Fn(f64, f64) -> f64,
) -> Option<(Vec<Span>, Vec<f64>)> {
    let left = side_counts(left_spans, left_buckets)?;
    let right = side_counts(right_spans, right_buckets)?;
    let mut values = BTreeMap::new();
    for idx in left.keys().chain(right.keys()) {
        values.insert(
            *idx,
            op(
                left.get(idx).copied().unwrap_or_default(),
                right.get(idx).copied().unwrap_or_default(),
            ),
        );
    }
    spans_from_indices_counts(values.into_iter().collect())
}

fn reduce_side(
    spans: &[Span],
    buckets: &[f64],
    schema: i32,
    target_schema: i32,
) -> Option<(Vec<Span>, Vec<f64>)> {
    let factor = 1_i32.checked_shl((schema - target_schema) as u32)?;
    let mut values = std::collections::BTreeMap::<i32, f64>::new();
    for (idx, count) in side_bucket_indices(spans)?.into_iter().zip(buckets) {
        let target_idx = ceil_div(idx, factor);
        *values.entry(target_idx).or_default() += *count;
    }
    spans_from_indices_counts(values.into_iter().collect())
}

fn compact_side(spans: &[Span], buckets: &[f64]) -> Option<(Vec<Span>, Vec<f64>)> {
    let indices = side_bucket_indices(spans)?;
    spans_from_indices_counts(indices.into_iter().zip(buckets.iter().copied()).collect())
}

fn map_custom_side(
    spans: &[Span],
    buckets: &[f64],
    old_values: &[f64],
    new_values: &[f64],
) -> Option<(Vec<Span>, Vec<f64>)> {
    let mut values = std::collections::BTreeMap::<i32, f64>::new();
    for (idx, count) in side_bucket_indices(spans)?.into_iter().zip(buckets) {
        let upper = get_bound(idx, CUSTOM_BUCKETS_SCHEMA, old_values)?;
        let target_idx = new_values
            .iter()
            .position(|value| *value >= upper)
            .unwrap_or(new_values.len()) as i32;
        *values.entry(target_idx).or_default() += *count;
    }
    spans_from_indices_counts(values.into_iter().collect())
}

fn fold_zero_side(
    spans: &[Span],
    buckets: &[f64],
    schema: i32,
    zero_threshold: f64,
) -> Option<(Vec<Span>, Vec<f64>, f64)> {
    let mut kept = Vec::new();
    let mut zero_count = 0.0;
    for (idx, count) in side_bucket_indices(spans)?.into_iter().zip(buckets) {
        if get_bound(idx, schema, &[])? <= zero_threshold {
            zero_count += *count;
        } else {
            kept.push((idx, *count));
        }
    }
    let (spans, buckets) = spans_from_indices_counts(kept)?;
    Some((spans, buckets, zero_count))
}

fn expand_zero_threshold_side(
    spans: &[Span],
    buckets: &[f64],
    schema: i32,
    mut zero_threshold: f64,
) -> Option<f64> {
    for (idx, count) in side_bucket_indices(spans)?.into_iter().zip(buckets) {
        if *count == 0.0 {
            continue;
        }
        let lower = get_bound(idx.checked_sub(1)?, schema, &[])?;
        let upper = get_bound(idx, schema, &[])?;
        if lower < zero_threshold && zero_threshold < upper {
            zero_threshold = upper;
        }
    }
    Some(zero_threshold)
}

fn ceil_div(value: i32, divisor: i32) -> i32 {
    value.div_euclid(divisor) + i32::from(value.rem_euclid(divisor) != 0)
}

fn get_bound(idx: i32, schema: i32, custom_values: &[f64]) -> Option<f64> {
    if schema == CUSTOM_BUCKETS_SCHEMA {
        return match idx {
            -1 => Some(f64::NEG_INFINITY),
            idx if idx == custom_values.len() as i32 => Some(f64::INFINITY),
            idx if idx >= 0 && (idx as usize) < custom_values.len() => {
                Some(custom_values[idx as usize])
            }
            _ => None,
        };
    }

    if !(MIN_EXPONENTIAL_SCHEMA..=MAX_EXPONENTIAL_SCHEMA).contains(&schema) {
        return None;
    }
    if schema < 0 {
        let exponent = idx.checked_shl((-schema) as u32)?;
        return Some(if exponent == f64::MAX_EXP {
            f64::MAX
        } else {
            2.0_f64.powi(exponent)
        });
    }
    let exponent = idx as f64 / (1u32 << schema) as f64;
    Some(if exponent == f64::MAX_EXP as f64 {
        f64::MAX
    } else {
        2.0_f64.powf(exponent)
    })
}

/// Returns the Arrow type for a complete native histogram value.
pub fn native_histogram_arrow_type() -> ArrowDataType {
    native_histogram_value_type().as_arrow_type()
}

fn struct_child<'a>(array: &'a StructArray, name: &str) -> DfResult<&'a ArrayRef> {
    let index = array
        .fields()
        .iter()
        .position(|field| field.name() == name)
        .ok_or_else(|| {
            DataFusionError::Execution(format!("native histogram missing field {name}"))
        })?;
    Ok(array.column(index))
}

fn primitive_child<'a, T>(array: &'a StructArray, name: &str) -> DfResult<&'a PrimitiveArray<T>>
where
    T: ArrowPrimitiveType,
{
    let child = struct_child(array, name)?;
    child
        .as_any()
        .downcast_ref::<PrimitiveArray<T>>()
        .ok_or_else(|| {
            DataFusionError::Execution(format!(
                "native histogram field {name} has invalid type {}",
                child.data_type()
            ))
        })
}

fn list_child<'a>(array: &'a StructArray, name: &str) -> DfResult<&'a ListArray> {
    let child = struct_child(array, name)?;
    child.as_any().downcast_ref::<ListArray>().ok_or_else(|| {
        DataFusionError::Execution(format!(
            "native histogram field {name} has invalid type {}",
            child.data_type()
        ))
    })
}

fn required_primitive<T>(array: &StructArray, name: &str, row: usize) -> DfResult<T::Native>
where
    T: ArrowPrimitiveType,
{
    let values = primitive_child::<T>(array, name)?;
    if values.is_null(row) {
        return Err(DataFusionError::Execution(format!(
            "native histogram field {name} is null"
        )));
    }
    Ok(values.value(row))
}

fn optional_primitive<T>(array: &StructArray, name: &str, row: usize) -> DfResult<Option<T::Native>>
where
    T: ArrowPrimitiveType,
{
    let values = primitive_child::<T>(array, name)?;
    Ok((!values.is_null(row)).then(|| values.value(row)))
}

fn list_values<T>(array: &StructArray, name: &str, row: usize) -> DfResult<Vec<T::Native>>
where
    T: ArrowPrimitiveType,
{
    let list = list_child(array, name)?;
    if list.is_null(row) {
        return Ok(Vec::new());
    }

    let values = list.value(row);
    let values = values
        .as_any()
        .downcast_ref::<PrimitiveArray<T>>()
        .ok_or_else(|| {
            DataFusionError::Execution(format!(
                "native histogram list field {name} has invalid value type {}",
                values.data_type()
            ))
        })?;

    values
        .iter()
        .map(|value| {
            value.ok_or_else(|| {
                DataFusionError::Execution(format!(
                    "native histogram list field {name} contains null"
                ))
            })
        })
        .collect()
}

fn read_spans(offsets: Vec<i32>, lengths: Vec<u32>, name: &str) -> DfResult<Vec<Span>> {
    if offsets.len() != lengths.len() {
        return Err(DataFusionError::Execution(format!(
            "native histogram {name} span offsets and lengths mismatch: {} vs {}",
            offsets.len(),
            lengths.len()
        )));
    }
    Ok(offsets
        .into_iter()
        .zip(lengths)
        .map(|(offset, length)| Span { offset, length })
        .collect())
}

fn check_span_bucket_count(spans: &[Span], buckets: usize, name: &str) -> DfResult<()> {
    let span_len = spans
        .iter()
        .try_fold(0usize, |sum, span| sum.checked_add(span.length as usize))
        .ok_or_else(|| {
            DataFusionError::Execution(format!("native histogram {name} spans overflow"))
        })?;
    if span_len != buckets {
        return Err(DataFusionError::Execution(format!(
            "native histogram {name} spans describe {span_len} buckets, found {buckets}"
        )));
    }
    Ok(())
}

/// Decodes one native histogram row into the query-time [`NativeHistogram`] model.
///
/// A null struct returns `Ok(None)`. Integer payloads are converted to absolute
/// `f64` counts, and malformed child fields or span layouts return an error.
pub fn read_histogram(array: &StructArray, row: usize) -> DfResult<Option<NativeHistogram>> {
    if array.is_null(row) {
        return Ok(None);
    }

    let schema = required_primitive::<Int32Type>(array, SCHEMA_FIELD, row)?;
    let positive_spans = read_spans(
        list_values::<Int32Type>(array, POSITIVE_SPAN_OFFSETS_FIELD, row)?,
        list_values::<UInt32Type>(array, POSITIVE_SPAN_LENGTHS_FIELD, row)?,
        "positive",
    )?;
    let negative_spans = read_spans(
        list_values::<Int32Type>(array, NEGATIVE_SPAN_OFFSETS_FIELD, row)?,
        list_values::<UInt32Type>(array, NEGATIVE_SPAN_LENGTHS_FIELD, row)?,
        "negative",
    )?;

    let (count, zero_count, positive_buckets, negative_buckets) =
        if let Some(count) = optional_primitive::<Float64Type>(array, COUNT_F64_FIELD, row)? {
            (
                count,
                optional_primitive::<Float64Type>(array, ZERO_COUNT_F64_FIELD, row)?
                    .unwrap_or_default(),
                list_values::<Float64Type>(array, POSITIVE_BUCKETS_F64_FIELD, row)?,
                list_values::<Float64Type>(array, NEGATIVE_BUCKETS_F64_FIELD, row)?,
            )
        } else {
            (
                required_primitive::<UInt64Type>(array, COUNT_U64_FIELD, row)? as f64,
                optional_primitive::<UInt64Type>(array, ZERO_COUNT_U64_FIELD, row)?
                    .unwrap_or_default() as f64,
                list_values::<Int64Type>(array, POSITIVE_BUCKETS_I64_FIELD, row)?
                    .into_iter()
                    .map(|value| value as f64)
                    .collect(),
                list_values::<Int64Type>(array, NEGATIVE_BUCKETS_I64_FIELD, row)?
                    .into_iter()
                    .map(|value| value as f64)
                    .collect(),
            )
        };

    check_span_bucket_count(&positive_spans, positive_buckets.len(), "positive")?;
    check_span_bucket_count(&negative_spans, negative_buckets.len(), "negative")?;

    Ok(Some(NativeHistogram {
        schema,
        zero_threshold: required_primitive::<Float64Type>(array, ZERO_THRESHOLD_FIELD, row)?,
        sum: required_primitive::<Float64Type>(array, SUM_FIELD, row)?,
        reset_hint: required_primitive::<Int32Type>(array, RESET_HINT_FIELD, row)?.into(),
        start_timestamp: optional_primitive::<TimestampMillisecondType>(
            array,
            START_TIMESTAMP_FIELD,
            row,
        )?,
        custom_values: list_values::<Float64Type>(array, CUSTOM_VALUES_FIELD, row)?,
        positive_spans,
        negative_spans,
        count,
        zero_count,
        positive_buckets,
        negative_buckets,
    }))
}

fn list_opt<T>(values: Vec<T>) -> Option<Vec<Option<T>>> {
    Some(values.into_iter().map(Some).collect())
}

/// Builds a native histogram Struct array from query-time values.
///
/// The result uses the canonical `f64` payload fields. Integer count and bucket
/// fields remain empty because [`NativeHistogram`] no longer tracks the source
/// payload family after decoding.
pub fn build_histogram_array(values: &[Option<NativeHistogram>]) -> ArrayRef {
    let mut schemas = Vec::with_capacity(values.len());
    let mut zero_thresholds = Vec::with_capacity(values.len());
    let mut sums = Vec::with_capacity(values.len());
    let mut reset_hints = Vec::with_capacity(values.len());
    let mut start_timestamps = Vec::with_capacity(values.len());
    let mut custom_values = Vec::with_capacity(values.len());
    let mut positive_span_offsets = Vec::with_capacity(values.len());
    let mut positive_span_lengths = Vec::with_capacity(values.len());
    let mut negative_span_offsets = Vec::with_capacity(values.len());
    let mut negative_span_lengths = Vec::with_capacity(values.len());
    let mut count_u64 = Vec::<Option<u64>>::with_capacity(values.len());
    let mut zero_count_u64 = Vec::<Option<u64>>::with_capacity(values.len());
    let mut positive_buckets_i64 = Vec::with_capacity(values.len());
    let mut negative_buckets_i64 = Vec::with_capacity(values.len());
    let mut count_f64 = Vec::with_capacity(values.len());
    let mut zero_count_f64 = Vec::with_capacity(values.len());
    let mut positive_buckets_f64 = Vec::with_capacity(values.len());
    let mut negative_buckets_f64 = Vec::with_capacity(values.len());
    let mut validity = Vec::with_capacity(values.len());

    for value in values {
        validity.push(value.is_some());
        if let Some(histogram) = value {
            schemas.push(Some(histogram.schema));
            zero_thresholds.push(Some(histogram.zero_threshold));
            sums.push(Some(histogram.sum));
            reset_hints.push(Some(i32::from(histogram.reset_hint)));
            start_timestamps.push(histogram.start_timestamp);
            custom_values.push(list_opt(histogram.custom_values.clone()));
            positive_span_offsets.push(list_opt(
                histogram
                    .positive_spans
                    .iter()
                    .map(|span| span.offset)
                    .collect(),
            ));
            positive_span_lengths.push(list_opt(
                histogram
                    .positive_spans
                    .iter()
                    .map(|span| span.length)
                    .collect(),
            ));
            negative_span_offsets.push(list_opt(
                histogram
                    .negative_spans
                    .iter()
                    .map(|span| span.offset)
                    .collect(),
            ));
            negative_span_lengths.push(list_opt(
                histogram
                    .negative_spans
                    .iter()
                    .map(|span| span.length)
                    .collect(),
            ));
            count_u64.push(None);
            zero_count_u64.push(None);
            positive_buckets_i64.push(list_opt(Vec::<i64>::new()));
            negative_buckets_i64.push(list_opt(Vec::<i64>::new()));
            count_f64.push(Some(histogram.count));
            zero_count_f64.push(Some(histogram.zero_count));
            positive_buckets_f64.push(list_opt(histogram.positive_buckets.clone()));
            negative_buckets_f64.push(list_opt(histogram.negative_buckets.clone()));
        } else {
            schemas.push(None);
            zero_thresholds.push(None);
            sums.push(None);
            reset_hints.push(None);
            start_timestamps.push(None);
            custom_values.push(None);
            positive_span_offsets.push(None);
            positive_span_lengths.push(None);
            negative_span_offsets.push(None);
            negative_span_lengths.push(None);
            count_u64.push(None);
            zero_count_u64.push(None);
            positive_buckets_i64.push(None);
            negative_buckets_i64.push(None);
            count_f64.push(None);
            zero_count_f64.push(None);
            positive_buckets_f64.push(None);
            negative_buckets_f64.push(None);
        }
    }

    let named_arrays: Vec<(&str, ArrayRef)> = vec![
        (SCHEMA_FIELD, Arc::new(Int32Array::from(schemas))),
        (
            ZERO_THRESHOLD_FIELD,
            Arc::new(Float64Array::from(zero_thresholds)),
        ),
        (SUM_FIELD, Arc::new(Float64Array::from(sums))),
        (RESET_HINT_FIELD, Arc::new(Int32Array::from(reset_hints))),
        (
            START_TIMESTAMP_FIELD,
            Arc::new(TimestampMillisecondArray::from_iter(start_timestamps)),
        ),
        (
            CUSTOM_VALUES_FIELD,
            Arc::new(ListArray::from_iter_primitive::<Float64Type, _, _>(
                custom_values,
            )),
        ),
        (
            POSITIVE_SPAN_OFFSETS_FIELD,
            Arc::new(ListArray::from_iter_primitive::<Int32Type, _, _>(
                positive_span_offsets,
            )),
        ),
        (
            POSITIVE_SPAN_LENGTHS_FIELD,
            Arc::new(ListArray::from_iter_primitive::<UInt32Type, _, _>(
                positive_span_lengths,
            )),
        ),
        (
            NEGATIVE_SPAN_OFFSETS_FIELD,
            Arc::new(ListArray::from_iter_primitive::<Int32Type, _, _>(
                negative_span_offsets,
            )),
        ),
        (
            NEGATIVE_SPAN_LENGTHS_FIELD,
            Arc::new(ListArray::from_iter_primitive::<UInt32Type, _, _>(
                negative_span_lengths,
            )),
        ),
        (COUNT_U64_FIELD, Arc::new(UInt64Array::from(count_u64))),
        (
            ZERO_COUNT_U64_FIELD,
            Arc::new(UInt64Array::from(zero_count_u64)),
        ),
        (
            POSITIVE_BUCKETS_I64_FIELD,
            Arc::new(ListArray::from_iter_primitive::<Int64Type, _, _>(
                positive_buckets_i64,
            )),
        ),
        (
            NEGATIVE_BUCKETS_I64_FIELD,
            Arc::new(ListArray::from_iter_primitive::<Int64Type, _, _>(
                negative_buckets_i64,
            )),
        ),
        (COUNT_F64_FIELD, Arc::new(Float64Array::from(count_f64))),
        (
            ZERO_COUNT_F64_FIELD,
            Arc::new(Float64Array::from(zero_count_f64)),
        ),
        (
            POSITIVE_BUCKETS_F64_FIELD,
            Arc::new(ListArray::from_iter_primitive::<Float64Type, _, _>(
                positive_buckets_f64,
            )),
        ),
        (
            NEGATIVE_BUCKETS_F64_FIELD,
            Arc::new(ListArray::from_iter_primitive::<Float64Type, _, _>(
                negative_buckets_f64,
            )),
        ),
    ];
    let (fields, arrays): (Vec<_>, Vec<_>) = named_arrays
        .into_iter()
        .map(|(name, array)| (Field::new(name, array.data_type().clone(), true), array))
        .unzip();

    Arc::new(StructArray::new(
        fields.into(),
        arrays,
        Some(NullBuffer::from(validity)),
    ))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn histogram(positive_spans: Vec<Span>, positive_buckets: Vec<f64>) -> NativeHistogram {
        let count = positive_buckets.iter().sum();
        NativeHistogram {
            schema: 0,
            zero_threshold: 0.0,
            sum: count,
            reset_hint: CounterResetHint::CounterReset,
            start_timestamp: None,
            custom_values: Vec::new(),
            positive_spans,
            negative_spans: Vec::new(),
            count,
            zero_count: 0.0,
            positive_buckets,
            negative_buckets: Vec::new(),
        }
    }

    #[test]
    fn span_rebuild_checks_offsets_and_preserves_empty_input() {
        assert_eq!(
            spans_from_indices_counts(Vec::new()),
            Some((Vec::new(), Vec::new()))
        );
        assert_eq!(
            spans_from_indices_counts(vec![(2, 1.0), (3, 2.0), (5, 3.0)]),
            Some((
                vec![
                    Span {
                        offset: 2,
                        length: 2,
                    },
                    Span {
                        offset: 1,
                        length: 1,
                    },
                ],
                vec![1.0, 2.0, 3.0],
            ))
        );
        assert_eq!(
            spans_from_indices_counts(vec![(i32::MIN, 1.0), (i32::MAX, 2.0)]),
            None
        );
    }

    #[test]
    fn arrow_round_trip_preserves_unrecognized_reset_hint() {
        let mut expected = histogram(
            vec![Span {
                offset: 0,
                length: 1,
            }],
            vec![1.0],
        );
        expected.reset_hint = CounterResetHint::Unrecognized(42);

        let array = build_histogram_array(&[Some(expected.clone())]);
        assert_eq!(array.data_type(), &native_histogram_arrow_type());
        let array = array.as_any().downcast_ref::<StructArray>().unwrap();

        assert_eq!(read_histogram(array, 0).unwrap(), Some(expected));
        assert!(
            primitive_child::<UInt64Type>(array, COUNT_U64_FIELD)
                .unwrap()
                .is_null(0)
        );
        assert_eq!(
            primitive_child::<Float64Type>(array, COUNT_F64_FIELD)
                .unwrap()
                .value(0),
            1.0
        );
    }

    #[test]
    fn add_uses_sparse_bucket_union() {
        let left = histogram(
            vec![Span {
                offset: 0,
                length: 1,
            }],
            vec![1.0],
        );
        let right = histogram(
            vec![Span {
                offset: 2,
                length: 1,
            }],
            vec![2.0],
        );

        let result = left.add(&right).unwrap();
        assert_eq!(
            result.positive_spans,
            vec![
                Span {
                    offset: 0,
                    length: 1,
                },
                Span {
                    offset: 1,
                    length: 1,
                },
            ]
        );
        assert_eq!(result.positive_buckets, vec![1.0, 2.0]);
    }

    #[test]
    fn add_combines_reset_hints_for_empty_payloads() {
        let mut empty = histogram(Vec::new(), Vec::new());
        empty.reset_hint = CounterResetHint::Gauge;
        let mut populated = histogram(
            vec![Span {
                offset: 0,
                length: 1,
            }],
            vec![1.0],
        );
        populated.reset_hint = CounterResetHint::NotCounterReset;

        assert_eq!(
            empty.add(&populated).unwrap().reset_hint,
            CounterResetHint::Gauge
        );
        assert_eq!(
            populated.add(&empty).unwrap().reset_hint,
            CounterResetHint::Gauge
        );
    }

    #[test]
    fn zero_threshold_expands_to_populated_bucket_boundary() {
        let left = NativeHistogram {
            zero_threshold: 0.5,
            ..histogram(
                vec![Span {
                    offset: 0,
                    length: 1,
                }],
                vec![1.0],
            )
        };
        let right = NativeHistogram {
            zero_threshold: 0.75,
            ..histogram(
                vec![Span {
                    offset: 1,
                    length: 1,
                }],
                vec![1.0],
            )
        };

        let result = left.add(&right).unwrap();
        assert_eq!(result.zero_threshold, 1.0);
        assert_eq!(result.zero_count, 1.0);
        assert_eq!(result.positive_buckets, vec![1.0]);
    }

    #[test]
    fn custom_reconciliation_without_shared_bounds_uses_overflow_bucket() {
        let mut left = histogram(
            vec![Span {
                offset: 0,
                length: 1,
            }],
            vec![1.0],
        );
        left.schema = CUSTOM_BUCKETS_SCHEMA;
        left.custom_values = vec![1.0];

        let mut right = histogram(
            vec![Span {
                offset: 0,
                length: 1,
            }],
            vec![2.0],
        );
        right.schema = CUSTOM_BUCKETS_SCHEMA;
        right.custom_values = vec![2.0];

        let result = left.add(&right).unwrap();
        assert!(result.custom_values.is_empty());
        assert_eq!(
            result.positive_spans,
            vec![Span {
                offset: 0,
                length: 1,
            }]
        );
        assert_eq!(result.positive_buckets, vec![3.0]);
    }

    #[test]
    fn promql_eq_ignores_reset_hint_start_timestamp_and_sparse_zeros() {
        let mut left = histogram(
            vec![Span {
                offset: 0,
                length: 2,
            }],
            vec![1.0, 0.0],
        );
        left.reset_hint = CounterResetHint::CounterReset;
        left.start_timestamp = Some(1000);

        let mut right = histogram(
            vec![Span {
                offset: 0,
                length: 1,
            }],
            vec![1.0],
        );
        right.reset_hint = CounterResetHint::NotCounterReset;
        right.start_timestamp = Some(2000);

        assert!(left.promql_eq(&right));
    }

    #[test]
    fn promql_eq_compares_payload_floats_by_bits() {
        let nan = f64::from_bits(0x7ff8_0000_0000_0001);
        let other_nan = f64::from_bits(0x7ff8_0000_0000_0002);
        let mut left = histogram(
            vec![Span {
                offset: 0,
                length: 1,
            }],
            vec![nan],
        );
        left.count = nan;
        left.zero_count = nan;
        left.sum = nan;
        let right = left.clone();

        assert!(left.promql_eq(&right));

        for changed in [
            NativeHistogram {
                count: other_nan,
                ..right.clone()
            },
            NativeHistogram {
                zero_count: other_nan,
                ..right.clone()
            },
            NativeHistogram {
                sum: other_nan,
                ..right.clone()
            },
            NativeHistogram {
                positive_buckets: vec![other_nan],
                ..right.clone()
            },
        ] {
            assert!(!left.promql_eq(&changed));
        }

        let undecodable = histogram(
            vec![Span {
                offset: i32::MAX,
                length: 1,
            }],
            vec![1.0],
        );
        assert!(undecodable.promql_eq(&undecodable.clone()));
    }

    #[test]
    fn detect_reset_preserves_layout_direction() {
        let mut previous = histogram(
            vec![Span {
                offset: 1,
                length: 1,
            }],
            vec![1.0],
        );
        previous.reset_hint = CounterResetHint::Unknown;

        let mut higher_resolution = previous.clone();
        higher_resolution.schema = 1;
        higher_resolution.positive_spans[0].offset = 2;
        assert!(higher_resolution.detect_reset(&previous));

        let mut lower_resolution = previous.clone();
        lower_resolution.schema = 0;
        lower_resolution.positive_spans[0].offset = 1;
        let mut previous_higher_resolution = higher_resolution;
        previous_higher_resolution.reset_hint = CounterResetHint::Unknown;
        assert!(!lower_resolution.detect_reset(&previous_higher_resolution));

        let mut smaller_zero_threshold = previous.clone();
        smaller_zero_threshold.zero_threshold = 0.5;
        previous.zero_threshold = 1.0;
        assert!(smaller_zero_threshold.detect_reset(&previous));

        let mut previous = histogram(
            vec![Span {
                offset: 0,
                length: 1,
            }],
            vec![1.0],
        );
        previous.reset_hint = CounterResetHint::Unknown;
        let mut split_bucket = histogram(Vec::new(), Vec::new());
        split_bucket.reset_hint = CounterResetHint::Unknown;
        split_bucket.count = 1.0;
        split_bucket.zero_count = 1.0;
        split_bucket.zero_threshold = 0.75;
        assert!(split_bucket.detect_reset(&previous));

        split_bucket.zero_threshold = 1.0;
        assert!(!split_bucket.detect_reset(&previous));
    }

    #[test]
    fn extreme_bucket_index_returns_none_instead_of_overflowing() {
        assert_eq!(ceil_div(i32::MIN, 2), i32::MIN / 2);

        let mut histogram = histogram(
            vec![Span {
                offset: i32::MIN,
                length: 1,
            }],
            vec![1.0],
        );
        assert!(histogram.side_buckets(true).is_none());

        histogram.schema = 1;
        let reduced = histogram.copy_to_schema(0).unwrap();
        assert_eq!(reduced.positive_spans[0].offset, i32::MIN / 2);
    }

    #[test]
    fn terminal_exponential_bucket_uses_max_finite_bound() {
        for (schema, index) in [(-1, 512), (0, 1024), (2, 4096)] {
            assert_eq!(get_bound(index, schema, &[]), Some(f64::MAX));
            assert_eq!(get_bound(index + 1, schema, &[]), Some(f64::INFINITY));
        }
    }

    #[test]
    fn fraction_excludes_nan_observations() {
        let mut histogram = histogram(
            vec![Span {
                offset: 0,
                length: 1,
            }],
            vec![8.0],
        );
        histogram.count = 10.0;
        histogram.sum = f64::NAN;

        assert_eq!(histogram.fraction(f64::NEG_INFINITY, f64::INFINITY), 0.8);
    }

    #[test]
    fn subtraction_returns_gauge_histogram() {
        let left = histogram(
            vec![Span {
                offset: 0,
                length: 1,
            }],
            vec![3.0],
        );
        let right = histogram(
            vec![Span {
                offset: 0,
                length: 1,
            }],
            vec![1.0],
        );

        let result = left.sub(&right).unwrap();
        assert_eq!(result.reset_hint, CounterResetHint::Gauge);
        assert_eq!(result.positive_buckets, vec![2.0]);
    }

    #[test]
    fn subtraction_treats_empty_left_as_zero() {
        let left = histogram(vec![], vec![]);
        let mut right = histogram(
            vec![Span {
                offset: 0,
                length: 1,
            }],
            vec![2.0],
        );
        right.schema = CUSTOM_BUCKETS_SCHEMA;
        right.custom_values = vec![1.0];

        let result = left.sub(&right).unwrap();

        assert_eq!(result.schema, CUSTOM_BUCKETS_SCHEMA);
        assert_eq!(result.reset_hint, CounterResetHint::Gauge);
        assert_eq!(result.count, -2.0);
        assert_eq!(result.sum, -2.0);
        assert_eq!(result.positive_buckets, vec![-2.0]);
    }
}

// ---------------------------------------------------------------------------
// Stable Parquet field ids for native-histogram sub-fields.
//
// External readers resolve nested struct fields by `PARQUET:field_id`, so each
// sub-field (and list element) needs a stable positive id. The struct schema
// is fixed (always the same 18 fields). Each histogram column owns a block of
// ids (offset from a reserved base by the column's id), so several histogram
// columns in one table get disjoint sub-field ids. The reserved base is
// disjoint from user column ids and mito2 internal ids (`1 << 30`).
//
// The id is computed with checked arithmetic: the reserved base plus
// `column_id * stride` cannot always fit in a positive `i32` (a `ColumnId` is
// `u32`), so derivation returns `None` once the representable range is
// exceeded. Callers must handle `None` explicitly — the SST parquet writer
// surfaces it as an error rather than wrapping, panicking, or silently
// dropping the field id.
// ---------------------------------------------------------------------------

/// Reserved base for native-histogram struct sub-field ids.
pub const NATIVE_HISTOGRAM_SUBFIELD_ID_BASE: i32 = 0x5000_0000;

/// Number of ids reserved per histogram column (18 sub-fields + headroom for
/// list element ids), so multiple histogram columns get disjoint ids.
pub const NATIVE_HISTOGRAM_SUBFIELD_ID_STRIDE: i32 = 64;

/// Offset of list element ids within a column's id block.
pub const NATIVE_HISTOGRAM_LIST_ELEMENT_OFFSET: i32 = 32;

/// Returns the stable field id for a native-histogram struct sub-field,
/// namespaced by its parent `column_id`, or `None` if `name` is not a known
/// sub-field or the derived id overflows a positive `i32`.
pub fn native_histogram_subfield_id(column_id: i32, name: &str) -> Option<i32> {
    let idx = subfield_index(name)?;
    NATIVE_HISTOGRAM_SUBFIELD_ID_BASE
        .checked_add(column_id.checked_mul(NATIVE_HISTOGRAM_SUBFIELD_ID_STRIDE)?)
        .and_then(|v| v.checked_add(idx))
}

/// Returns the stable list `element-id` for a list-typed native-histogram
/// sub-field, namespaced by its parent `column_id`, or `None` if `name` is not
/// a known sub-field or the derived id overflows a positive `i32`.
pub fn native_histogram_list_element_id(column_id: i32, name: &str) -> Option<i32> {
    let idx = subfield_index(name)?;
    NATIVE_HISTOGRAM_SUBFIELD_ID_BASE
        .checked_add(column_id.checked_mul(NATIVE_HISTOGRAM_SUBFIELD_ID_STRIDE)?)
        .and_then(|v| v.checked_add(NATIVE_HISTOGRAM_LIST_ELEMENT_OFFSET))
        .and_then(|v| v.checked_add(idx))
}

fn subfield_index(name: &str) -> Option<i32> {
    NATIVE_HISTOGRAM_FIELD_NAMES
        .iter()
        .position(|n| *n == name)
        .map(|i| i as i32)
}

#[cfg(test)]
mod subfield_id_tests {
    use super::*;

    #[test]
    fn subfield_ids_are_namespaced_and_disjoint() {
        // SCHEMA=0, ZERO_THRESHOLD=1, SUM=2, ..., CUSTOM_VALUES=5.
        let sum_idx = 2;
        let custom_values_idx = 5;

        // Ids are offset from the reserved base by the parent column id and
        // the sub-field index.
        assert_eq!(
            native_histogram_subfield_id(1, SUM_FIELD),
            Some(NATIVE_HISTOGRAM_SUBFIELD_ID_BASE + NATIVE_HISTOGRAM_SUBFIELD_ID_STRIDE + sum_idx,)
        );
        // List element ids additionally carry the list offset.
        assert_eq!(
            native_histogram_list_element_id(1, CUSTOM_VALUES_FIELD),
            Some(
                NATIVE_HISTOGRAM_SUBFIELD_ID_BASE
                    + NATIVE_HISTOGRAM_SUBFIELD_ID_STRIDE
                    + NATIVE_HISTOGRAM_LIST_ELEMENT_OFFSET
                    + custom_values_idx,
            )
        );
        // Different parent columns get disjoint ids for the same sub-field.
        assert_ne!(
            native_histogram_subfield_id(1, SUM_FIELD),
            native_histogram_subfield_id(7, SUM_FIELD)
        );
        // Unknown sub-field name -> None.
        assert_eq!(native_histogram_subfield_id(1, "not_a_field"), None);
    }

    #[test]
    fn subfield_ids_overflow_returns_none() {
        // `column_id` is u32-sized, but the derived id must fit in a positive
        // i32. At column_id = 12_582_912, BASE + column_id*64 == i32::MAX + 1,
        // which previously overflowed (debug panic / release wrap). Checked
        // arithmetic must yield None instead of wrapping or panicking.
        assert_eq!(native_histogram_subfield_id(12_582_912, SUM_FIELD), None);
        assert_eq!(
            native_histogram_list_element_id(12_582_912, CUSTOM_VALUES_FIELD),
            None
        );
        // One below that boundary is still representable.
        assert!(native_histogram_subfield_id(12_582_911, SUM_FIELD).is_some());
    }
}
