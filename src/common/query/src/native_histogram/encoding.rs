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
use api::greptime_proto::io::prometheus::write::v2::{BucketSpan, Histogram};
use api::helper::ColumnDataTypeWrapper;
use api::prom_store::remote;
use api::v1::value::ValueData;
use api::v1::{ColumnSchema, ListValue, SemanticType, Value};
use datafusion::arrow::array::{Array, StructArray};
use datafusion::arrow::datatypes::{Float64Type, Int32Type, Int64Type, TimestampMillisecondType};
use snafu::{Snafu, ensure};

use crate::native_histogram::{
    self, CUSTOM_BUCKETS_SCHEMA, MAX_EXPONENTIAL_SCHEMA, NATIVE_HISTOGRAM_FIELD_NAMES,
    exponential_overflow_bucket_index, native_histogram_value_type,
};
use crate::prelude::greptime_native_histogram;

const MAX_REDUCIBLE_NATIVE_HISTOGRAM_SCHEMA: i32 = 52;

/// Error returned while validating or encoding a native histogram.
#[derive(Debug, Snafu)]
#[snafu(display("{message}"))]
pub struct NativeHistogramError {
    message: String,
}

type Result<T> = std::result::Result<T, NativeHistogramError>;

/// Returns the canonical column schema for a native histogram value.
pub fn native_histogram_column_schema() -> Result<ColumnSchema> {
    let (datatype, datatype_extension) =
        ColumnDataTypeWrapper::try_from(native_histogram_value_type().clone())
            .map_err(|error| NativeHistogramError {
                message: format!("native histogram type cannot be encoded: {error}"),
            })?
            .into_parts();

    Ok(ColumnSchema {
        column_name: greptime_native_histogram().to_string(),
        datatype: datatype as i32,
        semantic_type: SemanticType::Field as i32,
        datatype_extension,
        options: None,
    })
}

/// Validates and encodes a Prometheus histogram into the canonical Struct value.
pub fn encode_native_histogram(histogram: &Histogram) -> Result<ValueData> {
    let uses_float_counts = native_histogram_uses_float_counts(histogram)?;
    validate_native_histogram(histogram, uses_float_counts)?;

    let mut items = Vec::with_capacity(NATIVE_HISTOGRAM_FIELD_NAMES.len());
    let positive_span_lengths = i32_span_lengths("positive", &histogram.positive_spans)?;
    let negative_span_lengths = i32_span_lengths("negative", &histogram.negative_spans)?;
    items.extend([
        pb_value(ValueData::I32Value(histogram.schema)),
        pb_value(ValueData::F64Value(histogram.zero_threshold)),
        pb_value(ValueData::F64Value(histogram.sum)),
        pb_value(ValueData::I32Value(histogram.reset_hint)),
        optional_pb_value((histogram.start_timestamp != 0).then_some(
            ValueData::TimestampMillisecondValue(histogram.start_timestamp),
        )),
        f64_list_value(histogram.custom_values.iter().copied()),
        i32_list_value(histogram.positive_spans.iter().map(|span| span.offset)),
        i32_list_value(positive_span_lengths),
        i32_list_value(histogram.negative_spans.iter().map(|span| span.offset)),
        i32_list_value(negative_span_lengths),
    ]);

    if uses_float_counts {
        let count = match histogram.count.as_ref() {
            Some(Count::CountFloat(count)) => *count,
            _ => 0.0,
        };
        let zero_count = match histogram.zero_count.as_ref() {
            Some(ZeroCount::ZeroCountFloat(zero_count)) => *zero_count,
            _ => 0.0,
        };
        validate_float_native_histogram_counts(
            count,
            zero_count,
            &histogram.positive_counts,
            &histogram.negative_counts,
        )?;
        items.extend([
            null_pb_value(),
            null_pb_value(),
            i64_list_value(std::iter::empty()),
            i64_list_value(std::iter::empty()),
            pb_value(ValueData::F64Value(count)),
            pb_value(ValueData::F64Value(zero_count)),
            f64_list_value(histogram.positive_counts.iter().copied()),
            f64_list_value(histogram.negative_counts.iter().copied()),
        ]);
    } else {
        let count = match histogram.count.as_ref() {
            Some(Count::CountInt(count)) => *count,
            _ => 0,
        };
        let zero_count = match histogram.zero_count.as_ref() {
            Some(ZeroCount::ZeroCountInt(zero_count)) => *zero_count,
            _ => 0,
        };
        let positive_buckets = bucket_counts_from_deltas(&histogram.positive_deltas)?;
        let negative_buckets = bucket_counts_from_deltas(&histogram.negative_deltas)?;
        validate_integer_native_histogram_counts(
            count,
            zero_count,
            histogram.sum,
            &positive_buckets,
            &negative_buckets,
        )?;
        let count = i64::try_from(count).map_err(|_| NativeHistogramError {
            message: format!("native histogram integer count {count} overflows i64"),
        })?;
        let zero_count = i64::try_from(zero_count).map_err(|_| NativeHistogramError {
            message: format!("native histogram integer zero_count {zero_count} overflows i64"),
        })?;
        items.extend([
            pb_value(ValueData::I64Value(count)),
            pb_value(ValueData::I64Value(zero_count)),
            i64_list_value(positive_buckets),
            i64_list_value(negative_buckets),
            null_pb_value(),
            null_pb_value(),
            f64_list_value(std::iter::empty()),
            f64_list_value(std::iter::empty()),
        ]);
    }

    Ok(ValueData::StructValue(api::v1::StructValue { items }))
}

/// Decodes a stored histogram without rounding integer counts through `f64`.
///
/// Null structs are absent samples. Malformed or sampled-wire-incompatible
/// values are errors. The sampled format has no field for `start_timestamp`.
pub fn decode_native_histogram_for_remote_read(
    array: &StructArray,
    row: usize,
    timestamp: i64,
) -> Result<Option<remote::Histogram>> {
    use crate::native_histogram::{
        COUNT_F64_FIELD, COUNT_I64_FIELD, CUSTOM_VALUES_FIELD, NEGATIVE_BUCKETS_F64_FIELD,
        NEGATIVE_BUCKETS_I64_FIELD, NEGATIVE_SPAN_LENGTHS_FIELD, NEGATIVE_SPAN_OFFSETS_FIELD,
        POSITIVE_BUCKETS_F64_FIELD, POSITIVE_BUCKETS_I64_FIELD, POSITIVE_SPAN_LENGTHS_FIELD,
        POSITIVE_SPAN_OFFSETS_FIELD, RESET_HINT_FIELD, SCHEMA_FIELD, START_TIMESTAMP_FIELD,
        SUM_FIELD, ZERO_COUNT_F64_FIELD, ZERO_COUNT_I64_FIELD, ZERO_THRESHOLD_FIELD,
    };

    ensure!(
        row < array.len(),
        NativeHistogramSnafu {
            message: "native histogram row is out of bounds"
        }
    );
    if array.is_null(row) {
        return Ok(None);
    }
    let invalid_field = |error| NativeHistogramError {
        message: format!("invalid stored native histogram: {error}"),
    };
    let read_spans = |name, offsets, lengths| -> Result<Vec<remote::BucketSpan>> {
        let offsets = native_histogram::list_values::<Int32Type>(array, offsets, row)
            .map_err(invalid_field)?;
        let lengths = native_histogram::list_values::<Int32Type>(array, lengths, row)
            .map_err(invalid_field)?;
        native_histogram::read_spans(offsets, lengths, name)
            .map_err(invalid_field)?
            .into_iter()
            .map(|span| {
                Ok(remote::BucketSpan {
                    offset: span.offset,
                    length: u32::try_from(span.length).map_err(|_| NativeHistogramError {
                        message: format!("native histogram {name} span length is negative"),
                    })?,
                })
            })
            .collect()
    };
    let mut histogram = remote::Histogram {
        schema: native_histogram::required_primitive::<Int32Type>(array, SCHEMA_FIELD, row)
            .map_err(invalid_field)?,
        zero_threshold: native_histogram::required_primitive::<Float64Type>(
            array,
            ZERO_THRESHOLD_FIELD,
            row,
        )
        .map_err(invalid_field)?,
        sum: native_histogram::required_primitive::<Float64Type>(array, SUM_FIELD, row)
            .map_err(invalid_field)?,
        reset_hint: native_histogram::required_primitive::<Int32Type>(array, RESET_HINT_FIELD, row)
            .map_err(invalid_field)?,
        custom_values: native_histogram::list_values::<Float64Type>(
            array,
            CUSTOM_VALUES_FIELD,
            row,
        )
        .map_err(invalid_field)?,
        positive_spans: read_spans(
            "positive",
            POSITIVE_SPAN_OFFSETS_FIELD,
            POSITIVE_SPAN_LENGTHS_FIELD,
        )?,
        negative_spans: read_spans(
            "negative",
            NEGATIVE_SPAN_OFFSETS_FIELD,
            NEGATIVE_SPAN_LENGTHS_FIELD,
        )?,
        timestamp,
        ..Default::default()
    };
    // Validate the stored child even though the sampled wire format cannot carry it.
    native_histogram::optional_primitive::<TimestampMillisecondType>(
        array,
        START_TIMESTAMP_FIELD,
        row,
    )
    .map_err(invalid_field)?;
    let count_i64 = native_histogram::optional_primitive::<Int64Type>(array, COUNT_I64_FIELD, row)
        .map_err(invalid_field)?;
    let count_f64 =
        native_histogram::optional_primitive::<Float64Type>(array, COUNT_F64_FIELD, row)
            .map_err(invalid_field)?;
    let zero_i64 =
        native_histogram::optional_primitive::<Int64Type>(array, ZERO_COUNT_I64_FIELD, row)
            .map_err(invalid_field)?;
    let zero_f64 =
        native_histogram::optional_primitive::<Float64Type>(array, ZERO_COUNT_F64_FIELD, row)
            .map_err(invalid_field)?;
    let positive_i64 =
        native_histogram::list_values::<Int64Type>(array, POSITIVE_BUCKETS_I64_FIELD, row)
            .map_err(invalid_field)?;
    let negative_i64 =
        native_histogram::list_values::<Int64Type>(array, NEGATIVE_BUCKETS_I64_FIELD, row)
            .map_err(invalid_field)?;
    let positive_f64 =
        native_histogram::list_values::<Float64Type>(array, POSITIVE_BUCKETS_F64_FIELD, row)
            .map_err(invalid_field)?;
    let negative_f64 =
        native_histogram::list_values::<Float64Type>(array, NEGATIVE_BUCKETS_F64_FIELD, row)
            .map_err(invalid_field)?;

    let (zero_count_is_zero, positive_len, negative_len) = match (count_i64, count_f64) {
        (Some(count), None) => {
            ensure!(
                zero_f64.is_none() && positive_f64.is_empty() && negative_f64.is_empty(),
                NativeHistogramSnafu {
                    message: "integer native histogram has populated float fields"
                }
            );
            let count = u64::try_from(count).map_err(|_| NativeHistogramError {
                message: "native histogram integer count is negative".to_string(),
            })?;
            let zero_count =
                u64::try_from(zero_i64.unwrap_or_default()).map_err(|_| NativeHistogramError {
                    message: "native histogram integer zero_count is negative".to_string(),
                })?;
            validate_integer_native_histogram_counts(
                count,
                zero_count,
                histogram.sum,
                &positive_i64,
                &negative_i64,
            )?;
            histogram.count = Some(remote::histogram::Count::CountInt(count));
            histogram.zero_count = Some(remote::histogram::ZeroCount::ZeroCountInt(zero_count));
            histogram.positive_deltas = bucket_deltas_from_counts(&positive_i64)?;
            histogram.negative_deltas = bucket_deltas_from_counts(&negative_i64)?;
            (zero_count == 0, positive_i64.len(), negative_i64.len())
        }
        (None, Some(count)) => {
            ensure!(
                zero_i64.is_none() && positive_i64.is_empty() && negative_i64.is_empty(),
                NativeHistogramSnafu {
                    message: "float native histogram has populated integer fields"
                }
            );
            let zero_count = zero_f64.unwrap_or_default();
            validate_float_native_histogram_counts(
                count,
                zero_count,
                &positive_f64,
                &negative_f64,
            )?;
            histogram.count = Some(remote::histogram::Count::CountFloat(count));
            histogram.zero_count = Some(remote::histogram::ZeroCount::ZeroCountFloat(zero_count));
            histogram.positive_counts = positive_f64;
            histogram.negative_counts = negative_f64;
            (
                zero_count == 0.0,
                histogram.positive_counts.len(),
                histogram.negative_counts.len(),
            )
        }
        _ => {
            return NativeHistogramSnafu {
                message: "stored native histogram must have exactly one populated count family",
            }
            .fail();
        }
    };
    validate_native_histogram_layout(
        histogram.schema,
        histogram.zero_threshold,
        zero_count_is_zero,
        &histogram.custom_values,
        (
            histogram
                .positive_spans
                .iter()
                .map(|s| (s.offset, s.length)),
            positive_len,
        ),
        (
            histogram
                .negative_spans
                .iter()
                .map(|s| (s.offset, s.length)),
            negative_len,
        ),
    )?;
    Ok(Some(histogram))
}

fn bucket_deltas_from_counts(counts: &[i64]) -> Result<Vec<i64>> {
    let mut previous = 0i64;
    counts
        .iter()
        .map(|&count| {
            let delta = count
                .checked_sub(previous)
                .ok_or_else(|| NativeHistogramError {
                    message: "native histogram bucket delta overflows i64".to_string(),
                })?;
            previous = count;
            Ok(delta)
        })
        .collect()
}

fn validate_native_histogram(histogram: &Histogram, uses_float_counts: bool) -> Result<()> {
    let (positive_buckets, negative_buckets) = if uses_float_counts {
        (
            histogram.positive_counts.len(),
            histogram.negative_counts.len(),
        )
    } else {
        (
            histogram.positive_deltas.len(),
            histogram.negative_deltas.len(),
        )
    };
    validate_native_histogram_layout(
        histogram.schema,
        histogram.zero_threshold,
        native_histogram_zero_count_is_zero(histogram),
        &histogram.custom_values,
        (
            histogram
                .positive_spans
                .iter()
                .map(|s| (s.offset, s.length)),
            positive_buckets,
        ),
        (
            histogram
                .negative_spans
                .iter()
                .map(|s| (s.offset, s.length)),
            negative_buckets,
        ),
    )
}

fn validate_native_histogram_layout(
    schema: i32,
    zero_threshold: f64,
    zero_count_is_zero: bool,
    custom_values: &[f64],
    positive: (impl ExactSizeIterator<Item = (i32, u32)> + Clone, usize),
    negative: (impl ExactSizeIterator<Item = (i32, u32)> + Clone, usize),
) -> Result<()> {
    let exponential_overflow_index = validate_native_histogram_schema(schema)?;
    validate_native_histogram_custom_values(schema, custom_values)?;
    if schema == CUSTOM_BUCKETS_SCHEMA {
        ensure!(
            zero_threshold == 0.0 && zero_count_is_zero,
            NativeHistogramSnafu {
                message: "custom native histogram must not use a zero bucket"
            }
        );
        ensure!(
            negative.0.len() == 0 && negative.1 == 0,
            NativeHistogramSnafu {
                message: "custom native histogram must not use negative buckets"
            }
        );
    }

    let bucket_index_range = if let Some(overflow_index) = exponential_overflow_index {
        (i32::MIN, overflow_index)
    } else {
        (
            0,
            i32::try_from(custom_values.len()).map_err(|_| NativeHistogramError {
                message: "custom native histogram has too many custom_values".to_string(),
            })?,
        )
    };
    let positive_extent =
        validate_native_histogram_spans("positive", positive.0, positive.1, bucket_index_range)?;
    validate_native_histogram_spans("negative", negative.0, negative.1, bucket_index_range)?;

    if schema == CUSTOM_BUCKETS_SCHEMA {
        let max_extent =
            bucket_index_range
                .1
                .checked_add(1)
                .ok_or_else(|| NativeHistogramError {
                    message: "custom native histogram extent overflows i32".to_string(),
                })?;
        ensure!(
            positive_extent <= max_extent,
            NativeHistogramSnafu {
                message: "custom native histogram span extent exceeds custom_values plus the overflow bucket"
            }
        );
    }

    Ok(())
}

fn validate_native_histogram_schema(schema: i32) -> Result<Option<i32>> {
    if schema == CUSTOM_BUCKETS_SCHEMA {
        return Ok(None);
    }

    if let Some(overflow_index) = exponential_overflow_bucket_index(schema) {
        return Ok(Some(overflow_index));
    }

    if (MAX_EXPONENTIAL_SCHEMA + 1..=MAX_REDUCIBLE_NATIVE_HISTOGRAM_SCHEMA).contains(&schema) {
        Err(NativeHistogramError {
            message: format!("native histogram schema {schema} must be reduced before ingestion"),
        })
    } else {
        Err(NativeHistogramError {
            message: format!("native histogram schema {schema} is unsupported"),
        })
    }
}

fn validate_native_histogram_custom_values(schema: i32, custom_values: &[f64]) -> Result<()> {
    if schema != CUSTOM_BUCKETS_SCHEMA {
        ensure!(
            custom_values.is_empty(),
            NativeHistogramSnafu {
                message: "standard native histogram must not use custom_values"
            }
        );
        return Ok(());
    }

    for value in custom_values {
        ensure!(
            !value.is_nan() && *value != f64::INFINITY,
            NativeHistogramSnafu {
                message: "custom native histogram custom_values must not contain +Inf or NaN"
            }
        );
    }
    for values in custom_values.windows(2) {
        ensure!(
            values[0] < values[1],
            NativeHistogramSnafu {
                message: "custom native histogram custom_values must be sorted"
            }
        );
    }

    Ok(())
}

fn validate_native_histogram_spans(
    name: &str,
    spans: impl Iterator<Item = (i32, u32)> + Clone,
    bucket_count: usize,
    bucket_index_range: (i32, i32),
) -> Result<i32> {
    let span_len = spans.clone().try_fold(0usize, |sum, (_, length)| {
        let length = usize::try_from(length).map_err(|_| NativeHistogramError {
            message: format!("native histogram {name} span length exceeds usize"),
        })?;
        sum.checked_add(length).ok_or_else(|| NativeHistogramError {
            message: format!("native histogram {name} spans overflow"),
        })
    })?;
    ensure!(
        span_len == bucket_count,
        NativeHistogramSnafu {
            message: format!(
                "native histogram {name} spans describe {span_len} buckets, found {bucket_count}"
            )
        }
    );

    let mut current_index = 0i32;
    for (span_index, (offset, length)) in spans.enumerate() {
        ensure!(
            offset >= 0 || (span_index == 0 && bucket_index_range.0 == i32::MIN),
            NativeHistogramSnafu {
                message: format!(
                    "native histogram {name} span {} has negative offset {}",
                    span_index + 1,
                    offset
                )
            }
        );
        ensure!(
            span_index != 0 || offset != i32::MIN,
            NativeHistogramSnafu {
                message: format!(
                    "native histogram {name} initial span offset i32::MIN is incompatible with Remote Read"
                )
            }
        );
        current_index = if span_index == 0 {
            offset
        } else {
            current_index
                .checked_add(offset)
                .ok_or_else(|| NativeHistogramError {
                    message: format!("native histogram {name} span index overflows i32"),
                })?
        };

        for _ in 0..length {
            ensure!(
                (bucket_index_range.0..=bucket_index_range.1).contains(&current_index),
                NativeHistogramSnafu {
                    message: format!(
                        "native histogram {name} bucket index {current_index} is out of range"
                    )
                }
            );
            current_index = current_index
                .checked_add(1)
                .ok_or_else(|| NativeHistogramError {
                    message: format!("native histogram {name} span index overflows i32"),
                })?;
        }
    }

    Ok(current_index)
}

fn validate_float_native_histogram_counts(
    count: f64,
    zero_count: f64,
    positive_buckets: &[f64],
    negative_buckets: &[f64],
) -> Result<()> {
    ensure!(
        count >= 0.0 || count.is_nan(),
        NativeHistogramSnafu {
            message: "native histogram float count must not be negative"
        }
    );

    ensure!(
        zero_count >= 0.0 || zero_count.is_nan(),
        NativeHistogramSnafu {
            message: "native histogram float zero_count must not be negative"
        }
    );

    for (name, counts) in [
        ("positive", positive_buckets),
        ("negative", negative_buckets),
    ] {
        for (index, count) in counts.iter().enumerate() {
            ensure!(
                *count >= 0.0 || count.is_nan(),
                NativeHistogramSnafu {
                    message: format!(
                        "native histogram {name} bucket {} count must not be negative",
                        index + 1
                    )
                }
            );
        }
    }

    Ok(())
}

fn validate_integer_native_histogram_counts(
    count: u64,
    zero_count: u64,
    sum: f64,
    positive_buckets: &[i64],
    negative_buckets: &[i64],
) -> Result<()> {
    let bucket_count =
        positive_buckets
            .iter()
            .chain(negative_buckets)
            .try_fold(zero_count, |total, bucket| {
                let bucket = u64::try_from(*bucket).map_err(|_| NativeHistogramError {
                    message: "native histogram bucket count is negative".to_string(),
                })?;
                total
                    .checked_add(bucket)
                    .ok_or_else(|| NativeHistogramError {
                        message: "native histogram bucket total overflows u64".to_string(),
                    })
            })?;
    ensure!(
        if sum.is_nan() {
            bucket_count <= count
        } else {
            bucket_count == count
        },
        NativeHistogramSnafu {
            message: format!(
                "native histogram has {bucket_count} observations in buckets, count is {count}"
            )
        }
    );

    Ok(())
}

fn native_histogram_zero_count_is_zero(histogram: &Histogram) -> bool {
    match histogram.zero_count.as_ref() {
        Some(ZeroCount::ZeroCountInt(zero_count)) => *zero_count == 0,
        Some(ZeroCount::ZeroCountFloat(zero_count)) => *zero_count == 0.0,
        None => true,
    }
}

fn native_histogram_uses_float_counts(histogram: &Histogram) -> Result<bool> {
    let uses_float_count = matches!(histogram.count, Some(Count::CountFloat(_)))
        || matches!(histogram.zero_count, Some(ZeroCount::ZeroCountFloat(_)));
    let uses_int_count = matches!(histogram.count, Some(Count::CountInt(_)))
        || matches!(histogram.zero_count, Some(ZeroCount::ZeroCountInt(_)));
    let uses_float_buckets =
        !histogram.positive_counts.is_empty() || !histogram.negative_counts.is_empty();
    let uses_int_buckets =
        !histogram.positive_deltas.is_empty() || !histogram.negative_deltas.is_empty();

    ensure!(
        !matches!(
            (&histogram.count, &histogram.zero_count),
            (Some(Count::CountInt(_)), Some(ZeroCount::ZeroCountFloat(_)))
                | (Some(Count::CountFloat(_)), Some(ZeroCount::ZeroCountInt(_)))
        ),
        NativeHistogramSnafu {
            message: "native histogram count and zero_count must use the same integer or float family"
        }
    );
    ensure!(
        !(uses_float_buckets && uses_int_buckets),
        NativeHistogramSnafu {
            message: "native histogram bucket counts must use either integer deltas or float counts"
        }
    );
    ensure!(
        !(uses_float_count && uses_int_buckets),
        NativeHistogramSnafu {
            message: "float native histogram must not use integer bucket deltas"
        }
    );
    ensure!(
        !(uses_int_count && uses_float_buckets),
        NativeHistogramSnafu {
            message: "integer native histogram must not use float bucket counts"
        }
    );

    Ok(uses_float_count || uses_float_buckets)
}

fn pb_value(value_data: ValueData) -> Value {
    optional_pb_value(Some(value_data))
}

fn null_pb_value() -> Value {
    optional_pb_value(None)
}

fn optional_pb_value(value_data: Option<ValueData>) -> Value {
    Value { value_data }
}

fn list_value(values: impl IntoIterator<Item = ValueData>) -> Value {
    pb_value(ValueData::ListValue(ListValue {
        items: values.into_iter().map(pb_value).collect(),
    }))
}

fn i32_list_value(values: impl IntoIterator<Item = i32>) -> Value {
    list_value(values.into_iter().map(ValueData::I32Value))
}

fn i32_span_lengths(name: &str, spans: &[BucketSpan]) -> Result<Vec<i32>> {
    spans
        .iter()
        .map(|span| {
            i32::try_from(span.length).map_err(|_| NativeHistogramError {
                message: format!(
                    "native histogram {name} span length {} overflows i32",
                    span.length
                ),
            })
        })
        .collect()
}

fn i64_list_value(values: impl IntoIterator<Item = i64>) -> Value {
    list_value(values.into_iter().map(ValueData::I64Value))
}

fn f64_list_value(values: impl IntoIterator<Item = f64>) -> Value {
    list_value(values.into_iter().map(ValueData::F64Value))
}

fn bucket_counts_from_deltas(deltas: &[i64]) -> Result<Vec<i64>> {
    let mut count = 0_i64;
    let mut buckets = Vec::with_capacity(deltas.len());

    for delta in deltas {
        count = count
            .checked_add(*delta)
            .ok_or_else(|| NativeHistogramError {
                message: "native histogram bucket count overflows i64".to_string(),
            })?;
        ensure!(
            count >= 0,
            NativeHistogramSnafu {
                message: "native histogram bucket count is negative"
            }
        );
        buckets.push(count);
    }

    Ok(buckets)
}

#[cfg(test)]
mod tests {
    use super::*;

    use std::sync::Arc;

    use datafusion::arrow::array::{ArrayRef, Float64Array, Int32Array};
    use datafusion::arrow::datatypes::Field;

    use crate::native_histogram::testing::histogram_array;
    use crate::native_histogram::*;

    fn empty_histogram(float: bool) -> Histogram {
        Histogram {
            count: Some(if float {
                Count::CountFloat(0.0)
            } else {
                Count::CountInt(0)
            }),
            zero_count: Some(if float {
                ZeroCount::ZeroCountFloat(0.0)
            } else {
                ZeroCount::ZeroCountInt(0)
            }),
            ..Default::default()
        }
    }

    fn stored(histogram: &Histogram) -> StructArray {
        histogram_array(&[Some(encode_native_histogram(histogram).unwrap())])
    }

    fn modified(histogram: &Histogram, changes: &[(&str, Value)]) -> StructArray {
        let ValueData::StructValue(mut value) = encode_native_histogram(histogram).unwrap() else {
            unreachable!()
        };
        for (name, replacement) in changes {
            let index = NATIVE_HISTOGRAM_FIELD_NAMES
                .iter()
                .position(|field| field == name)
                .unwrap();
            value.items[index] = replacement.clone();
        }
        histogram_array(&[Some(ValueData::StructValue(value))])
    }

    fn assert_invalid(array: &StructArray, message: &str) {
        let error = decode_native_histogram_for_remote_read(array, 0, 123).unwrap_err();
        assert!(
            error.to_string().contains(message),
            "{error}: expected {message}"
        );
    }

    #[test]
    fn native_histogram_remote_read_preserves_integer_counts_and_deltas() {
        for count in [(1u64 << 53) + 1, i64::MAX as u64] {
            let histogram = Histogram {
                count: Some(Count::CountInt(count)),
                zero_count: Some(ZeroCount::ZeroCountInt(count - 12)),
                positive_spans: vec![
                    BucketSpan {
                        offset: -2,
                        length: 2,
                    },
                    BucketSpan {
                        offset: 3,
                        length: 1,
                    },
                ],
                negative_spans: vec![
                    BucketSpan {
                        offset: -3,
                        length: 1,
                    },
                    BucketSpan {
                        offset: 2,
                        length: 2,
                    },
                ],
                positive_deltas: vec![1, 2, -1],
                negative_deltas: vec![1, 2, -1],
                sum: 10.0,
                reset_hint: 42,
                start_timestamp: 123,
                ..Default::default()
            };
            let array = stored(&histogram);
            assert_eq!(
                native_histogram::list_values::<Int64Type>(&array, POSITIVE_BUCKETS_I64_FIELD, 0)
                    .unwrap(),
                vec![1, 3, 2]
            );
            assert_eq!(
                native_histogram::list_values::<Int64Type>(&array, NEGATIVE_BUCKETS_I64_FIELD, 0)
                    .unwrap(),
                vec![1, 3, 2]
            );
            let decoded = decode_native_histogram_for_remote_read(&array, 0, -2)
                .unwrap()
                .unwrap();
            assert_eq!(
                decoded.count,
                Some(remote::histogram::Count::CountInt(count))
            );
            assert_eq!(
                decoded.zero_count,
                Some(remote::histogram::ZeroCount::ZeroCountInt(count - 12))
            );
            assert_eq!(decoded.positive_deltas, vec![1, 2, -1]);
            assert_eq!(decoded.negative_deltas, vec![1, 2, -1]);
            assert_eq!(
                decoded
                    .positive_spans
                    .iter()
                    .map(|s| (s.offset, s.length))
                    .collect::<Vec<_>>(),
                vec![(-2, 2), (3, 1)]
            );
            assert_eq!(decoded.reset_hint, 42);
            assert_eq!(decoded.timestamp, -2);
        }
        // A NaN sum permits unobserved NaNs; an incorrect [3, 2] delta vector
        // would also pass a total check, so assert the deltas independently.
        let histogram = Histogram {
            count: Some(Count::CountInt(8)),
            sum: f64::NAN,
            positive_spans: vec![BucketSpan {
                offset: 0,
                length: 2,
            }],
            positive_deltas: vec![3, -1],
            ..Default::default()
        };
        let decoded = decode_native_histogram_for_remote_read(&stored(&histogram), 0, 0)
            .unwrap()
            .unwrap();
        assert_eq!(decoded.positive_deltas, vec![3, -1]);
    }

    #[test]
    fn native_histogram_remote_read_zero_families_and_float_bits() {
        for float in [false, true] {
            let histogram = empty_histogram(float);
            let array = modified(
                &histogram,
                &[
                    (
                        if float {
                            ZERO_COUNT_F64_FIELD
                        } else {
                            ZERO_COUNT_I64_FIELD
                        },
                        null_pb_value(),
                    ),
                    (POSITIVE_BUCKETS_I64_FIELD, null_pb_value()),
                    (POSITIVE_BUCKETS_F64_FIELD, null_pb_value()),
                ],
            );
            let decoded = decode_native_histogram_for_remote_read(&array, 0, 1)
                .unwrap()
                .unwrap();
            assert_eq!(
                matches!(
                    decoded.count,
                    Some(remote::histogram::Count::CountFloat(0.0))
                ),
                float
            );
            assert_eq!(
                matches!(decoded.count, Some(remote::histogram::Count::CountInt(0))),
                !float
            );
            assert_eq!(
                matches!(
                    decoded.zero_count,
                    Some(remote::histogram::ZeroCount::ZeroCountFloat(0.0))
                ),
                float
            );
            assert_eq!(
                matches!(
                    decoded.zero_count,
                    Some(remote::histogram::ZeroCount::ZeroCountInt(0))
                ),
                !float
            );
            for bits in [0x7ff8_0000_0000_0123, 0x7ff0_0000_0000_0002] {
                let histogram = Histogram {
                    sum: f64::from_bits(bits),
                    ..histogram.clone()
                };
                let decoded = decode_native_histogram_for_remote_read(&stored(&histogram), 0, 1)
                    .unwrap()
                    .unwrap();
                assert_eq!(decoded.sum.to_bits(), bits);
            }
        }
        let histogram = Histogram {
            count: Some(Count::CountFloat(0.3)),
            positive_spans: vec![BucketSpan {
                offset: 0,
                length: 2,
            }],
            positive_counts: vec![0.1, 0.2],
            ..empty_histogram(true)
        };
        let decoded = decode_native_histogram_for_remote_read(&stored(&histogram), 0, 1)
            .unwrap()
            .unwrap();
        assert_eq!(decoded.positive_counts, vec![0.1, 0.2]);
        for value in [f64::INFINITY, f64::from_bits(0x7ff8_0000_0000_0123)] {
            let histogram = Histogram {
                count: Some(Count::CountFloat(value)),
                zero_count: Some(ZeroCount::ZeroCountFloat(value)),
                positive_counts: vec![value, value],
                ..histogram.clone()
            };
            let decoded = decode_native_histogram_for_remote_read(&stored(&histogram), 0, 1)
                .unwrap()
                .unwrap();
            let Some(remote::histogram::Count::CountFloat(count)) = decoded.count else {
                panic!()
            };
            assert_eq!(count.to_bits(), value.to_bits());
            assert_eq!(decoded.positive_counts[0].to_bits(), value.to_bits());
        }
        assert!(
            decode_native_histogram_for_remote_read(&histogram_array(&[None]), 0, 0)
                .unwrap()
                .is_none()
        );
    }

    #[test]
    fn native_histogram_remote_read_rejects_invalid_stored_children() {
        let integer = empty_histogram(false);
        let float = empty_histogram(true);
        for field in [
            SCHEMA_FIELD,
            ZERO_THRESHOLD_FIELD,
            SUM_FIELD,
            RESET_HINT_FIELD,
        ] {
            assert_invalid(&modified(&integer, &[(field, null_pb_value())]), "is null");
        }
        for (histogram, changes, message) in [
            (
                &integer,
                vec![(COUNT_I64_FIELD, null_pb_value())],
                "exactly one",
            ),
            (
                &integer,
                vec![(COUNT_F64_FIELD, pb_value(ValueData::F64Value(0.0)))],
                "exactly one",
            ),
            (
                &integer,
                vec![(ZERO_COUNT_F64_FIELD, pb_value(ValueData::F64Value(0.0)))],
                "populated float",
            ),
            (
                &integer,
                vec![(POSITIVE_BUCKETS_F64_FIELD, f64_list_value([0.0]))],
                "populated float",
            ),
            (
                &integer,
                vec![(NEGATIVE_BUCKETS_F64_FIELD, f64_list_value([0.0]))],
                "populated float",
            ),
            (
                &float,
                vec![(ZERO_COUNT_I64_FIELD, pb_value(ValueData::I64Value(0)))],
                "populated integer",
            ),
            (
                &float,
                vec![(POSITIVE_BUCKETS_I64_FIELD, i64_list_value([0]))],
                "populated integer",
            ),
            (
                &float,
                vec![(NEGATIVE_BUCKETS_I64_FIELD, i64_list_value([0]))],
                "populated integer",
            ),
            (
                &integer,
                vec![(COUNT_I64_FIELD, pb_value(ValueData::I64Value(-1)))],
                "count is negative",
            ),
            (
                &integer,
                vec![(ZERO_COUNT_I64_FIELD, pb_value(ValueData::I64Value(-1)))],
                "zero_count is negative",
            ),
            (
                &integer,
                vec![(POSITIVE_BUCKETS_I64_FIELD, i64_list_value([-1]))],
                "count is negative",
            ),
            (
                &integer,
                vec![(NEGATIVE_BUCKETS_I64_FIELD, i64_list_value([-1]))],
                "count is negative",
            ),
            (
                &float,
                vec![(COUNT_F64_FIELD, pb_value(ValueData::F64Value(-1.0)))],
                "must not be negative",
            ),
            (
                &float,
                vec![(ZERO_COUNT_F64_FIELD, pb_value(ValueData::F64Value(-1.0)))],
                "must not be negative",
            ),
            (
                &float,
                vec![(POSITIVE_BUCKETS_F64_FIELD, f64_list_value([-1.0]))],
                "must not be negative",
            ),
            (
                &float,
                vec![(NEGATIVE_BUCKETS_F64_FIELD, f64_list_value([-1.0]))],
                "must not be negative",
            ),
            (
                &integer,
                vec![
                    (COUNT_I64_FIELD, pb_value(ValueData::I64Value(1))),
                    (
                        POSITIVE_BUCKETS_I64_FIELD,
                        i64_list_value([i64::MAX, i64::MAX, 3]),
                    ),
                ],
                "overflows u64",
            ),
            (
                &integer,
                vec![(COUNT_I64_FIELD, pb_value(ValueData::I64Value(1)))],
                "observations",
            ),
            (
                &integer,
                vec![(SCHEMA_FIELD, pb_value(ValueData::I32Value(53)))],
                "unsupported",
            ),
            (
                &integer,
                vec![(POSITIVE_SPAN_OFFSETS_FIELD, i32_list_value([0]))],
                "offsets and lengths mismatch",
            ),
            (
                &integer,
                vec![
                    (POSITIVE_SPAN_OFFSETS_FIELD, i32_list_value([0])),
                    (POSITIVE_SPAN_LENGTHS_FIELD, i32_list_value([-1])),
                ],
                "negative length",
            ),
            (
                &integer,
                vec![
                    (POSITIVE_SPAN_OFFSETS_FIELD, i32_list_value([0])),
                    (POSITIVE_SPAN_LENGTHS_FIELD, i32_list_value([1])),
                ],
                "describe 1 buckets",
            ),
            (
                &integer,
                vec![
                    (POSITIVE_SPAN_OFFSETS_FIELD, i32_list_value([i32::MAX, 1])),
                    (POSITIVE_SPAN_LENGTHS_FIELD, i32_list_value([0, 0])),
                ],
                "overflows i32",
            ),
            (
                &integer,
                vec![(
                    CUSTOM_VALUES_FIELD,
                    pb_value(ValueData::ListValue(ListValue {
                        items: vec![null_pb_value()],
                    })),
                )],
                "contains null",
            ),
        ] {
            assert_invalid(&modified(histogram, &changes), message);
        }
        let array = stored(&integer);
        let mut fields = array.fields().to_vec();
        let mut children = array.columns().to_vec();
        fields.remove(0);
        children.remove(0);
        assert_invalid(
            &StructArray::new(fields.into(), children, None),
            "missing field schema",
        );
        let mut fields = array.fields().to_vec();
        let mut children = array.columns().to_vec();
        children[0] = Arc::new(Float64Array::from(vec![0.0])) as ArrayRef;
        fields[0] = Arc::new(Field::new(
            SCHEMA_FIELD,
            children[0].data_type().clone(),
            true,
        ));
        assert_invalid(
            &StructArray::new(fields.into(), children, None),
            "invalid type",
        );
        // Even inactive children have to retain their declared types.
        let mut fields = array.fields().to_vec();
        let mut children = array.columns().to_vec();
        children[14] = Arc::new(Int32Array::from(vec![None])) as ArrayRef;
        fields[14] = Arc::new(Field::new(
            COUNT_F64_FIELD,
            children[14].data_type().clone(),
            true,
        ));
        assert_invalid(
            &StructArray::new(fields.into(), children, None),
            "invalid type",
        );
    }

    #[test]
    fn native_histogram_remote_read_schema_rules_match_ingestion() {
        for float in [false, true] {
            let base = empty_histogram(float);
            for bounds in [
                vec![f64::NAN],
                vec![f64::INFINITY],
                vec![1.0, 1.0],
                vec![2.0, 1.0],
            ] {
                let invalid = Histogram {
                    schema: CUSTOM_BUCKETS_SCHEMA,
                    custom_values: bounds.clone(),
                    ..base.clone()
                };
                assert!(encode_native_histogram(&invalid).is_err());
                assert_invalid(
                    &modified(
                        &base,
                        &[
                            (
                                SCHEMA_FIELD,
                                pb_value(ValueData::I32Value(CUSTOM_BUCKETS_SCHEMA)),
                            ),
                            (CUSTOM_VALUES_FIELD, f64_list_value(bounds)),
                        ],
                    ),
                    "custom_values",
                );
            }
            let invalid = Histogram {
                custom_values: vec![1.0],
                ..base.clone()
            };
            assert!(encode_native_histogram(&invalid).is_err());
            assert_invalid(
                &modified(&base, &[(CUSTOM_VALUES_FIELD, f64_list_value([1.0]))]),
                "must not use custom_values",
            );
            let custom = Histogram {
                schema: CUSTOM_BUCKETS_SCHEMA,
                custom_values: vec![-f64::INFINITY, 1.0],
                ..base
            };
            let decoded = decode_native_histogram_for_remote_read(&stored(&custom), 0, 0)
                .unwrap()
                .unwrap();
            assert_eq!(decoded.custom_values, vec![-f64::INFINITY, 1.0]);
            let invalid = Histogram {
                zero_threshold: 1.0,
                ..custom.clone()
            };
            assert!(encode_native_histogram(&invalid).is_err());
            assert_invalid(
                &modified(
                    &custom,
                    &[(ZERO_THRESHOLD_FIELD, pb_value(ValueData::F64Value(1.0)))],
                ),
                "must not use a zero bucket",
            );
            let (count_field, zero_field, bucket_field, one, buckets) = if float {
                (
                    COUNT_F64_FIELD,
                    ZERO_COUNT_F64_FIELD,
                    NEGATIVE_BUCKETS_F64_FIELD,
                    pb_value(ValueData::F64Value(1.0)),
                    f64_list_value([1.0]),
                )
            } else {
                (
                    COUNT_I64_FIELD,
                    ZERO_COUNT_I64_FIELD,
                    NEGATIVE_BUCKETS_I64_FIELD,
                    pb_value(ValueData::I64Value(1)),
                    i64_list_value([1]),
                )
            };
            let invalid = Histogram {
                count: Some(if float {
                    Count::CountFloat(1.0)
                } else {
                    Count::CountInt(1)
                }),
                zero_count: Some(if float {
                    ZeroCount::ZeroCountFloat(1.0)
                } else {
                    ZeroCount::ZeroCountInt(1)
                }),
                ..custom.clone()
            };
            assert!(encode_native_histogram(&invalid).is_err());
            assert_invalid(
                &modified(
                    &custom,
                    &[(count_field, one.clone()), (zero_field, one.clone())],
                ),
                "must not use a zero bucket",
            );
            for length in [0, 1] {
                let mut invalid = custom.clone();
                invalid.negative_spans = vec![BucketSpan { offset: 1, length }];
                let mut changes = vec![
                    (NEGATIVE_SPAN_OFFSETS_FIELD, i32_list_value([1])),
                    (NEGATIVE_SPAN_LENGTHS_FIELD, i32_list_value([length as i32])),
                ];
                if length == 1 {
                    invalid.count = Some(if float {
                        Count::CountFloat(1.0)
                    } else {
                        Count::CountInt(1)
                    });
                    if float {
                        invalid.negative_counts = vec![1.0];
                    } else {
                        invalid.negative_deltas = vec![1];
                    }
                    changes.extend([(count_field, one.clone()), (bucket_field, buckets.clone())]);
                }
                assert!(encode_native_histogram(&invalid).is_err());
                assert_invalid(
                    &modified(&custom, &changes),
                    "must not use negative buckets",
                );
            }
        }
    }

    #[test]
    fn native_histogram_remote_read_span_rules_match_ingestion() {
        for float in [false, true] {
            let custom = Histogram {
                schema: CUSTOM_BUCKETS_SCHEMA,
                custom_values: vec![1.0, 2.0],
                ..empty_histogram(float)
            };
            for custom_values in [vec![], custom.custom_values.clone()] {
                let overflow_index = custom_values.len() as i32;
                for (spans, accepted) in [
                    (vec![(-1, 0)], false),
                    (vec![(overflow_index + 2, 0)], false),
                    (vec![(overflow_index, 0), (2, 0)], false),
                    (vec![(overflow_index + 1, 0)], true),
                    (vec![(overflow_index, 0), (1, 0)], true),
                    (vec![(overflow_index, 1)], true),
                    (vec![(overflow_index + 1, 1)], false),
                ] {
                    let bucket_count = spans.iter().map(|(_, length)| *length).sum::<u32>();
                    let base = Histogram {
                        custom_values: custom_values.clone(),
                        positive_spans: vec![BucketSpan {
                            offset: 0,
                            length: bucket_count,
                        }],
                        positive_counts: if float {
                            vec![0.0; bucket_count as usize]
                        } else {
                            vec![]
                        },
                        positive_deltas: if float {
                            vec![]
                        } else {
                            vec![0; bucket_count as usize]
                        },
                        ..custom.clone()
                    };
                    let write = Histogram {
                        positive_spans: spans
                            .iter()
                            .map(|&(offset, length)| BucketSpan { offset, length })
                            .collect(),
                        ..base.clone()
                    };
                    assert_eq!(encode_native_histogram(&write).is_ok(), accepted);
                    let array = modified(
                        &base,
                        &[
                            (
                                POSITIVE_SPAN_OFFSETS_FIELD,
                                i32_list_value(spans.iter().map(|s| s.0)),
                            ),
                            (
                                POSITIVE_SPAN_LENGTHS_FIELD,
                                i32_list_value(spans.iter().map(|s| s.1 as i32)),
                            ),
                        ],
                    );
                    assert_eq!(
                        decode_native_histogram_for_remote_read(&array, 0, 0).is_ok(),
                        accepted
                    );
                }
            }
            for negative in [false, true] {
                for length in [0, 1] {
                    let mut base = empty_histogram(float);
                    let spans = vec![BucketSpan { offset: 0, length }];
                    if negative {
                        base.negative_spans = spans;
                    } else {
                        base.positive_spans = spans;
                    }
                    if length == 1 {
                        if float {
                            if negative {
                                base.negative_counts = vec![0.0];
                            } else {
                                base.positive_counts = vec![0.0];
                            }
                        } else if negative {
                            base.negative_deltas = vec![0];
                        } else {
                            base.positive_deltas = vec![0];
                        }
                    }
                    for offset in [i32::MIN, i32::MIN + 1, -2, 0] {
                        let mut write = base.clone();
                        let offset_field = if negative {
                            write.negative_spans[0].offset = offset;
                            NEGATIVE_SPAN_OFFSETS_FIELD
                        } else {
                            write.positive_spans[0].offset = offset;
                            POSITIVE_SPAN_OFFSETS_FIELD
                        };
                        if offset == i32::MIN {
                            assert!(encode_native_histogram(&write).is_err());
                            assert_invalid(
                                &modified(&base, &[(offset_field, i32_list_value([offset]))]),
                                "initial span offset i32::MIN",
                            );
                        } else {
                            assert!(
                                decode_native_histogram_for_remote_read(&stored(&write), 0, 0)
                                    .unwrap()
                                    .is_some()
                            );
                        }
                    }
                }
            }
            let base = empty_histogram(float);
            // Out-of-range buckets are different from valid empty end cursors.
            for offset in [-1, 3] {
                let custom = Histogram {
                    positive_spans: vec![BucketSpan {
                        offset: 0,
                        length: 1,
                    }],
                    positive_counts: if float { vec![0.0] } else { vec![] },
                    positive_deltas: if float { vec![] } else { vec![0] },
                    ..custom.clone()
                };
                assert_invalid(
                    &modified(
                        &custom,
                        &[(POSITIVE_SPAN_OFFSETS_FIELD, i32_list_value([offset]))],
                    ),
                    if offset < 0 {
                        "negative offset"
                    } else {
                        "out of range"
                    },
                );
            }
            let exponential = Histogram {
                positive_spans: vec![BucketSpan {
                    offset: 0,
                    length: 1,
                }],
                positive_counts: if float { vec![0.0] } else { vec![] },
                positive_deltas: if float { vec![] } else { vec![0] },
                ..base
            };
            assert_invalid(
                &modified(
                    &exponential,
                    &[(POSITIVE_SPAN_OFFSETS_FIELD, i32_list_value([i32::MAX]))],
                ),
                "out of range",
            );
        }
    }

    #[test]
    fn shared_errors_are_protocol_neutral() {
        let error = encode_native_histogram(&Histogram {
            schema: 9,
            ..Default::default()
        })
        .unwrap_err();

        assert_eq!(
            error.to_string(),
            "native histogram schema 9 must be reduced before ingestion"
        );
        assert!(!error.to_string().contains("remote write"));
        assert!(!error.to_string().contains("OTLP"));
    }
}
