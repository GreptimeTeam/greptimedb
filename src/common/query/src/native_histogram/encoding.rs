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
use api::v1::value::ValueData;
use api::v1::{ColumnSchema, ListValue, SemanticType, Value};
use snafu::{Snafu, ensure};

use super::{
    CUSTOM_BUCKETS_SCHEMA, MAX_EXPONENTIAL_SCHEMA, NATIVE_HISTOGRAM_FIELD_NAMES,
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
        validate_float_native_histogram_counts(histogram)?;
        let count = match histogram.count.as_ref() {
            Some(Count::CountFloat(count)) => *count,
            _ => 0.0,
        };
        let zero_count = match histogram.zero_count.as_ref() {
            Some(ZeroCount::ZeroCountFloat(zero_count)) => *zero_count,
            _ => 0.0,
        };
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
        validate_integer_native_histogram_counts(histogram, &positive_buckets, &negative_buckets)?;
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

fn validate_native_histogram(histogram: &Histogram, uses_float_counts: bool) -> Result<()> {
    let exponential_overflow_index = validate_native_histogram_schema(histogram.schema)?;
    validate_native_histogram_custom_values(histogram)?;

    if histogram.schema == CUSTOM_BUCKETS_SCHEMA {
        ensure!(
            histogram.zero_threshold == 0.0 && native_histogram_zero_count_is_zero(histogram),
            NativeHistogramSnafu {
                message: "custom native histogram must not use a zero bucket"
            }
        );
        ensure!(
            histogram.negative_spans.is_empty()
                && histogram.negative_deltas.is_empty()
                && histogram.negative_counts.is_empty(),
            NativeHistogramSnafu {
                message: "custom native histogram must not use negative buckets"
            }
        );
    }

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
    let bucket_index_range = if let Some(overflow_index) = exponential_overflow_index {
        (i32::MIN, overflow_index)
    } else {
        (
            0,
            i32::try_from(histogram.custom_values.len()).map_err(|_| NativeHistogramError {
                message: "custom native histogram has too many custom_values".to_string(),
            })?,
        )
    };
    validate_native_histogram_spans(
        "positive",
        &histogram.positive_spans,
        positive_buckets,
        bucket_index_range,
    )?;
    validate_native_histogram_spans(
        "negative",
        &histogram.negative_spans,
        negative_buckets,
        bucket_index_range,
    )?;

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

fn validate_native_histogram_custom_values(histogram: &Histogram) -> Result<()> {
    if histogram.schema != CUSTOM_BUCKETS_SCHEMA {
        ensure!(
            histogram.custom_values.is_empty(),
            NativeHistogramSnafu {
                message: "standard native histogram must not use custom_values"
            }
        );
        return Ok(());
    }

    for value in &histogram.custom_values {
        ensure!(
            !value.is_nan() && *value != f64::INFINITY,
            NativeHistogramSnafu {
                message: "custom native histogram custom_values must not contain +Inf or NaN"
            }
        );
    }
    for values in histogram.custom_values.windows(2) {
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
    spans: &[BucketSpan],
    bucket_count: usize,
    bucket_index_range: (i32, i32),
) -> Result<()> {
    let span_len = spans.iter().try_fold(0usize, |sum, span| {
        let length = usize::try_from(span.length).map_err(|_| NativeHistogramError {
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
    for (span_index, span) in spans.iter().enumerate() {
        ensure!(
            span.offset >= 0 || (span_index == 0 && bucket_index_range.0 == i32::MIN),
            NativeHistogramSnafu {
                message: format!(
                    "native histogram {name} span {} has negative offset {}",
                    span_index + 1,
                    span.offset
                )
            }
        );
        current_index = if span_index == 0 {
            span.offset
        } else {
            current_index
                .checked_add(span.offset)
                .ok_or_else(|| NativeHistogramError {
                    message: format!("native histogram {name} span index overflows i32"),
                })?
        };

        for _ in 0..span.length {
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

    Ok(())
}

fn validate_float_native_histogram_counts(histogram: &Histogram) -> Result<()> {
    let count = match histogram.count.as_ref() {
        Some(Count::CountFloat(count)) => *count,
        _ => 0.0,
    };
    ensure!(
        count >= 0.0 || count.is_nan(),
        NativeHistogramSnafu {
            message: "native histogram float count must not be negative"
        }
    );

    let zero_count = match histogram.zero_count.as_ref() {
        Some(ZeroCount::ZeroCountFloat(zero_count)) => *zero_count,
        _ => 0.0,
    };
    ensure!(
        zero_count >= 0.0 || zero_count.is_nan(),
        NativeHistogramSnafu {
            message: "native histogram float zero_count must not be negative"
        }
    );

    for (name, counts) in [
        ("positive", &histogram.positive_counts),
        ("negative", &histogram.negative_counts),
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
    histogram: &Histogram,
    positive_buckets: &[i64],
    negative_buckets: &[i64],
) -> Result<()> {
    let count = match histogram.count.as_ref() {
        Some(Count::CountInt(count)) => *count,
        _ => 0,
    };
    let zero_count = match histogram.zero_count.as_ref() {
        Some(ZeroCount::ZeroCountInt(zero_count)) => *zero_count,
        _ => 0,
    };
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
        if histogram.sum.is_nan() {
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
