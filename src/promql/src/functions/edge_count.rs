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

use std::sync::Arc;

use datafusion::arrow::array::{Float64Array, TimestampMillisecondArray};
use datafusion::common::DataFusionError;
use datafusion::logical_expr::{ScalarUDF, Volatility};
use datafusion::physical_plan::ColumnarValue;
use datafusion_expr::create_udf;
use datatypes::arrow::array::Array;
use datatypes::arrow::datatypes::DataType;

use crate::functions::extract_range_array;
use crate::range_array::RangeArray;

#[derive(Clone, Copy)]
pub(super) enum EdgeKind {
    Changes,
    Resets,
}

pub(super) fn scalar_udf(name: &'static str, kind: EdgeKind) -> ScalarUDF {
    create_udf(
        name,
        input_type(),
        DataType::Float64,
        Volatility::Volatile,
        Arc::new(
            move |input: &[ColumnarValue]| -> Result<ColumnarValue, DataFusionError> {
                calc(input, name, kind)
            },
        ) as _,
    )
}

fn input_type() -> Vec<DataType> {
    vec![
        RangeArray::convert_data_type(TimestampMillisecondArray::new_null(0).data_type().clone()),
        RangeArray::convert_data_type(Float64Array::new_null(0).data_type().clone()),
    ]
}

fn calc(
    input: &[ColumnarValue],
    name: &str,
    kind: EdgeKind,
) -> Result<ColumnarValue, DataFusionError> {
    assert_eq!(input.len(), 2);

    let timestamp_ranges = extract_range_array(&input[0])?;
    let value_ranges = extract_range_array(&input[1])?;
    if timestamp_ranges.len() != value_ranges.len() {
        return Err(DataFusionError::Execution(format!(
            "RangeArray have different lengths in PromQL function {name}: array1={}, array2={}",
            timestamp_ranges.len(),
            value_ranges.len()
        )));
    }

    timestamp_ranges
        .values()
        .as_any()
        .downcast_ref::<TimestampMillisecondArray>()
        .unwrap();
    let values = value_ranges
        .values()
        .as_any()
        .downcast_ref::<Float64Array>()
        .unwrap();
    let requested_edges = validate_windows(&timestamp_ranges, &value_ranges, name)?;
    let raw_values = values.values();
    // A NULL field value means the series has no sample at that timestamp. The prefix sums
    // encode edges between physically adjacent slots, which no longer holds once nulls are
    // skipped, so a null-bearing input falls back to scanning each window.
    let has_nulls = values.null_count() > 0;
    let direct = has_nulls || should_scan_direct(requested_edges, raw_values.len());
    let prefix = (!direct).then(|| build_prefix(raw_values.as_ref(), kind));

    let mut result = Vec::with_capacity(value_ranges.len());
    for index in 0..value_ranges.len() {
        let (offset, len) = range_at(&value_ranges, index, name)?;
        let end = checked_end(offset, len, index, name)?;
        let count = match len {
            0 => None,
            _ if has_nulls => count_edges_skipping_nulls(values, offset, end, kind),
            1 => Some(0),
            _ if direct => Some(count_edges(raw_values.as_ref(), offset, end, kind)),
            _ => {
                let prefix = prefix.as_ref().unwrap();
                Some(prefix[end - 1] - prefix[offset])
            }
        };
        result.push(count.map(|count| count as f64));
    }

    Ok(ColumnarValue::Array(Arc::new(Float64Array::from_iter(
        result,
    ))))
}

fn validate_windows(
    timestamps: &RangeArray,
    values: &RangeArray,
    name: &str,
) -> Result<usize, DataFusionError> {
    let mut requested_edges = 0usize;
    for index in 0..values.len() {
        let (timestamp_offset, timestamp_len) = range_at(timestamps, index, name)?;
        let (value_offset, value_len) = range_at(values, index, name)?;
        if timestamp_len != value_len {
            return Err(DataFusionError::Execution(format!(
                "RangeArray's element {index} have different lengths in PromQL function {name}: array1={timestamp_len}, array2={value_len}"
            )));
        }
        checked_end(timestamp_offset, timestamp_len, index, name)?;
        checked_end(value_offset, value_len, index, name)?;
        requested_edges = requested_edges.saturating_add(value_len.saturating_sub(1));
    }
    Ok(requested_edges)
}

fn range_at(
    ranges: &RangeArray,
    index: usize,
    name: &str,
) -> Result<(usize, usize), DataFusionError> {
    ranges.get_offset_length(index).ok_or_else(|| {
        DataFusionError::Execution(format!(
            "RangeArray's element {index} is unavailable in PromQL function {name}"
        ))
    })
}

fn checked_end(
    offset: usize,
    len: usize,
    index: usize,
    name: &str,
) -> Result<usize, DataFusionError> {
    offset.checked_add(len).ok_or_else(|| {
        DataFusionError::Execution(format!(
            "RangeArray's element {index} has an invalid range in PromQL function {name}"
        ))
    })
}

fn should_scan_direct(requested_edges: usize, backing_len: usize) -> bool {
    requested_edges <= backing_len.saturating_sub(1)
}

fn build_prefix(values: &[f64], kind: EdgeKind) -> Vec<u64> {
    let mut prefix = Vec::with_capacity(values.len());
    prefix.push(0);
    for index in 1..values.len() {
        prefix.push(prefix[index - 1] + u64::from(is_edge(values[index - 1], values[index], kind)));
    }
    prefix
}

fn count_edges(values: &[f64], offset: usize, end: usize, kind: EdgeKind) -> u64 {
    let mut count = 0;
    for index in offset + 1..end {
        count += u64::from(is_edge(values[index - 1], values[index], kind));
    }
    count
}

/// Counts edges between consecutive samples in `[offset, end)`, treating null slots as
/// absent. Returns `None` when the window holds no sample.
fn count_edges_skipping_nulls(
    values: &Float64Array,
    offset: usize,
    end: usize,
    kind: EdgeKind,
) -> Option<u64> {
    let raw_values = values.values();
    let mut count = 0;
    let mut previous = None;
    for index in offset..end {
        if values.is_null(index) {
            continue;
        }
        let current = raw_values[index];
        if let Some(previous) = previous.replace(current) {
            count += u64::from(is_edge(previous, current, kind));
        }
    }
    previous.is_some().then_some(count)
}

fn is_edge(previous: f64, current: f64, kind: EdgeKind) -> bool {
    match kind {
        EdgeKind::Changes => previous != current && !(previous.is_nan() && current.is_nan()),
        EdgeKind::Resets => current < previous,
    }
}

#[cfg(test)]
mod test {
    use super::*;

    #[test]
    fn adaptive_gate_uses_direct_scan_at_threshold() {
        assert!(should_scan_direct(4, 5));
        assert!(!should_scan_direct(5, 5));
        assert!(should_scan_direct(0, 0));
    }
}
