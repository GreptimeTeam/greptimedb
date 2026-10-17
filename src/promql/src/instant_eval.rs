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

use std::ops::RangeInclusive;

use common_query::prometheus::is_prometheus_stale_nan;
use datafusion::error::{DataFusionError, Result};
use datatypes::series::raw::RawTimestamps;
use datatypes::series::samples::{SampleRef, SeriesFields};

#[cfg(test)]
mod tests;

pub(crate) const MAX_INSTANT_MANIPULATE_OUTPUT_POINTS: usize = 1_000_000;

/// Selects source rows for an aligned PromQL instant-evaluation range.
///
/// Returns original logical row indices and evaluation timestamps from a complete source axis
/// of strictly increasing milliseconds. Any provided field's stale marker suppresses selection;
/// missing samples are not stale and remain selectable.
pub fn select_instants(
    time: &RawTimestamps,
    fields: &SeriesFields,
    range: RangeInclusive<i64>,
    step_ms: i64,
    lookback_ms: i64,
    offset_ms: i64,
) -> Result<(Vec<u64>, Vec<i64>)> {
    let start = *range.start();
    let end = *range.end();
    if step_ms <= 0 {
        return Err(DataFusionError::Execution(
            "InstantManipulate interval must be positive".into(),
        ));
    }
    if start > end {
        return Err(DataFusionError::Execution(
            "InstantManipulate start must not exceed end".into(),
        ));
    }
    if lookback_ms < 0 {
        return Err(DataFusionError::Execution(
            "InstantManipulate lookback must be non-negative".into(),
        ));
    }
    let timestamps = time.as_buffer();
    if timestamps.windows(2).any(|pair| pair[0] >= pair[1]) {
        return Err(DataFusionError::Execution(
            "raw timestamps must be strictly increasing".into(),
        ));
    }
    if fields
        .as_slice()
        .iter()
        .any(|field| field.len() != timestamps.len())
    {
        return Err(DataFusionError::Execution(
            "series fields and time axis have unequal lengths".into(),
        ));
    }
    if timestamps.is_empty() {
        return Ok((Vec::new(), Vec::new()));
    }

    // Match the native-tick selector's bounds calculation in wide arithmetic before scanning.
    let first = timestamps[0] as i128 + offset_ms as i128;
    let last = timestamps[timestamps.len() - 1] as i128 + offset_ms as i128;
    let useful_last = if lookback_ms == 0 {
        last
    } else {
        last + lookback_ms as i128 - 1
    };
    let query_start = start as i128;
    let query_end = end as i128;
    let step = step_ms as i128;
    let max_start = first.max(query_start);
    let min_end = useful_last.min(query_end);
    let (aligned_start, aligned_end) = if max_start > min_end {
        (1_i128, 0_i128)
    } else {
        (
            query_start + (max_start - query_start) / step * step,
            query_end - (query_end - min_end) / step * step,
        )
    };
    let estimated_points = if aligned_end >= aligned_start {
        (aligned_end - aligned_start) / step + 1
    } else {
        0
    };
    if estimated_points > MAX_INSTANT_MANIPULATE_OUTPUT_POINTS as i128 {
        return Err(DataFusionError::Execution(format!(
            "InstantManipulate output points exceed limit: {estimated_points} > {MAX_INSTANT_MANIPULATE_OUTPUT_POINTS}"
        )));
    }
    let mut indices = Vec::with_capacity(estimated_points as usize);
    let mut evaluation_timestamps = Vec::with_capacity(estimated_points as usize);
    let mut sample_iters: Vec<_> = fields.iter().map(|field| field.iter()).collect();
    let mut current_stale = false;
    let mut cursor = 0usize;
    for expected in (aligned_start..=aligned_end).step_by(step_ms as usize) {
        let source_expected = expected - offset_ms as i128;
        while cursor < timestamps.len() && timestamps[cursor] as i128 <= source_expected {
            current_stale = false;
            for samples in &mut sample_iters {
                let sample = samples.next().ok_or_else(|| {
                    DataFusionError::Execution(
                        "series sample iterator ended before the time axis".into(),
                    )
                })?;
                let stale = match sample {
                    SampleRef::Float(value) => is_prometheus_stale_nan(value),
                    SampleRef::Histogram(histogram) => is_prometheus_stale_nan(histogram.sum),
                    SampleRef::Missing => false,
                };
                current_stale |= stale;
            }
            cursor += 1;
        }
        let Some(candidate) = cursor.checked_sub(1) else {
            continue;
        };
        let candidate_ts = timestamps[candidate] as i128 + offset_ms as i128;
        let lower = expected - lookback_ms as i128;
        if (candidate_ts == expected || candidate_ts > lower)
            && candidate_ts <= expected
            && !current_stale
        {
            indices.push(candidate as u64);
            evaluation_timestamps.push(expected as i64);
        }
    }
    Ok((indices, evaluation_timestamps))
}
