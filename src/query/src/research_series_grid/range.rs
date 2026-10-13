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

use datafusion::arrow::array::{Float64Array, Int64Array, TimestampMillisecondArray};
use datafusion::arrow::datatypes::{DataType, Field};
use datafusion_common::config::ConfigOptions;
use datafusion_common::{DataFusionError, Result};
use datafusion_expr::{ColumnarValue, ScalarFunctionArgs};
use promql::functions::{IDelta, Increase, MaxOverTime, Rate};
use promql::range_array::RangeArray;

use crate::research_series_grid::grid::TimeGrid;
use crate::research_series_grid::scan_range::RangeFunction;

/// Reduces disjoint windows while retaining only reset values from the current window.
pub(crate) fn evaluate_counter_blocks(
    function: RangeFunction,
    grid: &TimeGrid,
    window: i64,
    offset: i64,
    blocks: &mut dyn Iterator<
        Item = std::result::Result<
            store_api::region_engine::VisibleSeriesBlock,
            common_error::ext::BoxedError,
        >,
    >,
    cancelled: &std::sync::atomic::AtomicBool,
    memory: &datafusion::execution::memory_pool::MemoryReservation,
) -> Result<Vec<Option<f64>>> {
    use std::sync::atomic::Ordering;

    let invalid = |message: &str| DataFusionError::Execution(message.into());
    if matches!(function, RangeFunction::Max | RangeFunction::Instant)
        || window <= 0
        || grid.instants.is_empty()
        || grid
            .instants
            .windows(2)
            .any(|p| i128::from(p[1]) - i128::from(p[0]) < i128::from(window))
    {
        return Err(invalid("Direct rate requires disjoint positive windows"));
    }
    let mut result = Vec::with_capacity(grid.instants.len());
    let mut state = RateWindow::default();
    let mut previous = None;
    for block in blocks {
        if cancelled.load(Ordering::Relaxed) {
            return Err(invalid("Direct rate cancelled"));
        }
        let block = block.map_err(|error| DataFusionError::External(Box::new(error)))?;
        if block.values.null_count() != 0
            || block.timestamp_step < 0
            || (block.timestamp_step == 0 && block.values.len() > 1)
        {
            return Err(invalid("Invalid visible rate block"));
        }
        if let store_api::region_engine::VisibleSeriesValues::Integer(values) = &block.values
            && function != RangeFunction::Irate
            && values.step() >= 0
            && block.timestamp_step > 0
            && !block.values.is_empty()
        {
            let timestamp = |index: usize| {
                block
                    .timestamp_step
                    .checked_mul(index as i128)
                    .and_then(|delta| i128::from(block.first_timestamp).checked_add(delta))
                    .and_then(|value| i64::try_from(value).ok())
                    .ok_or_else(|| invalid("Visible rate timestamp overflow"))
            };
            let last = timestamp(block.values.len() - 1)?;
            if previous.is_some_and(|previous| block.first_timestamp <= previous) {
                return Err(invalid("Visible rate blocks are not strictly ordered"));
            }
            previous = Some(last);
            let first = block
                .first_timestamp
                .checked_add(offset)
                .ok_or_else(|| invalid("Rate timestamp shift overflow"))?;
            let last_shifted = last
                .checked_add(offset)
                .ok_or_else(|| invalid("Rate timestamp shift overflow"))?;
            let shifted = |index: usize| i128::from(first) + index as i128 * block.timestamp_step;
            let after = |boundary: i128| {
                if boundary < i128::from(first) {
                    0
                } else if boundary >= i128::from(last_shifted) {
                    block.values.len()
                } else {
                    // Interior boundaries imply at least two samples. The checked
                    // i64 endpoints bound both the positive step and delta by u64::MAX.
                    let delta = (boundary - i128::from(first)) as u64;
                    (delta / block.timestamp_step as u64) as usize + 1
                }
            };
            let mut index = 0;
            while index < block.values.len() && result.len() < grid.instants.len() {
                if cancelled.load(Ordering::Relaxed) {
                    return Err(invalid("Direct rate cancelled"));
                }
                let end = grid.instants[result.len()];
                if shifted(index) > i128::from(end) {
                    result.push(state.finish(function, end, window)?);
                    continue;
                }
                index = index.max(after(i128::from(end) - i128::from(window)));
                let upper = after(i128::from(end));
                if index < upper {
                    state.push(shifted(index) as i64, values.value(index), function, memory)?;
                    // Nondecreasing exact values contain no internal counter reset.
                    // Keep the original last-first arithmetic and reset addition order.
                    state.last = (shifted(upper - 1) as i64, values.value(upper - 1));
                    state.count = state
                        .count
                        .checked_add(upper - index - 1)
                        .ok_or_else(|| invalid("Rate sample count overflow"))?;
                    index = upper;
                }
            }
            continue;
        }
        for (index, value) in block.values.iter_values().enumerate() {
            if index % 1024 == 0 && cancelled.load(Ordering::Relaxed) {
                return Err(invalid("Direct rate cancelled"));
            }
            let timestamp = block
                .timestamp_step
                .checked_mul(index as i128)
                .and_then(|delta| i128::from(block.first_timestamp).checked_add(delta))
                .and_then(|timestamp| i64::try_from(timestamp).ok())
                .ok_or_else(|| invalid("Visible rate timestamp overflow"))?;
            if previous.is_some_and(|previous| timestamp <= previous) {
                return Err(invalid("Visible rate blocks are not strictly ordered"));
            }
            previous = Some(timestamp);
            if value.to_bits() == 0x7ff0_0000_0000_0002 {
                continue;
            }
            let timestamp = timestamp
                .checked_add(offset)
                .ok_or_else(|| invalid("Rate timestamp shift overflow"))?;
            while result.len() < grid.instants.len() && timestamp > grid.instants[result.len()] {
                result.push(state.finish(function, grid.instants[result.len()], window)?);
            }
            if result.len() == grid.instants.len()
                || i128::from(timestamp)
                    <= i128::from(grid.instants[result.len()]) - i128::from(window)
            {
                continue;
            }
            state.push(timestamp, value, function, memory)?;
        }
    }
    while result.len() < grid.instants.len() {
        result.push(state.finish(function, grid.instants[result.len()], window)?);
    }
    Ok(result)
}

#[derive(Default)]
struct RateWindow {
    first: (i64, f64),
    last: (i64, f64),
    penultimate: (i64, f64),
    count: usize,
    resets: Vec<f64>,
}

impl RateWindow {
    fn push(
        &mut self,
        timestamp: i64,
        value: f64,
        function: RangeFunction,
        memory: &datafusion::execution::memory_pool::MemoryReservation,
    ) -> Result<()> {
        if self.count == 0 {
            self.first = (timestamp, value);
        } else if function != RangeFunction::Irate && value < self.last.1 {
            if self.resets.len() == self.resets.capacity() {
                let extra = self.resets.capacity().max(8);
                memory.try_grow(extra.checked_mul(8).ok_or_else(|| {
                    DataFusionError::Execution("Rate reset capacity overflow".into())
                })?)?;
                self.resets
                    .try_reserve_exact(extra)
                    .map_err(|error| DataFusionError::Execution(error.to_string()))?;
            }
            self.resets.push(self.last.1);
        }
        self.penultimate = self.last;
        self.last = (timestamp, value);
        self.count += 1;
        Ok(())
    }

    fn finish(&mut self, function: RangeFunction, end: i64, window: i64) -> Result<Option<f64>> {
        let count = std::mem::take(&mut self.count);
        if count < 2 {
            self.resets.clear();
            return Ok(None);
        }
        if function == RangeFunction::Irate {
            let interval = self
                .last
                .0
                .checked_sub(self.penultimate.0)
                .ok_or_else(|| DataFusionError::Execution("Irate interval overflow".into()))?;
            if interval == 0 {
                return Ok(None);
            }
            let value = if self.last.1 < self.penultimate.1 {
                self.last.1
            } else {
                self.last.1 - self.penultimate.1
            };
            return Ok(Some(value / (interval as f64 / 1000.0)));
        }
        // Reset additions must follow the first difference, in original sample order.
        // Summing resets separately changes f64 rounding.
        let value = self
            .resets
            .drain(..)
            .fold(self.last.1 - self.first.1, |value, reset| value + reset);
        let difference = |a: i64, b: i64| {
            a.checked_sub(b)
                .ok_or_else(|| DataFusionError::Execution("Rate interval overflow".into()))
        };
        let start = difference(end, window)?;
        let sampled = difference(self.last.0, self.first.0)? as f64;
        let average = sampled / (count - 1) as f64;
        let mut leading = difference(self.first.0, start)? as f64;
        let mut trailing = difference(end, self.last.0)? as f64;
        let threshold = average * 1.1;
        if leading >= threshold {
            leading = average / 2.0;
        }
        if value > 0.0 && self.first.1 >= 0.0 {
            let zero = sampled * (self.first.1 / value);
            if zero < leading {
                leading = zero;
            }
        }
        if trailing >= threshold {
            trailing = average / 2.0;
        }
        let mut factor = if sampled == 0.0 {
            1.0
        } else {
            (sampled + leading + trailing) / sampled
        };
        if function == RangeFunction::Rate {
            factor /= window as f64 / 1000.0;
        }
        Ok(Some(value * factor))
    }
}

/// Consumes visible blocks without assembling a full-series timestamp or value array.
pub(crate) fn evaluate_selection_blocks(
    function: RangeFunction,
    grid: &TimeGrid,
    window: i64,
    offset: i64,
    blocks: &mut dyn Iterator<
        Item = std::result::Result<
            store_api::region_engine::VisibleSeriesBlock,
            common_error::ext::BoxedError,
        >,
    >,
    cancelled: &std::sync::atomic::AtomicBool,
) -> Result<Vec<Option<f64>>> {
    use std::sync::atomic::Ordering;

    use crate::research_series_grid::block_eval::{BlockEvaluator, Mode};

    if matches!(function, RangeFunction::Instant) {
        return evaluate_instant_blocks(grid, window, offset, blocks, cancelled);
    }

    let invalid = |message: &str| DataFusionError::Execution(message.into());
    let mode = match function {
        RangeFunction::Max => Mode::DisjointMax,
        RangeFunction::Instant => Mode::Instant,
        _ => return Err(invalid("Expected instant or max selection")),
    };
    let mut evaluator =
        BlockEvaluator::new(&grid.instants, window, offset, mode).map_err(invalid)?;
    let mut previous = None;
    for block in blocks {
        if cancelled.load(Ordering::Relaxed) {
            return Err(invalid("Direct range evaluation cancelled"));
        }
        let block = block.map_err(|error| DataFusionError::External(Box::new(error)))?;
        if block.values.null_count() != 0
            || block.timestamp_step < 0
            || (block.timestamp_step == 0 && block.values.len() > 1)
        {
            return Err(invalid("Invalid visible range block"));
        }
        for (index, value) in block.values.iter_values().enumerate() {
            if index % 1024 == 0 && cancelled.load(Ordering::Relaxed) {
                return Err(invalid("Direct range evaluation cancelled"));
            }
            let timestamp = block
                .timestamp_step
                .checked_mul(index as i128)
                .and_then(|delta| i128::from(block.first_timestamp).checked_add(delta))
                .and_then(|timestamp| i64::try_from(timestamp).ok())
                .ok_or_else(|| invalid("Visible range timestamp overflow"))?;
            if previous.is_some_and(|previous| timestamp <= previous) {
                return Err(invalid("Visible range blocks are not strictly ordered"));
            }
            previous = Some(timestamp);
            evaluator.push(timestamp, value).map_err(invalid)?;
        }
    }
    Ok(evaluator.finish())
}

fn evaluate_instant_blocks(
    grid: &TimeGrid,
    window: i64,
    offset: i64,
    blocks: &mut dyn Iterator<
        Item = std::result::Result<
            store_api::region_engine::VisibleSeriesBlock,
            common_error::ext::BoxedError,
        >,
    >,
    cancelled: &std::sync::atomic::AtomicBool,
) -> Result<Vec<Option<f64>>> {
    use std::sync::atomic::Ordering;

    let invalid = |message: &str| DataFusionError::Execution(message.into());
    if window < 0
        || grid.instants.is_empty()
        || grid.instants.windows(2).any(|pair| pair[0] >= pair[1])
    {
        return Err(invalid("Invalid block evaluation grid"));
    }
    let select = |latest: Option<(i128, f64)>, end: i128| {
        latest.and_then(|(timestamp, value)| {
            ((timestamp == end || timestamp > end - i128::from(window))
                && value.to_bits() != 0x7ff0_0000_0000_0002)
                .then_some(value)
        })
    };
    let mut output = Vec::with_capacity(grid.instants.len());
    let mut previous = None;
    let mut latest = None;
    for block in blocks {
        if cancelled.load(Ordering::Relaxed) {
            return Err(invalid("Direct range evaluation cancelled"));
        }
        let block = block.map_err(|error| DataFusionError::External(Box::new(error)))?;
        if block.values.null_count() != 0
            || block.timestamp_step < 0
            || (block.timestamp_step == 0 && block.values.len() > 1)
        {
            return Err(invalid("Invalid visible range block"));
        }
        if block.values.is_empty() {
            continue;
        }
        let last = block
            .timestamp_step
            .checked_mul((block.values.len() - 1) as i128)
            .and_then(|delta| i128::from(block.first_timestamp).checked_add(delta))
            .and_then(|timestamp| i64::try_from(timestamp).ok())
            .ok_or_else(|| invalid("Visible range timestamp overflow"))?;
        if previous.is_some_and(|previous| block.first_timestamp <= previous) {
            return Err(invalid("Visible range blocks are not strictly ordered"));
        }
        previous = Some(last);
        let first = i128::from(block.first_timestamp) + i128::from(offset);
        let last = i128::from(last) + i128::from(offset);
        while let Some(&end) = grid.instants.get(output.len()) {
            let end = i128::from(end);
            if end > last {
                break;
            }
            if output.len() % 1024 == 0 && cancelled.load(Ordering::Relaxed) {
                return Err(invalid("Direct range evaluation cancelled"));
            }
            if end >= first {
                let index = if block.timestamp_step == 0 {
                    0
                } else {
                    usize::try_from((end - first) / block.timestamp_step)
                        .map_err(|_| invalid("Direct series index overflow"))?
                };
                latest = Some((
                    first + block.timestamp_step * index as i128,
                    block.values.value(index),
                ));
            }
            output.push(select(latest, end));
        }
        latest = Some((last, block.values.value(block.values.len() - 1)));
    }
    for &end in &grid.instants[output.len()..] {
        if output.len() % 1024 == 0 && cancelled.load(Ordering::Relaxed) {
            return Err(invalid("Direct range evaluation cancelled"));
        }
        output.push(select(latest, i128::from(end)));
    }
    Ok(output)
}

/// Evaluates all windows for one series with the existing GT range UDF.
pub(crate) fn evaluate(
    name: &str,
    grid: &TimeGrid,
    timestamps: Vec<i64>,
    values: Vec<f64>,
    window: i64,
    offset: i64,
) -> Result<Vec<Option<f64>>> {
    if name == "instant" {
        return grid
            .instant_indices(&timestamps, &values, window, offset)
            .map(|indices| {
                indices
                    .into_iter()
                    .map(|index| index.map(|index| values[index]))
                    .collect()
            })
            .map_err(|message| DataFusionError::Execution(message.into()));
    }
    let udf = match name {
        "rate" => Rate::scalar_udf(),
        "increase" => Increase::scalar_udf(),
        "irate" => IDelta::<true>::scalar_udf(),
        "max_over_time" => MaxOverTime::scalar_udf(),
        _ => {
            return Err(DataFusionError::NotImplemented(format!(
                "Research range function: {name}"
            )));
        }
    };
    if timestamps.len() != values.len() {
        return Err(DataFusionError::Execution(
            "Mismatched source arrays".into(),
        ));
    }
    let mut times = Vec::with_capacity(timestamps.len());
    let mut samples = Vec::with_capacity(values.len());
    for (timestamp, value) in timestamps.into_iter().zip(values) {
        if value.to_bits() == 0x7ff0_0000_0000_0002 {
            continue;
        }
        times.push(timestamp.checked_add(offset).ok_or_else(|| {
            DataFusionError::Execution("Range UDF timestamp shift overflow".into())
        })?);
        samples.push(value);
    }
    let ranges = grid
        .ranges(&times, window, 0)
        .map_err(|e| DataFusionError::Execution(e.into()))?;
    let times = RangeArray::from_ranges(
        Arc::new(TimestampMillisecondArray::from(times)),
        ranges.clone(),
    )?;
    let samples = RangeArray::from_ranges(Arc::new(Float64Array::from(samples)), ranges)?;
    let mut args = vec![
        ColumnarValue::Array(Arc::new(times.into_dict())),
        ColumnarValue::Array(Arc::new(samples.into_dict())),
    ];
    if matches!(name, "rate" | "increase") {
        args.push(ColumnarValue::Array(Arc::new(
            TimestampMillisecondArray::from(grid.instants.clone()),
        )));
        args.push(ColumnarValue::Array(Arc::new(Int64Array::from(vec![
            window,
        ]))));
    }
    let arg_fields = args
        .iter()
        .enumerate()
        .map(|(index, arg)| Arc::new(Field::new(format!("arg{index}"), arg.data_type(), true)))
        .collect();
    let output = udf
        .invoke_with_args(ScalarFunctionArgs {
            args,
            arg_fields,
            number_rows: grid.instants.len(),
            return_field: Arc::new(Field::new("value", DataType::Float64, true)),
            config_options: Arc::new(ConfigOptions::default()),
        })?
        .into_array(grid.instants.len())?;
    let values = output
        .as_any()
        .downcast_ref::<Float64Array>()
        .ok_or_else(|| DataFusionError::Execution("Range UDF did not return Float64".into()))?;
    Ok(values.iter().collect())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::research_series_grid::block_eval::{BlockEvaluator, Mode};

    #[test]
    fn counter_blocks_match_udf_across_resets_gaps_offsets_and_block_edges() {
        use datafusion::execution::memory_pool::{GreedyMemoryPool, MemoryConsumer, MemoryPool};
        use store_api::region_engine::VisibleSeriesBlock;
        let pool: Arc<dyn MemoryPool> = Arc::new(GreedyMemoryPool::new(1 << 20));
        let cancelled = std::sync::atomic::AtomicBool::new(false);
        let special = [
            1e30,
            0.0,
            1.0,
            0.0,
            -0.0,
            0.0,
            f64::NAN,
            f64::INFINITY,
            f64::NEG_INFINITY,
            f64::from_bits(0x7ff0000000000002),
        ];
        let mut seed = 41u64;
        let values: Vec<_> = (0..240)
            .map(|i| {
                seed = seed.wrapping_mul(6364136223846793005).wrapping_add(1);
                if i % 3 == 0 {
                    special[i % special.len()]
                } else {
                    (seed >> 32) as f64
                }
            })
            .collect();
        let times: Vec<_> = (0..values.len()).map(|i| -500 + i as i64 * 7).collect();
        for function in [
            RangeFunction::Rate,
            RangeFunction::Increase,
            RangeFunction::Irate,
        ] {
            for width in [1, 14, 35, 300] {
                for stride in [width, width + 17] {
                    let grid = TimeGrid::new(-600, 1500, stride).unwrap();
                    for offset in [-23, 0, 41] {
                        let expected = evaluate(
                            function.name(),
                            &grid,
                            times.clone(),
                            values.clone(),
                            width,
                            offset,
                        )
                        .unwrap();
                        for block_size in [1, 3, 17, 240] {
                            let memory = MemoryConsumer::new("rate-block-test").register(&pool);
                            let mut blocks =
                                values.chunks(block_size).enumerate().map(|(i, values)| {
                                    Ok(VisibleSeriesBlock {
                                        first_timestamp: times[i * block_size],
                                        timestamp_step: 7,
                                        values: Float64Array::from(values.to_vec()).into(),
                                    })
                                });
                            let actual = evaluate_counter_blocks(
                                function,
                                &grid,
                                width,
                                offset,
                                &mut blocks,
                                &cancelled,
                                &memory,
                            )
                            .unwrap();
                            assert_eq!(actual.len(), expected.len());
                            for (actual, expected) in actual.into_iter().zip(&expected) {
                                match (actual, expected) {
                                    (Some(a), Some(b)) if a.is_nan() && b.is_nan() => {}
                                    (a, b) => assert_eq!(a.map(f64::to_bits), b.map(f64::to_bits)),
                                }
                            }
                        }
                    }
                }
            }
        }
        assert_eq!(pool.reserved(), 0);
    }

    #[test]
    fn native_values_match_udf_across_resets_gaps_and_slices() {
        use datafusion::execution::memory_pool::{GreedyMemoryPool, MemoryConsumer, MemoryPool};
        use store_api::region_engine::{VisibleSeriesBlock, VisibleSeriesValues};
        let pool: Arc<dyn MemoryPool> = Arc::new(GreedyMemoryPool::new(1 << 20));
        let cancelled = std::sync::atomic::AtomicBool::new(false);
        let times: Vec<_> = (0..180).map(|i| -1000 + i as i64 * 7).collect();
        let values: Vec<_> = (0..180)
            .map(|i| match i {
                0..60 => (i * 10) as f64,
                60..120 => ((1i64 << 53) - 1000 + i as i64) as f64,
                120..150 => (i - 120) as f64,
                _ => (180 - i) as f64,
            })
            .collect();
        for function in [
            RangeFunction::Rate,
            RangeFunction::Increase,
            RangeFunction::Irate,
        ] {
            for width in [1, 14, 35, 300] {
                for stride in [width, width + 17] {
                    let grid = TimeGrid::new(-1200, 500, stride).unwrap();
                    for offset in [-23, 0, 41] {
                        let expected = evaluate(
                            function.name(),
                            &grid,
                            times.clone(),
                            values.clone(),
                            width,
                            offset,
                        )
                        .unwrap();
                        for chunk in [4, 17, 60, 180] {
                            let memory = MemoryConsumer::new("native-rate-test").register(&pool);
                            let mut blocks = values.chunks(chunk).enumerate().map(|(i, values)| {
                                let compact = VisibleSeriesValues::compact(Float64Array::from(
                                    values.to_vec(),
                                ));
                                Ok(VisibleSeriesBlock {
                                    first_timestamp: times[i * chunk],
                                    timestamp_step: 7,
                                    values: compact.slice(0, compact.len()),
                                })
                            });
                            let actual = evaluate_counter_blocks(
                                function,
                                &grid,
                                width,
                                offset,
                                &mut blocks,
                                &cancelled,
                                &memory,
                            )
                            .unwrap();
                            assert_eq!(
                                actual
                                    .iter()
                                    .map(|v| v.map(f64::to_bits))
                                    .collect::<Vec<_>>(),
                                expected
                                    .iter()
                                    .map(|v| v.map(f64::to_bits))
                                    .collect::<Vec<_>>()
                            );
                        }
                    }
                }
            }
        }
        assert_eq!(pool.reserved(), 0);
    }

    #[test]
    fn native_rate_boundaries_cover_wide_timestamp_differences() {
        use datafusion::execution::memory_pool::{GreedyMemoryPool, MemoryConsumer, MemoryPool};
        use store_api::region_engine::{VisibleSeriesBlock, VisibleSeriesValues};

        let pool: Arc<dyn MemoryPool> = Arc::new(GreedyMemoryPool::new(1 << 20));
        let memory = MemoryConsumer::new("wide-rate-test").register(&pool);
        let cancelled = std::sync::atomic::AtomicBool::new(false);
        for (first, step, end, window, count) in [
            (i64::MIN, i128::from(i64::MAX / 3), 0, i64::MAX, 4),
            (i64::MAX - 30, 10, i64::MAX, 40, 4),
            (i64::MIN + 30, 10, i64::MIN + 80, 40, 4),
            (0, i128::MAX, 1, 2, 1),
        ] {
            let grid = TimeGrid::new(end, end, window).unwrap();
            let dense = Float64Array::from(vec![100.0, 110.0, 120.0, 130.0]);
            let native = VisibleSeriesValues::compact(dense.clone()).slice(0, count);
            assert!(matches!(native, VisibleSeriesValues::Integer(_)));
            let mut results = Vec::new();
            for values in [VisibleSeriesValues::Dense(dense.slice(0, count)), native] {
                let mut blocks = std::iter::once(Ok(VisibleSeriesBlock {
                    first_timestamp: first,
                    timestamp_step: step,
                    values,
                }));
                results.push(
                    evaluate_counter_blocks(
                        RangeFunction::Rate,
                        &grid,
                        window,
                        0,
                        &mut blocks,
                        &cancelled,
                        &memory,
                    )
                    .unwrap(),
                );
            }
            assert_eq!(
                results[0]
                    .iter()
                    .map(|v| v.map(f64::to_bits))
                    .collect::<Vec<_>>(),
                results[1]
                    .iter()
                    .map(|v| v.map(f64::to_bits))
                    .collect::<Vec<_>>(),
            );
        }
    }

    #[test]
    fn rate_blocks_reject_overlap_cancel_and_unbounded_reset_memory() {
        use datafusion::execution::memory_pool::{GreedyMemoryPool, MemoryConsumer, MemoryPool};
        use store_api::region_engine::VisibleSeriesBlock;
        let pool: Arc<dyn MemoryPool> = Arc::new(GreedyMemoryPool::new(1));
        let memory = MemoryConsumer::new("rate-reject-test").register(&pool);
        let cancelled = std::sync::atomic::AtomicBool::new(false);
        let make = || {
            vec![Ok(VisibleSeriesBlock {
                first_timestamp: 1,
                timestamp_step: 1,
                values: Float64Array::from(vec![2.0, 1.0]).into(),
            })]
            .into_iter()
        };
        let grid = TimeGrid::new(3, 3, 1).unwrap();
        assert!(
            evaluate_counter_blocks(
                RangeFunction::Rate,
                &grid,
                3,
                0,
                &mut make(),
                &cancelled,
                &memory
            )
            .is_err()
        );
        cancelled.store(true, std::sync::atomic::Ordering::Relaxed);
        assert!(
            evaluate_counter_blocks(
                RangeFunction::Rate,
                &grid,
                3,
                0,
                &mut make(),
                &cancelled,
                &memory
            )
            .is_err()
        );
        cancelled.store(false, std::sync::atomic::Ordering::Relaxed);
        let grid = TimeGrid::new(3, 6, 1).unwrap();
        assert!(
            evaluate_counter_blocks(
                RangeFunction::Rate,
                &grid,
                3,
                0,
                &mut make(),
                &cancelled,
                &memory
            )
            .is_err()
        );
    }

    #[test]
    fn instant_blocks_seek_across_gaps_stale_and_extreme_offsets() {
        use std::sync::atomic::AtomicBool;

        use store_api::region_engine::VisibleSeriesBlock;

        let segments = [
            (-30, vec![1.0, -0.0, 2.0]),
            (
                20,
                vec![3.0, f64::from_bits(0x7ff0_0000_0000_0002), f64::NAN],
            ),
            (60, vec![f64::INFINITY]),
        ];
        let timestamps: Vec<_> = segments
            .iter()
            .flat_map(|(first, values)| (0..values.len()).map(move |i| first + i as i64 * 10))
            .collect();
        let values: Vec<_> = segments
            .iter()
            .flat_map(|(_, values)| values.iter().copied())
            .collect();
        let bits = |values: Vec<Option<f64>>| {
            values
                .into_iter()
                .map(|value| value.map(f64::to_bits))
                .collect::<Vec<_>>()
        };
        for (start, end) in [
            (-50, 100),
            (i64::MIN, i64::MIN + 63),
            (i64::MAX - 63, i64::MAX),
        ] {
            let grid = TimeGrid::new(start, end, 7).unwrap();
            for window in [0, 5, 10, 30] {
                for offset in [-13, 0, 19, i64::MIN, i64::MAX] {
                    let expected = bits(
                        evaluate(
                            "instant",
                            &grid,
                            timestamps.clone(),
                            values.clone(),
                            window,
                            offset,
                        )
                        .unwrap(),
                    );
                    for size in [1, 2, 3] {
                        let mut blocks = segments.iter().flat_map(|(first, values)| {
                            values.chunks(size).enumerate().map(move |(chunk, values)| {
                                Ok(VisibleSeriesBlock {
                                    first_timestamp: first + (chunk * size) as i64 * 10,
                                    timestamp_step: if values.len() == 1 { 0 } else { 10 },
                                    values: Float64Array::from(values.to_vec()).into(),
                                })
                            })
                        });
                        assert_eq!(
                            bits(
                                evaluate_selection_blocks(
                                    RangeFunction::Instant,
                                    &grid,
                                    window,
                                    offset,
                                    &mut blocks,
                                    &AtomicBool::new(false)
                                )
                                .unwrap()
                            ),
                            expected
                        );
                    }
                }
            }
        }
    }

    #[test]
    fn visible_blocks_match_range_udf_across_boundaries() {
        use std::sync::atomic::AtomicBool;

        use store_api::region_engine::VisibleSeriesBlock;

        let times: Vec<_> = (0..83).map(|i| i * 7 - 100).collect();
        let special = [
            0.0,
            -0.0,
            f64::NAN,
            f64::INFINITY,
            f64::NEG_INFINITY,
            f64::from_bits(0x7ff0_0000_0000_0002),
            3.0,
        ];
        let values: Vec<_> = (0..times.len())
            .map(|i| special[i % special.len()])
            .collect();
        let bits = |values: Vec<Option<f64>>| {
            values
                .into_iter()
                .map(|value| value.map(f64::to_bits))
                .collect::<Vec<_>>()
        };
        for interval in [7, 30, 61] {
            let grid = TimeGrid::new(-120, 600, interval).unwrap();
            for window in [1, interval] {
                for offset in [-13, 0, 19] {
                    let expected = bits(
                        evaluate(
                            "max_over_time",
                            &grid,
                            times.clone(),
                            values.clone(),
                            window,
                            offset,
                        )
                        .unwrap(),
                    );
                    for chunk in [1, 3, 17, 83] {
                        let mut blocks =
                            times.chunks(chunk).zip(values.chunks(chunk)).map(|(t, v)| {
                                Ok(VisibleSeriesBlock {
                                    first_timestamp: t[0],
                                    timestamp_step: 7,
                                    values: Float64Array::from(v.to_vec()).into(),
                                })
                            });
                        assert_eq!(
                            bits(
                                evaluate_selection_blocks(
                                    RangeFunction::Max,
                                    &grid,
                                    window,
                                    offset,
                                    &mut blocks,
                                    &AtomicBool::new(false)
                                )
                                .unwrap()
                            ),
                            expected
                        );
                    }
                }
            }
        }
        let grid = TimeGrid::new(0, 10, 10).unwrap();
        let mut blocks = [Ok(VisibleSeriesBlock {
            first_timestamp: 0,
            timestamp_step: 1,
            values: Float64Array::from(vec![1.0]).into(),
        })]
        .into_iter();
        assert!(
            evaluate_selection_blocks(
                RangeFunction::Max,
                &grid,
                10,
                0,
                &mut blocks,
                &AtomicBool::new(true)
            )
            .unwrap_err()
            .to_string()
            .contains("cancelled")
        );
    }

    #[test]
    fn block_evaluation_matches_existing_selectors() {
        let timestamps = vec![-15, -10, -4, 0, 1, 9, 10, 20, 25];
        let values = vec![
            -0.0,
            0.0,
            f64::from_bits(0x7ff0000000000002),
            f64::from_bits(0x7ff8000000000001),
            f64::from_bits(0x7ff8000000000007),
            3.0,
            f64::INFINITY,
            f64::NEG_INFINITY,
            -0.0,
        ];
        let bits = |values: Vec<Option<f64>>| {
            values
                .into_iter()
                .map(|value| value.map(f64::to_bits))
                .collect::<Vec<_>>()
        };
        for step in [1, 5, 10, 19] {
            let grid = TimeGrid::new(-20, 60, step).unwrap();
            for window in [1, step] {
                for offset in [-3, 0, 4] {
                    for mode in [Mode::Instant, Mode::DisjointMax] {
                        let expected = match mode {
                            Mode::Instant => grid
                                .instant_indices(&timestamps, &values, window, offset)
                                .unwrap()
                                .into_iter()
                                .map(|index| index.map(|index| values[index]))
                                .collect(),
                            Mode::DisjointMax => evaluate(
                                "max_over_time",
                                &grid,
                                timestamps.clone(),
                                values.clone(),
                                window,
                                offset,
                            )
                            .unwrap(),
                        };
                        let mut evaluator =
                            BlockEvaluator::new(&grid.instants, window, offset, mode).unwrap();
                        for (&timestamp, &value) in timestamps.iter().zip(&values) {
                            evaluator.push(timestamp, value).unwrap();
                        }
                        assert_eq!(bits(evaluator.finish()), bits(expected));
                    }
                }
            }
        }
    }

    #[test]
    fn retains_gt_extrapolation_instead_of_vm_delta() {
        let grid = TimeGrid::new(40_000, 40_000, 1).unwrap();
        let times = vec![10_000, 20_000, 30_000];
        let values = vec![1000.0, 1010.0, 1020.0];
        assert_eq!(
            evaluate("increase", &grid, times.clone(), values.clone(), 40_000, 0).unwrap(),
            [Some(40.0)]
        );
        assert_eq!(
            evaluate("rate", &grid, times, values, 40_000, 0).unwrap(),
            [Some(1.0)]
        );
    }

    #[test]
    fn offset_shifts_sample_times_and_filters_stale_before_ranges() {
        let grid = TimeGrid::new(50_000, 50_000, 1).unwrap();
        assert_eq!(
            evaluate(
                "irate",
                &grid,
                vec![10_000, 20_000, 25_000, 30_000],
                vec![
                    1000.0,
                    1010.0,
                    f64::from_bits(0x7ff0_0000_0000_0002),
                    1020.0
                ],
                40_000,
                10_000
            )
            .unwrap(),
            [Some(1.0)]
        );
    }
}
