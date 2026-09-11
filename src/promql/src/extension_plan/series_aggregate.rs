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

mod final_merge;
#[cfg(test)]
mod tests;

use std::any::Any;
use std::hash::BuildHasher;
use std::mem::size_of;
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};
use std::time::Instant;

use common_telemetry::debug;
use datafusion::arrow::array::{
    Array, ArrayRef, BooleanArray, DictionaryArray, Float64Array, StringArray, StringViewArray,
    TimestampMillisecondArray, UInt16Array, UInt32Array, UInt64Array,
};
use datafusion::arrow::compute::{cast, filter_record_batch, take};
use datafusion::arrow::datatypes::{
    ArrowDictionaryKeyType, DataType, SchemaRef, TimeUnit, UInt16Type, UInt32Type,
};
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::common::{HashMap, Result, ScalarValue, exec_err};
use datafusion::execution::TaskContext;
use datafusion::execution::memory_pool::{MemoryConsumer, MemoryReservation};
use datafusion::functions_aggregate::average::Avg;
use datafusion::functions_aggregate::sum::Sum;
use datafusion::logical_expr::{EmitTo, GroupsAccumulator};
use datafusion::physical_expr::aggregate::AggregateFunctionExpr;
use datafusion::physical_expr::expressions::{Column, IsNotNullExpr, Literal};
use datafusion::physical_expr::{PhysicalExpr, ScalarFunctionExpr};
use datafusion::physical_plan::aggregates::{AggregateExec, AggregateMode, PhysicalGroupBy};
use datafusion::physical_plan::filter::FilterExec;
use datafusion::physical_plan::metrics::{BaselineMetrics, ExecutionPlanMetricsSet, MetricsSet};
use datafusion::physical_plan::projection::ProjectionExec;
use datafusion::physical_plan::{
    DisplayAs, DisplayFormatType, ExecutionPlan, InputOrderMode, PlanProperties, RecordBatchStream,
    SendableRecordBatchStream,
};
pub use final_merge::SeriesFinalAggregateExec;
use futures::{Stream, StreamExt, ready};

use crate::extension_plan::normalize::SeriesNormalizeExec;
use crate::extension_plan::range_manipulate::{
    FusedRateSource, RangeManipulateExec, calculate_ranges,
};
use crate::extension_plan::series_divide::SeriesDivideExec;
use crate::functions::{Rate, calculate_rate};

const UNSEEN: usize = usize::MAX;

/// The evaluation timestamps a PromQL range query asks for.
#[derive(Debug, Clone, Copy)]
struct Grid {
    start: i64,
    end: i64,
    step: i64,
    len: usize,
}

impl Grid {
    fn new(start: i64, end: i64, step: i64) -> Option<Self> {
        if step <= 0 || end < start {
            return None;
        }
        let len =
            usize::try_from(end.checked_sub(start)?.checked_div(step)?.checked_add(1)?).ok()?;
        Some(Self {
            start,
            end,
            step,
            len,
        })
    }

    fn slot(&self, timestamp: i64) -> Result<usize> {
        if timestamp < self.start || timestamp > self.end {
            return exec_err!("series aggregate timestamp outside the evaluation grid");
        }
        let offset = timestamp - self.start;
        if offset % self.step != 0 {
            return exec_err!("series aggregate timestamp outside the evaluation grid");
        }
        Ok((offset / self.step) as usize)
    }
}

/// Group ids of one label group, keyed by grid slot.
enum TimeSlots {
    Sparse(HashMap<usize, usize>),
    Dense(Vec<usize>),
}

impl TimeSlots {
    fn size(&self) -> usize {
        match self {
            Self::Sparse(slots) => slots.capacity() * (size_of::<(usize, usize)>() + 1) * 2,
            Self::Dense(slots) => slots.capacity() * size_of::<usize>(),
        }
    }

    fn intern(&mut self, slot: usize, next: usize, grid_len: usize) -> usize {
        let id = match self {
            Self::Sparse(slots) => *slots.entry(slot).or_insert(next),
            Self::Dense(slots) => {
                if slots[slot] == UNSEEN {
                    slots[slot] = next;
                }
                slots[slot]
            }
        };
        // A sparse query must not allocate the full requested time grid per label group.
        if matches!(self, Self::Sparse(_)) && grid_len <= self.size() / size_of::<usize>() {
            let mut dense = vec![UNSEEN; grid_len];
            if let Self::Sparse(slots) = self {
                for (slot, id) in slots.iter() {
                    dense[*slot] = *id;
                }
            }
            *self = Self::Dense(dense);
        }
        id
    }
}

/// Group state addressed as (label group, grid slot) instead of by hashing every row.
#[derive(Default)]
struct SeriesGroups {
    labels: HashMap<Vec<Option<String>>, usize>,
    slots: Vec<TimeSlots>,
    keys: Vec<(usize, i64)>,
}

impl SeriesGroups {
    fn size(&self) -> usize {
        self.labels.capacity() * (size_of::<(Vec<Option<String>>, usize)>() + 1) * 2
            + self
                .labels
                .keys()
                .map(|labels| {
                    labels.capacity() * size_of::<Option<String>>()
                        + labels.iter().flatten().map(String::capacity).sum::<usize>()
                })
                .sum::<usize>()
            + self.slots.capacity() * size_of::<TimeSlots>()
            + self.slots.iter().map(TimeSlots::size).sum::<usize>()
            + self.keys.capacity() * size_of::<(usize, i64)>()
    }

    fn intern(
        &mut self,
        batch: &RecordBatch,
        columns: &[usize],
        time_group: usize,
        grid: Grid,
        ids: &mut Vec<usize>,
    ) -> Result<()> {
        if batch.num_rows() == 0 {
            ids.clear();
            return Ok(());
        }
        let timestamps = batch
            .column(columns[time_group])
            .as_any()
            .downcast_ref::<TimestampMillisecondArray>()
            .ok_or_else(|| {
                datafusion::error::DataFusionError::Execution(
                    "series aggregate requires millisecond timestamps".to_string(),
                )
            })?;
        if timestamps.null_count() != 0 {
            return exec_err!("series aggregate received a null evaluation timestamp");
        }
        let labels = series_labels(batch, columns, time_group, true)?;
        self.intern_timestamps(&labels, grid, timestamps.values().iter().copied(), ids)
    }

    /// Locates the label group once for the whole batch, then one group id per timestamp.
    fn intern_timestamps(
        &mut self,
        labels: &[Option<&str>],
        grid: Grid,
        timestamps: impl Iterator<Item = i64>,
        ids: &mut Vec<usize>,
    ) -> Result<()> {
        ids.clear();
        let hash = self.labels.hasher().hash_one(labels);
        let next_label = self.labels.len();
        let label = *self
            .labels
            .raw_entry_mut()
            .from_hash(hash, |owned| {
                owned.len() == labels.len()
                    && owned.iter().zip(labels).all(|(a, b)| a.as_deref() == *b)
            })
            .or_insert_with(|| {
                (
                    labels.iter().map(|s| s.map(str::to_owned)).collect(),
                    next_label,
                )
            })
            .1;
        if label == self.slots.len() {
            self.slots.push(TimeSlots::Sparse(HashMap::default()));
        }
        for timestamp in timestamps {
            let slot = grid.slot(timestamp)?;
            let next = self.keys.len();
            let id = self.slots[label].intern(slot, next, grid.len);
            if id == next {
                self.keys.push((label, timestamp));
            }
            ids.push(id);
        }
        Ok(())
    }

    fn into_output(self, state: Vec<ArrayRef>, label_count: usize) -> PendingOutput {
        let mut values = vec![vec![None; self.labels.len()]; label_count];
        for (labels, id) in &self.labels {
            for (column, label) in labels.iter().enumerate() {
                values[column][*id] = label.as_deref();
            }
        }
        let labels = values
            .into_iter()
            .map(|values| Arc::new(StringViewArray::from(values)) as ArrayRef)
            .collect();
        PendingOutput {
            labels,
            keys: self.keys,
            state,
            offset: 0,
        }
    }
}

/// Reads the one label value each group column holds for a whole series batch.
///
/// `validate` rejects a batch that turns out to span more than one series; a caller
/// that already took the batch from a proven series boundary reads the first row.
fn series_labels<'a>(
    batch: &'a RecordBatch,
    columns: &[usize],
    time_group: usize,
    validate: bool,
) -> Result<Vec<Option<&'a str>>> {
    columns
        .iter()
        .enumerate()
        .filter(|(group, _)| *group != time_group)
        .map(|(_, index)| series_label(batch.column(*index).as_ref(), validate))
        .collect()
}

fn string_at(array: &dyn Array, row: usize) -> Result<Option<&str>> {
    if array.is_null(row) {
        return Ok(None);
    }
    if let Some(values) = array.as_any().downcast_ref::<StringViewArray>() {
        return Ok(Some(values.value(row)));
    }
    if let Some(values) = array.as_any().downcast_ref::<StringArray>() {
        return Ok(Some(values.value(row)));
    }
    exec_err!(
        "unsupported series aggregate label type: {}",
        array.data_type()
    )
}

fn dictionary_label<K: ArrowDictionaryKeyType>(
    array: &dyn Array,
    validate: bool,
) -> Result<Option<&str>> {
    let array = array
        .as_any()
        .downcast_ref::<DictionaryArray<K>>()
        .ok_or_else(|| {
            datafusion::error::DataFusionError::Execution(
                "invalid series label dictionary".to_string(),
            )
        })?;
    // Comparing keys is enough: two rows of one batch share a dictionary, so equal
    // keys mean equal values.
    if validate {
        let first = array.keys().iter().next().flatten();
        if !array.keys().iter().all(|key| key == first) {
            return exec_err!("mixed series labels in one aggregate input batch");
        }
    }
    match array.key(0) {
        Some(key) => string_at(array.values().as_ref(), key),
        None => Ok(None),
    }
}

/// The single label value `array` holds, or an error when `validate` is set and it
/// turns out to hold more than one.
fn series_label(array: &dyn Array, validate: bool) -> Result<Option<&str>> {
    match array.data_type() {
        DataType::Dictionary(key, _) if key.as_ref() == &DataType::UInt16 => {
            dictionary_label::<UInt16Type>(array, validate)
        }
        DataType::Dictionary(key, _) if key.as_ref() == &DataType::UInt32 => {
            dictionary_label::<UInt32Type>(array, validate)
        }
        _ => {
            let value = string_at(array, 0)?;
            if validate {
                for row in 1..array.len() {
                    if string_at(array, row)? != value {
                        return exec_err!("mixed series labels in one aggregate input batch");
                    }
                }
            }
            Ok(value)
        }
    }
}

fn supported_label(data_type: &DataType) -> bool {
    match data_type {
        DataType::Utf8 | DataType::Utf8View => true,
        DataType::Dictionary(key, value) => {
            matches!(key.as_ref(), DataType::UInt16 | DataType::UInt32)
                && matches!(value.as_ref(), DataType::Utf8 | DataType::Utf8View)
        }
        _ => false,
    }
}

/// Aggregate state kept as label groups and grid slots, expanded to output rows on demand.
struct PendingOutput {
    labels: Vec<ArrayRef>,
    keys: Vec<(usize, i64)>,
    state: Vec<ArrayRef>,
    offset: usize,
}

impl PendingOutput {
    fn size(&self) -> usize {
        self.keys.capacity() * size_of::<(usize, i64)>()
            + (self.labels.capacity() + self.state.capacity()) * size_of::<ArrayRef>()
            + self
                .labels
                .iter()
                .chain(&self.state)
                .map(|array| array.get_array_memory_size())
                .sum::<usize>()
    }

    fn next(
        &mut self,
        schema: &SchemaRef,
        time_group: usize,
        batch_size: usize,
    ) -> Result<Option<RecordBatch>> {
        if self.offset == self.keys.len() {
            return Ok(None);
        }
        let dictionary_limit = if schema.fields().iter().any(|field| {
            matches!(field.data_type(), DataType::Dictionary(key, _) if key.as_ref() == &DataType::UInt16)
        }) { u16::MAX as usize + 1 } else { u32::MAX as usize };
        let len = batch_size
            .min(dictionary_limit)
            .min(self.keys.len() - self.offset);
        let keys = &self.keys[self.offset..self.offset + len];
        let mut dictionary = HashMap::<usize, u32>::default();
        let mut unique = Vec::new();
        let mut last = None;
        let indices = UInt32Array::from_iter_values(keys.iter().map(|(label, _)| {
            if let Some((previous, index)) = last
                && previous == *label
            {
                return index;
            }
            let index = *dictionary.entry(*label).or_insert_with(|| {
                let index = unique.len() as u32;
                unique.push(*label as u64);
                index
            });
            last = Some((*label, index));
            index
        }));
        let unique = UInt64Array::from(unique);
        let mut columns = Vec::with_capacity(schema.fields().len());
        let mut label = 0;
        for group in 0..=self.labels.len() {
            if group == time_group {
                columns.push(Arc::new(TimestampMillisecondArray::from_iter_values(
                    keys.iter().map(|(_, timestamp)| *timestamp),
                )) as ArrayRef);
            } else {
                let values = take(self.labels[label].as_ref(), &unique, None)?;
                let array: ArrayRef = match schema.field(group).data_type() {
                    DataType::Dictionary(key, value) => {
                        let values = cast(&values, value)?;
                        // Preserve logical null labels as null keys, independently of the values bitmap.
                        if key.as_ref() == &DataType::UInt16 {
                            let keys =
                                UInt16Array::from_iter(indices.values().iter().map(|index| {
                                    (!values.is_null(*index as usize)).then_some(*index as u16)
                                }));
                            Arc::new(DictionaryArray::<UInt16Type>::try_new(keys, values)?)
                        } else {
                            let keys =
                                UInt32Array::from_iter(indices.values().iter().map(|index| {
                                    (!values.is_null(*index as usize)).then_some(*index)
                                }));
                            Arc::new(DictionaryArray::<UInt32Type>::try_new(keys, values)?)
                        }
                    }
                    data_type => cast(&take(values.as_ref(), &indices, None)?, data_type)?,
                };
                columns.push(array);
                label += 1;
            }
        }
        columns.extend(self.state.iter().map(|array| array.slice(self.offset, len)));
        self.offset += len;
        Ok(Some(RecordBatch::try_new(schema.clone(), columns)?))
    }
}

/// Partial float aggregation for batches with constant labels on a known evaluation grid.
/// The optimizer must prove the series-batch origin before constructing this plan.
#[derive(Debug)]
pub struct SeriesAggregateExec {
    input: Arc<dyn ExecutionPlan>,
    input_mode: SeriesAggregateInput,
    aggregates: Vec<Arc<AggregateFunctionExpr>>,
    group_by: PhysicalGroupBy,
    columns: Vec<usize>,
    time_group: usize,
    grid: Grid,
    properties: Arc<PlanProperties>,
    metrics: ExecutionPlanMetricsSet,
}

#[derive(Debug, Clone)]
enum SeriesAggregateInput {
    /// Group the rows the original filter and projection already produced.
    Materialized {
        predicate: Arc<dyn PhysicalExpr>,
        arguments: Vec<Vec<Arc<dyn PhysicalExpr>>>,
    },
    /// Compute `prom_rate` from the raw samples, skipping the range fold entirely.
    FusedRate(FusedRateInput),
}

#[derive(Debug, Clone)]
struct FusedRateInput {
    group_columns: Vec<usize>,
    time_index: usize,
    value_index: usize,
    /// Lookback window of the range selector. The evaluation grid is the aggregate's
    /// own, which `fused_rate_source` has already checked they agree on.
    range: i64,
}

impl FusedRateInput {
    fn try_new(
        filter: &FilterExec,
        aggregates: &[Arc<AggregateFunctionExpr>],
        group_columns: &[usize],
        time_group: usize,
        grid: Grid,
    ) -> Option<(Self, Arc<dyn ExecutionPlan>)> {
        let projection = filter.input().as_any().downcast_ref::<ProjectionExec>()?;
        let rate_column = aggregates
            .first()?
            .expressions()
            .first()?
            .as_any()
            .downcast_ref::<Column>()?
            .index();
        if aggregates.iter().any(|aggregate| {
            aggregate.expressions().len() != 1
                || aggregate.expressions()[0]
                    .as_any()
                    .downcast_ref::<Column>()
                    .map(Column::index)
                    != Some(rate_column)
        }) {
            return None;
        }
        // The materialized plan drops the windows `prom_rate` left null; the fused path
        // must be able to drop exactly those, and nothing else.
        let predicate = filter
            .predicate()
            .as_any()
            .downcast_ref::<IsNotNullExpr>()?;
        if predicate
            .arg()
            .as_any()
            .downcast_ref::<Column>()
            .map(Column::index)
            != Some(rate_column)
        {
            return None;
        }
        let rate = projection
            .expr()
            .get(rate_column)?
            .expr
            .as_any()
            .downcast_ref::<ScalarFunctionExpr>()?;
        if rate.name() != Rate::name() || rate.args().len() != 4 {
            return None;
        }
        let argument_column = |index: usize| {
            rate.args()[index]
                .as_any()
                .downcast_ref::<Column>()
                .map(Column::index)
        };
        let timestamp_range = argument_column(0)?;
        let value = argument_column(1)?;
        let evaluation_time = argument_column(2)?;
        let range_length = match rate.args()[3].as_any().downcast_ref::<Literal>()?.value() {
            ScalarValue::Int64(Some(value)) => *value,
            _ => return None,
        };
        let range = projection
            .input()
            .as_any()
            .downcast_ref::<RangeManipulateExec>()?;
        let FusedRateSource {
            input,
            range,
            time_index,
            value_index,
        } = range.fused_rate_source(
            timestamp_range,
            value,
            evaluation_time,
            range_length,
            (grid.start, grid.end, grid.step),
        )?;
        let source_group_columns = group_columns
            .iter()
            .map(|index| {
                projection
                    .expr()
                    .get(*index)?
                    .expr
                    .as_any()
                    .downcast_ref::<Column>()
                    .map(Column::index)
            })
            .collect::<Option<Vec<_>>>()?;
        if source_group_columns.get(time_group).copied() != Some(time_index) {
            return None;
        }
        let source_schema = input.schema();
        if source_group_columns
            .iter()
            .enumerate()
            .any(|(group, index)| {
                let Some(field) = source_schema.fields().get(*index) else {
                    return true;
                };
                if group == time_group {
                    !matches!(
                        field.data_type(),
                        DataType::Timestamp(TimeUnit::Millisecond, None)
                    )
                } else {
                    !supported_label(field.data_type())
                }
            })
        {
            return None;
        }
        proves_one_series_per_batch(&input, &source_group_columns, time_group).then_some(())?;
        Some((
            Self {
                group_columns: source_group_columns,
                time_index,
                value_index,
                range,
            },
            input,
        ))
    }
}

/// Whether `input` delivers one whole series per batch, with every group label held
/// constant across it.
///
/// A `PromSeriesDivideExec` cuts its output on tag boundaries and `PromSeriesNormalizeExec`
/// preserves them, so a batch from that pair spans a single series. Requiring the group
/// labels to be among those tags is what lets the fused path read a label once per batch
/// rather than checking it holds for every row.
fn proves_one_series_per_batch(
    input: &Arc<dyn ExecutionPlan>,
    group_columns: &[usize],
    time_group: usize,
) -> bool {
    let Some(divide) = input
        .as_any()
        .downcast_ref::<SeriesNormalizeExec>()
        .and_then(|normalize| normalize.children().into_iter().next().cloned())
    else {
        return false;
    };
    let Some(divide) = divide.as_any().downcast_ref::<SeriesDivideExec>() else {
        return false;
    };
    let schema = divide.schema();
    let tags = divide.tag_columns();
    group_columns
        .iter()
        .enumerate()
        .filter(|(group, _)| *group != time_group)
        .all(|(_, index)| {
            schema
                .fields()
                .get(*index)
                .is_some_and(|field| tags.contains(field.name()))
        })
}

impl SeriesAggregateExec {
    /// The partial aggregate shapes this operator can stand in for, or the reason it
    /// cannot. Returning a reason rather than a bool is what makes a near miss
    /// explainable: these are the plans the rewrite was meant to catch.
    fn unsupported_partial(aggregate: &AggregateExec) -> Option<&'static str> {
        if aggregate.mode() != &AggregateMode::Partial {
            return Some("aggregate is not in Partial mode");
        }
        if aggregate.input_order_mode() != &InputOrderMode::Linear {
            // This node keeps the aggregate's PlanProperties, and those are projected from
            // the input: a non-Linear mode means the input is sorted on a group-key prefix
            // and that ordering is carried into the output. Groups come out here in
            // first-seen order, so inheriting such a claim would mislead whatever consumes
            // it. The emission type differs for the same reason.
            return Some("input order mode is not Linear");
        }
        if !aggregate.group_expr().is_single() {
            // ROLLUP/CUBE need one state per grouping set.
            return Some("grouping is not a single group-by set");
        }
        if aggregate.limit_options().is_some() {
            return Some("aggregate carries a limit");
        }
        if aggregate.filter_expr().iter().any(Option::is_some) {
            return Some("aggregate has a per-aggregate filter");
        }
        if aggregate.aggr_expr().is_empty() {
            return Some("aggregate has no accumulators");
        }
        None
    }

    /// Whether `expr` is one of the float accumulators whose partial state this operator
    /// can produce, reading a single `Float64` column.
    fn unsupported_accumulator(
        expr: &AggregateFunctionExpr,
        schema: &SchemaRef,
    ) -> Result<Option<&'static str>> {
        // Checked before `groups_accumulator_supported`, which happens to reject DISTINCT
        // for both SUM and AVG today but is not obliged to keep doing so. Accepting a
        // DISTINCT aggregate would feed every row to a non-distinct accumulator.
        if expr.is_distinct() {
            return Ok(Some("accumulator is DISTINCT"));
        }
        if !expr.order_bys().is_empty() {
            return Ok(Some("accumulator has an ORDER BY"));
        }
        if !expr.groups_accumulator_supported() {
            return Ok(Some("accumulator has no GroupsAccumulator"));
        }
        if !(expr.fun().inner().as_any().is::<Avg>() || expr.fun().inner().as_any().is::<Sum>()) {
            // Scope, not safety: the merge path is generic over GroupsAccumulator, but
            // only these two are verified against the original partial state layout.
            return Ok(Some("accumulator is neither SUM nor AVG"));
        }
        if expr.expressions().len() != 1 || !expr.expressions()[0].as_any().is::<Column>() {
            return Ok(Some("accumulator argument is not a single column"));
        }
        if expr.expressions()[0].data_type(schema.as_ref())? != DataType::Float64 {
            return Ok(Some("accumulator argument is not Float64"));
        }
        Ok(None)
    }

    /// Retains the original partial schema and accumulator implementations.
    pub fn try_new(
        aggregate: &AggregateExec,
        start: i64,
        end: i64,
        step: i64,
    ) -> Result<Option<Self>> {
        // The caller has already traced the group columns back to a range manipulation, so
        // anything rejected below is a plan that looked like a match and was not.
        macro_rules! reject {
            ($reason:expr) => {{
                debug!(
                    "PromSeriesAggregateExec: not rewriting partial aggregate: {}",
                    $reason
                );
                return Ok(None);
            }};
        }
        if let Some(reason) = Self::unsupported_partial(aggregate) {
            reject!(reason);
        }
        let Some(filter) = aggregate.input().as_any().downcast_ref::<FilterExec>() else {
            reject!("aggregate input is not a filter");
        };
        if filter.fetch().is_some() || filter.projection().is_some() {
            reject!("filter carries a fetch or a projection");
        }
        let Some(grid) = Grid::new(start, end, step) else {
            reject!("evaluation grid is empty or has a non-positive step");
        };
        let schema = filter.schema();
        let mut columns = Vec::new();
        let mut time_group = None;
        for (group, (expr, _)) in aggregate.group_expr().expr().iter().enumerate() {
            let Some(column) = expr.as_any().downcast_ref::<Column>() else {
                // A computed group key would have to be evaluated per row.
                reject!("group key is not a plain column");
            };
            match schema.field(column.index()).data_type() {
                // The first millisecond timestamp is the evaluation time; the grid is
                // expressed in those units, and `Grid::slot` rejects anything off it.
                DataType::Timestamp(TimeUnit::Millisecond, None) if time_group.is_none() => {
                    time_group = Some(group)
                }
                data_type if supported_label(data_type) => {}
                _ => reject!("group key is neither a supported label nor the evaluation time"),
            }
            columns.push(column.index());
        }
        let Some(time_group) = time_group else {
            reject!("no millisecond timestamp among the group keys");
        };
        for expr in aggregate.aggr_expr() {
            if let Some(reason) = Self::unsupported_accumulator(expr, &schema)? {
                reject!(reason);
            }
        }
        let (input, input_mode) = match FusedRateInput::try_new(
            filter,
            aggregate.aggr_expr(),
            &columns,
            time_group,
            grid,
        ) {
            Some((rate, input)) => (input, SeriesAggregateInput::FusedRate(rate)),
            None => (
                filter.input().clone(),
                SeriesAggregateInput::Materialized {
                    predicate: filter.predicate().clone(),
                    arguments: aggregate
                        .aggr_expr()
                        .iter()
                        .map(|a| a.expressions())
                        .collect(),
                },
            ),
        };
        Ok(Some(Self {
            input,
            input_mode,
            aggregates: aggregate.aggr_expr().to_vec(),
            group_by: aggregate.group_expr().clone(),
            columns,
            time_group,
            grid,
            properties: aggregate.properties().clone(),
            metrics: ExecutionPlanMetricsSet::new(),
        }))
    }
}

impl DisplayAs for SeriesAggregateExec {
    fn fmt_as(&self, _format: DisplayFormatType, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(
            f,
            "PromSeriesAggregateExec: mode=Partial, grid=[{}..{}; {}], groups={:?}, aggr=[{}]",
            self.grid.start,
            self.grid.end,
            self.grid.step,
            self.columns,
            self.aggregates
                .iter()
                .map(|a| a.name())
                .collect::<Vec<_>>()
                .join(", ")
        )?;
        if matches!(self.input_mode, SeriesAggregateInput::FusedRate(_)) {
            f.write_str(", fused=prom_rate")?;
        }
        Ok(())
    }
}

impl ExecutionPlan for SeriesAggregateExec {
    fn name(&self) -> &str {
        "PromSeriesAggregateExec"
    }
    fn as_any(&self) -> &dyn Any {
        self
    }
    fn properties(&self) -> &Arc<PlanProperties> {
        &self.properties
    }
    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        vec![&self.input]
    }
    fn with_new_children(
        self: Arc<Self>,
        children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        if children.len() != 1 || children[0].schema() != self.input.schema() {
            return exec_err!("series aggregate requires one child with the original schema");
        }
        if let SeriesAggregateInput::FusedRate(rate) = &self.input_mode {
            // The fused path replaced the range fold, so there is no original chain to fall
            // back to. A child that no longer proves the series source cannot be accepted.
            if !proves_one_series_per_batch(&children[0], &rate.group_columns, self.time_group) {
                return exec_err!(
                    "fused prom_rate aggregate requires a series divide and normalize child"
                );
            }
            return Ok(Arc::new(Self {
                input: children[0].clone(),
                input_mode: self.input_mode.clone(),
                aggregates: self.aggregates.clone(),
                group_by: self.group_by.clone(),
                columns: self.columns.clone(),
                time_group: self.time_group,
                grid: self.grid,
                properties: self.properties.clone(),
                metrics: ExecutionPlanMetricsSet::new(),
            }));
        }
        // Rebuild the original chain and revalidate, so a changed child cannot inherit a
        // stale series-grid proof.
        let input_schema = children[0].schema();
        let SeriesAggregateInput::Materialized { predicate, .. } = &self.input_mode else {
            unreachable!()
        };
        let filter = Arc::new(FilterExec::try_new(predicate.clone(), children[0].clone())?);
        let aggregate = AggregateExec::try_new(
            AggregateMode::Partial,
            self.group_by.clone(),
            self.aggregates.clone(),
            vec![None; self.aggregates.len()],
            filter,
            input_schema,
        )?;
        match Self::try_new(&aggregate, self.grid.start, self.grid.end, self.grid.step)? {
            Some(plan) => Ok(Arc::new(plan)),
            None => Ok(Arc::new(aggregate)),
        }
    }
    fn metrics(&self) -> Option<MetricsSet> {
        Some(self.metrics.clone_inner())
    }
    fn execute(
        &self,
        partition: usize,
        context: Arc<TaskContext>,
    ) -> Result<SendableRecordBatchStream> {
        Ok(Box::pin(self.stream(partition, context)?))
    }
}

impl SeriesAggregateExec {
    fn stream(&self, partition: usize, context: Arc<TaskContext>) -> Result<SeriesAggregateStream> {
        if let SeriesAggregateInput::FusedRate(rate) = &self.input_mode
            && !proves_one_series_per_batch(&self.input, &rate.group_columns, self.time_group)
        {
            return exec_err!("fused prom_rate aggregate lost its series divide child");
        }
        let input = self.input.execute(partition, context.clone())?;
        Ok(SeriesAggregateStream {
            input,
            input_mode: self.input_mode.clone(),
            accumulators: self
                .aggregates
                .iter()
                .map(|a| a.create_groups_accumulator())
                .collect::<Result<_>>()?,
            columns: self.columns.clone(),
            time_group: self.time_group,
            grid: self.grid,
            groups: SeriesGroups::default(),
            ids: Vec::new(),
            pending: None,
            finished: false,
            schema: self.schema(),
            batch_size: context.session_config().batch_size(),
            reservation: MemoryConsumer::new(format!("PromSeriesAggregate[{partition}]"))
                .with_can_spill(true)
                .register(context.memory_pool()),
            metrics: BaselineMetrics::new(&self.metrics, partition),
        })
    }
}

struct SeriesAggregateStream {
    input: SendableRecordBatchStream,
    input_mode: SeriesAggregateInput,
    accumulators: Vec<Box<dyn GroupsAccumulator>>,
    columns: Vec<usize>,
    time_group: usize,
    grid: Grid,
    groups: SeriesGroups,
    ids: Vec<usize>,
    pending: Option<PendingOutput>,
    finished: bool,
    schema: SchemaRef,
    batch_size: usize,
    reservation: MemoryReservation,
    metrics: BaselineMetrics,
}

impl SeriesAggregateStream {
    /// Yields the compact group state instead of expanding it into output batches.
    fn into_states(self) -> futures::stream::BoxStream<'static, Result<PartialState>> {
        futures::stream::try_unfold(self, |mut stream| async move {
            let Some(result) = futures::future::poll_fn(|cx| stream.poll_aggregate(cx)).await
            else {
                return Ok(None);
            };
            result?;
            let output = stream.pending.take().ok_or_else(|| {
                datafusion::error::DataFusionError::Internal(
                    "missing partial aggregate state".to_string(),
                )
            })?;
            let size = output.size();
            let retained = stream.accumulators.iter().map(|a| a.size()).sum::<usize>();
            stream.reservation.try_resize(size + retained)?;
            // The state outlives this stream, so it has to carry its own reservation.
            let reservation = stream.reservation.split(size);
            stream.metrics.record_output(output.keys.len());
            stream.metrics.output_batches().add(1);
            Ok(Some((
                PartialState {
                    output,
                    _reservation: reservation,
                },
                stream,
            )))
        })
        .boxed()
    }

    fn aggregate(&mut self, batch: RecordBatch) -> Result<()> {
        match self.input_mode.clone() {
            SeriesAggregateInput::Materialized {
                predicate,
                arguments,
            } => self.aggregate_materialized(batch, &predicate, &arguments),
            SeriesAggregateInput::FusedRate(rate) => self.aggregate_rate(&batch, &rate),
        }
    }

    fn aggregate_materialized(
        &mut self,
        batch: RecordBatch,
        predicate: &Arc<dyn PhysicalExpr>,
        arguments: &[Vec<Arc<dyn PhysicalExpr>>],
    ) -> Result<()> {
        let predicate = predicate.evaluate(&batch)?.into_array(batch.num_rows())?;
        let predicate = predicate
            .as_any()
            .downcast_ref::<BooleanArray>()
            .ok_or_else(|| {
                datafusion::error::DataFusionError::Execution(
                    "series aggregate filter must be boolean".to_string(),
                )
            })?;
        let batch = filter_record_batch(&batch, predicate)?;
        if batch.num_rows() == 0 {
            return Ok(());
        }
        self.groups.intern(
            &batch,
            &self.columns,
            self.time_group,
            self.grid,
            &mut self.ids,
        )?;
        for (accumulator, arguments) in self.accumulators.iter_mut().zip(arguments) {
            let values = arguments
                .iter()
                .map(|expr| expr.evaluate(&batch)?.into_array(batch.num_rows()))
                .collect::<Result<Vec<_>>>()?;
            accumulator.update_batch(&values, &self.ids, None, self.groups.keys.len())?;
        }
        self.resize_or_flush()
    }

    fn aggregate_rate(&mut self, batch: &RecordBatch, rate: &FusedRateInput) -> Result<()> {
        if batch.num_rows() == 0 {
            return Ok(());
        }
        let timestamps = batch
            .column(rate.time_index)
            .as_any()
            .downcast_ref::<TimestampMillisecondArray>()
            .ok_or_else(|| {
                datafusion::error::DataFusionError::Execution(
                    "fused prom_rate aggregate requires millisecond timestamps".to_string(),
                )
            })?;
        let values = batch
            .column(rate.value_index)
            .as_any()
            .downcast_ref::<Float64Array>()
            .ok_or_else(|| {
                datafusion::error::DataFusionError::Execution(
                    "fused prom_rate aggregate requires Float64 values".to_string(),
                )
            })?;
        let grid = self.grid;
        let (ranges, (start, end)) =
            calculate_ranges(timestamps, grid.start, grid.end, grid.step, rate.range)?;
        if ranges.iter().all(|(_, len)| *len == 0) {
            return Ok(());
        }
        let eval_timestamps = (start..=end)
            .step_by(grid.step as usize)
            .collect::<Vec<_>>();
        let values = calculate_rate(timestamps, values, &ranges, &eval_timestamps, rate.range)?;
        // Null rates are the windows the materialized plan's IS NOT NULL filter removes.
        // A series whose every window is too short contributes nothing, and interning its
        // label would retain a group this operator never charges for or emits.
        if values.null_count() == values.len() {
            return Ok(());
        }
        let labels = series_labels(batch, &rate.group_columns, self.time_group, false)?;
        self.groups.intern_timestamps(
            &labels,
            self.grid,
            eval_timestamps
                .iter()
                .zip(values.iter())
                .filter_map(|(timestamp, value)| value.map(|_| *timestamp)),
            &mut self.ids,
        )?;
        let values = Arc::new(Float64Array::from_iter_values(values.iter().flatten())) as ArrayRef;
        for accumulator in &mut self.accumulators {
            accumulator.update_batch(
                std::slice::from_ref(&values),
                &self.ids,
                None,
                self.groups.keys.len(),
            )?;
        }
        self.resize_or_flush()
    }

    fn resize_or_flush(&mut self) -> Result<()> {
        let size = self.groups.size()
            + self.ids.capacity() * size_of::<usize>()
            + self.accumulators.iter().map(|a| a.size()).sum::<usize>();
        match self.reservation.try_resize(size) {
            Ok(()) => Ok(()),
            // Partial states can be regrouped by the unchanged final aggregate.
            Err(datafusion::error::DataFusionError::ResourcesExhausted(_))
                if self.groups.keys.len() > 1 =>
            {
                self.flush()
            }
            Err(error) => Err(error),
        }
    }

    fn flush(&mut self) -> Result<()> {
        if self.groups.keys.is_empty() {
            return Ok(());
        }
        let mut state = Vec::new();
        for accumulator in &mut self.accumulators {
            state.extend(accumulator.state(EmitTo::All)?);
        }
        let groups = std::mem::take(&mut self.groups);
        self.pending = Some(groups.into_output(state, self.columns.len() - 1));
        self.ids = Vec::new();
        Ok(())
    }

    fn poll_inner(&mut self, cx: &mut Context<'_>) -> Poll<Option<Result<RecordBatch>>> {
        for _ in 0..8 {
            if let Some(pending) = &mut self.pending {
                let started = Instant::now();
                let output = pending.next(&self.schema, self.time_group, self.batch_size);
                self.metrics.elapsed_compute().add_elapsed(started);
                if let Some(batch) = output? {
                    return Poll::Ready(Some(Ok(batch)));
                }
                self.pending = None;
                self.reservation.free();
            }
            match ready!(self.poll_aggregate(cx)) {
                Some(result) => result?,
                None => return Poll::Ready(None),
            }
        }
        cx.waker().wake_by_ref();
        Poll::Pending
    }

    fn poll_aggregate(&mut self, cx: &mut Context<'_>) -> Poll<Option<Result<()>>> {
        for _ in 0..8 {
            if self.pending.is_some() {
                return Poll::Ready(Some(Ok(())));
            }
            if self.finished {
                return Poll::Ready(None);
            }
            match ready!(self.input.poll_next_unpin(cx)) {
                Some(batch) => {
                    let started = Instant::now();
                    let result = self.aggregate(batch?);
                    self.metrics.elapsed_compute().add_elapsed(started);
                    result?;
                }
                None => {
                    self.finished = true;
                    let started = Instant::now();
                    let result = self.flush();
                    self.metrics.elapsed_compute().add_elapsed(started);
                    result?;
                }
            }
        }
        cx.waker().wake_by_ref();
        Poll::Pending
    }
}

/// Compact partial group state, handed to an adjacent final aggregate without being
/// expanded into per-point label rows.
struct PartialState {
    output: PendingOutput,
    _reservation: MemoryReservation,
}

impl PartialState {
    fn into_batches(
        self,
        schema: SchemaRef,
        time_group: usize,
        batch_size: usize,
    ) -> SendableRecordBatchStream {
        let output_schema = schema.clone();
        let stream = futures::stream::try_unfold(self, move |mut state| {
            let result = state.output.next(&schema, time_group, batch_size);
            std::future::ready(result.map(|batch| batch.map(|batch| (batch, state))))
        });
        Box::pin(
            datafusion::physical_plan::stream::RecordBatchStreamAdapter::new(output_schema, stream),
        )
    }
}

impl RecordBatchStream for SeriesAggregateStream {
    fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }
}

impl Stream for SeriesAggregateStream {
    type Item = Result<RecordBatch>;
    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let poll = self.poll_inner(cx);
        if matches!(poll, Poll::Ready(Some(Err(_)))) {
            self.finished = true;
            self.pending = None;
            self.groups = SeriesGroups::default();
            self.ids = Vec::new();
            self.accumulators.clear();
            self.reservation.free();
        }
        self.metrics.record_poll(poll)
    }
}
