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

//! Building evaluated series that cover only their result range on a shared grid.

use std::collections::HashMap;
use std::collections::hash_map::Entry;
use std::sync::Arc;

use datafusion::arrow::array::builder::NullBufferBuilder;
use datafusion::arrow::array::{Array, Float64Array, UInt32Array};
use datafusion::error::{DataFusionError, Result};
use datatypes::series::SeriesLabels;
use datatypes::series::histogram::{HistogramBatch, HistogramBatchBuilder};
use datatypes::series::samples::{
    DenseSamplesRef, HistogramSamples, SampleRef, Samples, Series, SeriesFields, SeriesSchema,
};
use datatypes::series::time::{EvalGrid, GridSpan, TimeAxis};

use crate::histogram_eval::float_histogram::FloatHistogram;

#[cfg(test)]
pub(crate) mod reference;
#[cfg(test)]
mod tests;

pub(crate) fn series_bytes(series: &Series) -> usize {
    let runs = series.time.runs().count();
    let segments: usize = series
        .fields
        .iter()
        .map(|field| field.segments().len())
        .sum();
    std::mem::size_of::<Series>()
        .saturating_add(std::mem::size_of::<TimeAxis>())
        .saturating_add(runs.saturating_mul(std::mem::size_of::<(usize, usize)>()))
        .saturating_add(
            segments.saturating_mul(std::mem::size_of::<Samples>() + std::mem::size_of::<usize>()),
        )
        .saturating_add(fields_bytes(&series.fields))
        .saturating_add(series.metric.name().map_or(0, str::len))
        .saturating_add(std::mem::size_of_val(series.metric.tags()))
        .saturating_add(
            series
                .metric
                .tags()
                .iter()
                .map(|tag| tag.name.len() + tag.value.len())
                .sum::<usize>(),
        )
}

pub(crate) fn fields_bytes(fields: &SeriesFields) -> usize {
    fields
        .iter()
        .map(|field| {
            field.segments().iter().fold(0usize, |total, segment| {
                total.saturating_add(segment_bytes(segment))
            })
        })
        .fold(0usize, usize::saturating_add)
}

/// The bytes one dense segment of samples holds.
pub(crate) fn segment_bytes(segment: &Samples) -> usize {
    match segment.dense() {
        Some(DenseSamplesRef::Float(values)) => values.get_array_memory_size(),
        Some(DenseSamplesRef::Histogram(histograms)) => histograms
            .indices()
            .get_array_memory_size()
            .saturating_add(table_bytes(histograms.batch())),
        Some(DenseSamplesRef::Mixed(mixed)) => mixed
            .floats()
            .get_array_memory_size()
            .saturating_add(mixed.histograms().indices().get_array_memory_size())
            .saturating_add(table_bytes(mixed.histograms().batch())),
        // `segments` yields dense segments only.
        None => 0,
    }
}

pub(crate) fn table_bytes(table: &Arc<HistogramBatch>) -> usize {
    std::mem::size_of::<HistogramBatch>().saturating_add(table.as_arrow().get_array_memory_size())
}

pub(crate) fn field_builder_bytes(width: usize) -> usize {
    width.saturating_mul(std::mem::size_of::<FieldBuilder>())
}

/// Builds one grid series from rows pushed at increasing absolute grid indices.
///
/// A row is kept only when at least one field holds a value; a field without a value at a kept row
/// gets a null. The rows are handed out as one [`Series`] whose [`TimeAxis`] names their maximal
/// runs, so positions without a value cost nothing.
///
/// A builder serves one series at a time; only [`GridSeriesBuilder::begin_series`] resets it.
pub struct GridSeriesBuilder {
    schema: Arc<SeriesSchema>,
    grid: Arc<EvalGrid>,
    /// Smallest index the next push may use.
    next: usize,
    /// Grid index of each retained row, ascending.
    indices: Vec<usize>,
    /// Empty until the first retained row of a hand-out.
    fields: Vec<FieldBuilder>,
}

struct FieldBuilder {
    values: Vec<f64>,
    nulls: NullBufferBuilder,
    /// Created at the first histogram value; holds an index per row from then on.
    histograms: Option<HistogramColumn>,
}

struct HistogramColumn {
    batch: HistogramBatchBuilder,
    indices: Vec<Option<u32>>,
}

/// A value of [`GridSeriesBuilder::push_samples`].
pub(crate) enum GridSample {
    Float(f64),
    /// Boxed so that buffered float results stay as small as `Option<f64>`.
    Histogram(Box<FloatHistogram>),
}

const _: () = assert!(std::mem::size_of::<Option<GridSample>>() == 16);

/// `sample` as an owned value; `None` when missing.
pub(crate) fn owned(sample: SampleRef<'_>) -> Result<Option<GridSample>> {
    Ok(match sample {
        SampleRef::Missing => None,
        SampleRef::Float(value) => Some(GridSample::Float(value)),
        SampleRef::Histogram(view) => Some(GridSample::Histogram(Box::new(
            FloatHistogram::try_from_view(&view)?,
        ))),
    })
}

impl GridSeriesBuilder {
    /// Errors when `schema` has no evaluation grid.
    pub fn try_new(schema: Arc<SeriesSchema>) -> Result<Self> {
        let grid = schema.eval_grid.clone().ok_or_else(|| {
            DataFusionError::Execution("grid series schema requires an evaluation grid".into())
        })?;
        Ok(Self {
            schema,
            grid,
            next: 0,
            indices: Vec::new(),
            fields: Vec::new(),
        })
    }

    /// Reserves capacity for the exact number of valued rows in the next series.
    ///
    /// Call after `begin_series` and before its first push; callers that skip it rely on normal
    /// growth.
    pub(crate) fn reserve_rows(&mut self, rows: usize) -> Result<()> {
        self.indices
            .try_reserve_exact(rows.saturating_sub(self.indices.len()))
            .map_err(|error| {
                DataFusionError::Execution(format!("cannot reserve grid row indices: {error}"))
            })?;
        if rows == 0 {
            return Ok(());
        }
        if self.fields.is_empty() {
            self.fields = (0..self.schema.fields.len())
                .map(|_| FieldBuilder::new(rows))
                .collect();
        }
        for field in &mut self.fields {
            field
                .values
                .try_reserve_exact(rows.saturating_sub(field.values.len()))
                .map_err(|error| {
                    DataFusionError::Execution(format!("cannot reserve grid values: {error}"))
                })?;
            field.nulls = NullBufferBuilder::new(rows);
        }
        Ok(())
    }

    /// Pushes the field values at absolute grid `index`; `None` is a missing value.
    ///
    /// Errors unless indices strictly increase within the grid and `values` has one entry per field.
    pub fn push(&mut self, index: usize, values: &[Option<f64>]) -> Result<()> {
        if !self.start_row(index, values.len(), values.iter().all(Option::is_none))? {
            return Ok(());
        }
        for (field, value) in self.fields.iter_mut().zip(values) {
            field.push_float(*value);
        }
        Ok(())
    }

    /// Like [`GridSeriesBuilder::push`], with native histogram values.
    pub(crate) fn push_samples(
        &mut self,
        index: usize,
        values: &[Option<GridSample>],
    ) -> Result<()> {
        if !self.start_row(index, values.len(), values.iter().all(Option::is_none))? {
            return Ok(());
        }
        for (field, value) in self.fields.iter_mut().zip(values) {
            match value {
                Some(GridSample::Histogram(h)) => {
                    let column = field.start_histogram();
                    column.indices.push(Some(h.push_to(&mut column.batch)?));
                }
                Some(GridSample::Float(value)) => field.push_float(Some(*value)),
                None => field.push_float(None),
            }
        }
        Ok(())
    }

    /// Like [`GridSeriesBuilder::push_samples`], copying histograms from their views without an
    /// owned intermediate.
    pub(crate) fn push_refs(&mut self, index: usize, values: &[SampleRef<'_>]) -> Result<()> {
        if !self.start_row(
            index,
            values.len(),
            values
                .iter()
                .all(|value| matches!(value, SampleRef::Missing)),
        )? {
            return Ok(());
        }
        for (field, value) in self.fields.iter_mut().zip(values) {
            match value {
                SampleRef::Histogram(view) => {
                    let column = field.start_histogram();
                    column
                        .indices
                        .push(Some(column.batch.push(*view).map_err(external)?));
                }
                SampleRef::Float(value) => field.push_float(Some(*value)),
                SampleRef::Missing => field.push_float(None),
            }
        }
        Ok(())
    }

    /// Validates the row and retains it; returns whether it holds a value to write.
    fn start_row(&mut self, index: usize, len: usize, empty: bool) -> Result<bool> {
        if len != self.schema.fields.len() {
            return Err(DataFusionError::Execution(format!(
                "grid series row has {len} fields, schema has {}",
                self.schema.fields.len()
            )));
        }
        if index < self.next || index >= self.grid.spec().len as usize {
            return Err(DataFusionError::Execution(format!(
                "grid series index {index} is not increasing or exceeds grid length {}",
                self.grid.spec().len as usize
            )));
        }
        self.next = index + 1;
        if empty {
            return Ok(false);
        }
        if self.fields.is_empty() {
            self.fields = (0..len).map(|_| FieldBuilder::new(0)).collect();
        }
        self.indices.push(index);
        Ok(true)
    }

    /// Whether the next [`Self::finish`] or [`Self::take_buffered`] has rows to return.
    pub fn has_pending(&self) -> bool {
        !self.indices.is_empty()
    }

    /// Starts the series the following pushes belong to and resets the index cursor.
    ///
    /// Panics in debug builds when rows of the previous series are still pending.
    pub fn begin_series(&mut self) {
        debug_assert!(
            !self.has_pending(),
            "a new series starts with rows of the previous one still pending"
        );
        self.next = 0;
        self.indices.clear();
        self.fields.clear();
    }

    /// Hands out the rows pushed since the last hand-out as one [`Series`], or `None` when there
    /// are none. The index cursor is kept, so later pushes continue the same series.
    ///
    /// Same as [`Self::finish`]; the name marks a prefix flush at the call site.
    pub fn take_buffered(&mut self, metric: SeriesLabels) -> Result<Option<Series>> {
        self.collect(metric)
    }

    /// Hands out the rows pushed since the last hand-out as one [`Series`], or `None` when there
    /// are none. Same as [`Self::take_buffered`]: neither resets the index cursor, so a caller
    /// starting another series calls [`Self::begin_series`].
    pub fn finish(&mut self, metric: SeriesLabels) -> Result<Option<Series>> {
        self.collect(metric)
    }

    fn collect(&mut self, metric: SeriesLabels) -> Result<Option<Series>> {
        if self.indices.is_empty() {
            return Ok(None);
        }
        let fields = std::mem::take(&mut self.fields);
        let indices = std::mem::take(&mut self.indices);
        let mut samples = Vec::new();
        samples.try_reserve_exact(fields.len()).map_err(|error| {
            DataFusionError::Execution(format!("cannot reserve grid field descriptions: {error}"))
        })?;
        for mut field in fields {
            let floats = Float64Array::new(field.values.into(), field.nulls.finish());
            let sample = if let Some(column) = field.histograms {
                let batch = column.batch.finish().map_err(external)?;
                let histograms =
                    HistogramSamples::try_new(UInt32Array::from(column.indices), batch)
                        .map_err(external)?;
                Samples::try_from_mixed(floats, histograms).map_err(external)?
            } else {
                Samples::from_float(floats)
            };
            samples.push(sample);
        }
        let fields = SeriesFields::try_from_vec(samples).map_err(external)?;
        let run_count = count_runs(&indices);
        let spans = spans_of(&indices, run_count)?;
        let time = TimeAxis::from_grid_spans(self.grid.clone(), spans).map_err(external)?;
        Series::try_new(&self.schema, metric, time, fields)
            .map(Some)
            .map_err(external)
    }
}

/// Counts the maximal runs of consecutive positions in ascending `indices`.
fn count_runs(indices: &[usize]) -> usize {
    indices
        .windows(2)
        .filter(|pair| pair[1] != pair[0] + 1)
        .count()
        .saturating_add(usize::from(!indices.is_empty()))
}

fn spans_of(indices: &[usize], runs: usize) -> Result<Vec<GridSpan>> {
    let mut spans: Vec<GridSpan> = Vec::new();
    spans.try_reserve_exact(runs).map_err(|error| {
        DataFusionError::Execution(format!("cannot reserve grid spans: {error}"))
    })?;
    let mut start = 0;
    while let Some(&position) = indices.get(start) {
        let mut len = 1;
        while indices.get(start + len) == Some(&(position + len)) {
            len += 1;
        }
        spans.push(GridSpan::new(position, len));
        start += len;
    }
    Ok(spans)
}

impl FieldBuilder {
    fn new(rows: usize) -> Self {
        Self {
            values: Vec::new(),
            nulls: NullBufferBuilder::new(rows),
            histograms: None,
        }
    }

    fn push_float(&mut self, value: Option<f64>) {
        self.values.push(value.unwrap_or_default());
        self.nulls.append(value.is_some());
        if let Some(column) = &mut self.histograms {
            column.indices.push(None);
        }
    }

    /// Writes the float side of a histogram row and returns the column its index goes into.
    fn start_histogram(&mut self) -> &mut HistogramColumn {
        self.values.push(0.0);
        self.nulls.append(false);
        let rows = self.values.len() - 1;
        self.histograms.get_or_insert_with(|| HistogramColumn {
            batch: HistogramBatchBuilder::new(),
            indices: vec![None; rows],
        })
    }
}

/// Prometheus `schema.IsMetadataLabel`: the labels removed with the metric name.
pub(crate) const METADATA_LABELS: [&str; 3] = ["__name__", "__type__", "__unit__"];

pub(crate) type LabelSet = Vec<(Arc<str>, Arc<str>)>;

/// A label set as Prometheus prints one in errors: `{name="value", ...}`.
pub(crate) fn render_label_set(labels: &LabelSet) -> String {
    let labels: Vec<String> = labels
        .iter()
        .map(|(name, value)| format!("{name}=\"{value}\""))
        .collect();
    format!("{{{}}}", labels.join(", "))
}

/// The label set Prometheus compares, which has no empty values.
pub(crate) fn label_set(metric: &SeriesLabels) -> LabelSet {
    let name = metric
        .name()
        .filter(|name| !name.is_empty())
        .map(|name| (Arc::from("__name__"), Arc::from(name)));
    name.into_iter()
        .chain(
            metric
                .tags()
                .iter()
                .filter(|tag| !tag.value.is_empty())
                .map(|tag| (tag.name.clone(), tag.value.clone())),
        )
        .collect()
}

/// Merges series that share a label set into one, as Prometheus does after a function that
/// rewrites labels or drops `__name__`. Errors when two of them hold the same field at one point.
///
/// Values are sliced from their segments rather than copied. Merged series keep the order of their
/// first member; the first member's metric names the result.
pub fn merge_same_labelset(schema: &Arc<SeriesSchema>, series: Vec<Series>) -> Result<Vec<Series>> {
    let mut groups: HashMap<LabelSet, Vec<usize>> = HashMap::with_capacity(series.len());
    let mut collided = false;
    for (index, series) in series.iter().enumerate() {
        match groups.entry(label_set(&series.metric)) {
            Entry::Occupied(mut entry) => {
                entry.get_mut().push(index);
                collided = true;
            }
            Entry::Vacant(entry) => {
                entry.insert(vec![index]);
            }
        }
    }
    if !collided {
        return Ok(series);
    }

    let grid = schema.eval_grid.clone().ok_or_else(|| {
        DataFusionError::Execution("grid series schema requires an evaluation grid".into())
    })?;
    let mut series: Vec<Option<Series>> = series.into_iter().map(Some).collect();
    let mut output = Vec::with_capacity(groups.len());
    // Order by first member so the output order is deterministic across runs.
    let mut groups: Vec<(LabelSet, Vec<usize>)> = groups.into_iter().collect();
    groups.sort_unstable_by_key(|(_, members)| members[0]);
    // `(absolute position, member slot, compact row)`.
    let mut rows: Vec<(usize, usize, usize)> = Vec::new();
    let mut positions: Vec<usize> = Vec::new();
    for (labels, members) in groups {
        if let [member] = members[..] {
            output.extend(series[member].take());
            continue;
        }
        let mut fragments: Vec<&Series> = Vec::with_capacity(members.len());
        for &member in &members {
            let fragment = series[member].as_ref().ok_or_else(|| {
                DataFusionError::Execution("each series is in exactly one label set".into())
            })?;
            let Some(fragment_grid) = fragment.time.grid() else {
                return Err(DataFusionError::Execution(
                    "merging requires evaluated series".into(),
                ));
            };
            if fragment_grid.spec() != grid.spec() {
                return Err(DataFusionError::Execution(
                    "merging requires one evaluation grid".into(),
                ));
            }
            if fragment.fields.len() != schema.fields.len() {
                return Err(DataFusionError::Execution(format!(
                    "merging requires {} fields, a fragment holds {}",
                    schema.fields.len(),
                    fragment.fields.len()
                )));
            }
            let time_len = fragment.time.len();
            if let Some((field, samples)) = fragment
                .fields
                .iter()
                .enumerate()
                .find(|(_, samples)| samples.len() != time_len)
            {
                return Err(DataFusionError::Execution(format!(
                    "merging requires every field to hold the {time_len} rows of its axis, field \
                     {field} holds {}",
                    samples.len()
                )));
            }
            fragments.push(fragment);
        }
        rows.clear();
        for (member_slot, fragment) in fragments.iter().enumerate() {
            let mut readers: Vec<FieldReader<'_>> =
                fragment.fields.iter().map(FieldReader::new).collect();
            for (position, row) in fragment.time.positions() {
                let mut present = false;
                for reader in &mut readers {
                    let (segment, offset) = reader.locate(row)?;
                    present |= !matches!(segment.sample(offset), SampleRef::Missing);
                }
                if present {
                    rows.push((position, member_slot, row));
                }
            }
        }
        // Within one member, rows stay ascending, so its `FieldReader`s only move forward.
        rows.sort_unstable();
        positions.clear();
        positions.extend(rows.iter().map(|&(position, _, _)| position));
        positions.dedup();
        let mut fields = Vec::with_capacity(schema.fields.len());
        for field in 0..schema.fields.len() {
            let mut readers: Vec<FieldReader<'_>> = fragments
                .iter()
                .map(|fragment| FieldReader::new(&fragment.fields.as_slice()[field]))
                .collect();
            fields.push(merge_field(&mut readers, &rows, &labels)?);
        }
        let time =
            TimeAxis::from_grid_spans(grid.clone(), spans_of(&positions, count_runs(&positions))?)
                .map_err(external)?;
        let fields = SeriesFields::try_from_vec(fields).map_err(external)?;
        // Members may differ in empty-valued labels; the first member's metric is kept.
        let metric = fragments[0].metric.clone();
        output.push(Series::try_new(schema, metric, time, fields).map_err(external)?);
    }
    Ok(output)
}

/// Builds one field of the merged series, one sample per merged position: slices of member segments
/// for values, null runs for positions only other fields fill.
fn merge_field(
    readers: &mut [FieldReader<'_>],
    rows: &[(usize, usize, usize)],
    labels: &LabelSet,
) -> Result<Samples> {
    let mut chunks: Vec<Samples> = Vec::new();
    let mut run: Option<FieldRun<'_>> = None;
    let mut missing = 0;
    let mut start = 0;
    while start < rows.len() {
        let position = rows[start].0;
        let mut end = start;
        let mut value: Option<(usize, &Samples, usize)> = None;
        while end < rows.len() && rows[end].0 == position {
            let (_, member, row) = rows[end];
            let (segment, offset) = readers[member].locate(row)?;
            if !matches!(segment.sample(offset), SampleRef::Missing) {
                if value.is_some() {
                    let labels: Vec<_> = labels
                        .iter()
                        .map(|(name, value)| format!("{name}=\"{value}\""))
                        .collect();
                    return Err(DataFusionError::Execution(format!(
                        "vector cannot contain metrics with the same labelset {{{}}}",
                        labels.join(", ")
                    )));
                }
                value = Some((member, segment, offset));
            }
            end += 1;
        }
        match value {
            Some((member, segment, offset)) => {
                if let Some(run) = &mut run
                    && run.continues(member, segment, offset)
                {
                    run.len += 1;
                } else {
                    if let Some(run) = run.take() {
                        run.push(&mut chunks)?;
                    }
                    push_missing(&mut chunks, &mut missing);
                    run = Some(FieldRun {
                        member,
                        segment,
                        offset,
                        len: 1,
                    });
                }
            }
            None => {
                if let Some(run) = run.take() {
                    run.push(&mut chunks)?;
                }
                missing += 1;
            }
        }
        start = end;
    }
    if let Some(run) = run {
        run.push(&mut chunks)?;
    }
    push_missing(&mut chunks, &mut missing);
    Samples::try_from_chunks(chunks).map_err(external)
}

/// Consecutive rows of one member's dense segment, handed out as one [`Samples::slice`].
struct FieldRun<'a> {
    member: usize,
    segment: &'a Samples,
    offset: usize,
    len: usize,
}

impl FieldRun<'_> {
    fn continues(&self, member: usize, segment: &Samples, offset: usize) -> bool {
        self.member == member
            && std::ptr::eq(self.segment, segment)
            && self.offset + self.len == offset
    }

    fn push(self, chunks: &mut Vec<Samples>) -> Result<()> {
        chunks.push(
            self.segment
                .slice(self.offset, self.len)
                .map_err(external)?,
        );
        Ok(())
    }
}

/// Forward-only reader over a chunked field. [`Samples::sample`] rescans segments from the first on
/// every call; this walks them once.
struct FieldReader<'a> {
    segments: &'a [Samples],
    segment: usize,
    /// Logical row the current segment starts at.
    start: usize,
}

impl<'a> FieldReader<'a> {
    fn new(samples: &'a Samples) -> Self {
        Self {
            segments: samples.segments(),
            segment: 0,
            start: 0,
        }
    }

    /// Returns the dense segment holding `row` and the offset inside it. `row` must not go
    /// backwards.
    fn locate(&mut self, row: usize) -> Result<(&'a Samples, usize)> {
        while let Some(segment) = self.segments.get(self.segment) {
            if let Some(offset) = row
                .checked_sub(self.start)
                .filter(|offset| *offset < segment.len())
            {
                return Ok((segment, offset));
            }
            self.segment += 1;
            self.start += segment.len();
        }
        Err(DataFusionError::Execution(
            "merging requires each field to be row-aligned with its time axis".into(),
        ))
    }
}

fn push_missing(chunks: &mut Vec<Samples>, missing: &mut usize) {
    if *missing != 0 {
        chunks.push(Samples::from_float(Float64Array::new_null(*missing)));
        *missing = 0;
    }
}

pub(crate) fn external(error: datatypes::error::Error) -> DataFusionError {
    DataFusionError::External(Box::new(error))
}
