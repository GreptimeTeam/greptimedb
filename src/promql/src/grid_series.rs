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
use datafusion::arrow::array::{Float64Array, UInt32Array};
use datafusion::error::{DataFusionError, Result};
use datatypes::series::MetricName;
use datatypes::series::histogram::HistogramBatchBuilder;
use datatypes::series::samples::{
    HistogramSamples, RowCursor, SampleRef, Samples, Series, SeriesFields, SeriesSchema,
};
use datatypes::series::time::{EvalGrid, GridSpan, TimeAxis};

use crate::histogram_eval::float_histogram::FloatHistogram;

#[cfg(test)]
pub(crate) mod reference;
#[cfg(test)]
mod tests;

/// Builds one grid series from rows pushed at absolute grid indices.
///
/// A push names one position and the value each field holds there. Rows are accumulated
/// compactly: a row is kept only when at least one field holds a value at that position, so an
/// empty stretch costs one skipped push and no storage at all. Fields stay row-aligned, so a field
/// with no value at a retained row still gets a null there; the Arrow layout needs one row per
/// retained position, and the position does hold a value.
///
/// [`GridSeriesBuilder::finish`] hands out all the buffered rows as **one** [`Series`], whose
/// [`TimeAxis`] names the maximal runs of consecutive positions the rows make up. The rows are not
/// densified: the axis says where they sit, so a series over two far-apart positions costs two rows
/// and two spans, not a row per position between them.
///
/// A builder serves one series at a time. [`GridSeriesBuilder::begin_series`] starts the next one;
/// nothing else resets the series state, so [`GridSeriesBuilder::take_buffered`] can hand out the
/// rows gathered so far and let the same series go on.
pub struct GridSeriesBuilder {
    schema: Arc<SeriesSchema>,
    grid: Arc<EvalGrid>,
    /// Smallest index the next push may use.
    next: usize,
    /// Grid index of each retained row, ascending.
    ///
    /// This vector is the only record of where the gaps are, and it says only where rows sit:
    /// with `V` entries, `F` fields and `k` runs (`k <= V`) the whole series costs `8*F*V` value
    /// bytes plus validity bits, one `usize` per retained row and `O(k)` run descriptors. Nothing
    /// is proportional to the positions holding no value.
    indices: Vec<usize>,
    /// One row per retained row, per field; empty when nothing is buffered.
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

impl GridSeriesBuilder {
    /// Creates a builder for series of `schema`, which must have an evaluation grid.
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

    /// Pushes the field values at absolute grid `index`; `None` is a missing value.
    ///
    /// Indices must be strictly increasing; skipped indices are positions that hold no value.
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
                    field.values.push(0.0);
                    field.nulls.append(false);
                    let rows = field.values.len() - 1;
                    let column = field.histograms.get_or_insert_with(|| HistogramColumn {
                        batch: HistogramBatchBuilder::new(),
                        indices: vec![None; rows],
                    });
                    column.indices.push(Some(h.push_to(&mut column.batch)?));
                }
                Some(GridSample::Float(value)) => field.push_float(Some(*value)),
                None => field.push_float(None),
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
        if index < self.next || index >= self.grid.timestamps().len() {
            return Err(DataFusionError::Execution(format!(
                "grid series index {index} is not increasing or exceeds grid length {}",
                self.grid.timestamps().len()
            )));
        }
        self.next = index + 1;
        if empty {
            // A position holding no value anywhere is not a row: an index absent from `indices`
            // already says exactly that, so writing it down would only cost storage.
            return Ok(false);
        }
        if self.fields.is_empty() {
            self.fields = (0..len).map(|_| FieldBuilder::new()).collect();
        }
        self.indices.push(index);
        Ok(true)
    }

    /// Whether rows have been pushed that no caller has taken yet.
    ///
    /// Every hand-out drains the buffer, so "rows buffered" and "rows a caller has not taken" are
    /// the same thing: a `true` here means the next [`Self::finish`] or [`Self::take_buffered`] has
    /// something to return.
    pub fn has_pending(&self) -> bool {
        !self.indices.is_empty()
    }

    /// Starts the series the following pushes belong to.
    ///
    /// This is the only place that resets the series state, so it is the logical-series boundary:
    /// [`Self::finish`] hands the rows out but resets nothing, and [`Self::take_buffered`]
    /// deliberately does not reset anything either.
    ///
    /// Panics in debug builds when rows are still pending, which means the previous series was
    /// left unread.
    pub fn begin_series(&mut self) {
        debug_assert!(
            !self.has_pending(),
            "a new series starts with rows of the previous one still pending"
        );
        self.next = 0;
        self.indices.clear();
        self.fields.clear();
    }

    /// Hands out every row buffered so far as one series, without ending the series.
    ///
    /// This is a prefix flush: the rows pushed since the last hand-out become one [`Series`] whose
    /// axis names the runs they make up. `None` means nothing is buffered, and the caller may push
    /// more rows. Later pushes still belong to the same logical series, so the caller keeps the
    /// metric and reads the next hand-out as a continuation rather than as a new series.
    ///
    /// Nothing is reset: not the metric, not the index cursor, not the position order. A caller
    /// that wants a new series calls [`Self::begin_series`].
    pub fn take_buffered(&mut self, metric: MetricName) -> Result<Option<Series>> {
        self.collect(metric)
    }

    /// Returns the series the pushed rows make up: one [`Series`] over the runs of consecutive
    /// positions that hold a value, or `None` when no retained row holds one.
    ///
    /// The rows share the one buffer per field, so the result costs a span per run and not a row
    /// per position between the runs. This ends the logical series; the next one starts with
    /// [`Self::begin_series`], which is the only reset.
    pub fn finish(&mut self, metric: MetricName) -> Result<Option<Series>> {
        self.collect(metric)
    }

    /// Turns the buffered rows into one series over the runs they make up.
    fn collect(&mut self, metric: MetricName) -> Result<Option<Series>> {
        if self.indices.is_empty() {
            return Ok(None);
        }
        let fields = std::mem::take(&mut self.fields);
        let indices = std::mem::take(&mut self.indices);
        let samples = fields
            .into_iter()
            .map(|mut field| {
                let floats = Float64Array::new(field.values.into(), field.nulls.finish());
                let Some(column) = field.histograms else {
                    return Ok(Samples::from_float(floats));
                };
                let batch = column.batch.finish().map_err(external)?;
                let histograms =
                    HistogramSamples::try_new(UInt32Array::from(column.indices), batch)
                        .map_err(external)?;
                Samples::try_from_mixed(floats, histograms).map_err(external)
            })
            .collect::<Result<Vec<_>>>()?;
        let fields = SeriesFields::try_from_vec(samples).map_err(external)?;
        // The runs are all the axis has to remember about the gaps, and the rows are already
        // compact, so no position holding no value is visited or stored.
        let spans = spans_of(&indices);
        let time = TimeAxis::from_grid_spans(self.grid.clone(), spans).map_err(external)?;
        Series::try_new(&self.schema, metric, time, fields)
            .map(Some)
            .map_err(external)
    }
}

/// Cuts the retained indices into maximal runs of consecutive positions.
fn spans_of(indices: &[usize]) -> Vec<GridSpan> {
    let mut spans: Vec<GridSpan> = Vec::new();
    let mut start = 0;
    while let Some(&position) = indices.get(start) {
        let mut len = 1;
        while indices.get(start + len) == Some(&(position + len)) {
            len += 1;
        }
        spans.push(GridSpan::new(position, len));
        start += len;
    }
    spans
}

impl FieldBuilder {
    fn new() -> Self {
        Self {
            values: Vec::new(),
            nulls: NullBufferBuilder::new(0),
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
}

/// Prometheus `schema.IsMetadataLabel`: the labels removed with the metric name.
pub(crate) const METADATA_LABELS: [&str; 3] = ["__name__", "__type__", "__unit__"];

pub(crate) type LabelSet = Vec<(Arc<str>, Arc<str>)>;

/// The label set Prometheus compares, which has no empty values.
pub(crate) fn label_set(metric: &MetricName) -> LabelSet {
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
/// rewrites labels or drops `__name__`; two of them holding one field at one point fail.
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

    let mut series: Vec<Option<Series>> = series.into_iter().map(Some).collect();
    let mut output = Vec::with_capacity(groups.len());
    let mut builder = GridSeriesBuilder::try_new(schema.clone())?;
    // `(absolute position, member, compact row)`, sorted by position before merging.
    let mut rows: Vec<(usize, usize, usize)> = Vec::new();
    let mut values = vec![None; schema.fields.len()];
    let mut merged = vec![None; schema.fields.len()];
    for (labels, members) in groups {
        if let [member] = members[..] {
            output.extend(series[member].take());
            continue;
        }
        builder.begin_series();
        rows.clear();
        for &member in &members {
            let member_series = series[member]
                .as_ref()
                .expect("each series is in one group");
            if member_series.time.grid().is_none() {
                return Err(DataFusionError::Execution(
                    "merging requires evaluated series".into(),
                ));
            }
            // The axis names the positions this member holds, and one cursor per field reads its
            // values in the same order, so a member of several runs costs its rows.
            let mut cursors: Vec<RowCursor<'_>> = member_series
                .fields
                .iter()
                .map(|field| field.cursor())
                .collect();
            for (point, row) in member_series.time.positions() {
                if read_row(&mut cursors, row, &mut values)? {
                    rows.push((point, member, row));
                }
            }
        }
        // By position, then by member: the merge below walks the rows of each member forward, so
        // its cursors never go back.
        rows.sort_unstable();
        // One forward cursor per member and field, so re-reading a row costs neither a chunk scan
        // nor an allocation.
        let mut cursors: Vec<Vec<RowCursor<'_>>> = members
            .iter()
            .map(|&member| {
                let member_series = series[member]
                    .as_ref()
                    .expect("each series is in one group");
                member_series
                    .fields
                    .iter()
                    .map(|field| field.cursor())
                    .collect()
            })
            .collect();
        let mut start = 0;
        while start < rows.len() {
            let point = rows[start].0;
            let end = start + rows[start..].partition_point(|&(p, _, _)| p == point);
            // Rows of one point merge field by field; only a field present twice collides.
            merged.iter_mut().for_each(|value| *value = None);
            for &(_, member, row) in &rows[start..end] {
                let member_index = members
                    .iter()
                    .position(|candidate| *candidate == member)
                    .expect("the row names one of the members");
                read_row(&mut cursors[member_index], row, &mut values)?;
                for (slot, value) in merged.iter_mut().zip(&values) {
                    if value.is_some() {
                        if slot.is_some() {
                            let labels: Vec<_> = labels
                                .iter()
                                .map(|(name, value)| format!("{name}=\"{value}\""))
                                .collect();
                            return Err(DataFusionError::Execution(format!(
                                "vector cannot contain metrics with the same labelset {{{}}}",
                                labels.join(", ")
                            )));
                        }
                        *slot = *value;
                    }
                }
            }
            builder.push(point, &merged)?;
            start = end;
        }
        let metric = series[members[0]].as_ref().unwrap().metric.clone();
        output.extend(builder.finish(metric)?);
    }
    Ok(output)
}

/// Reads the float fields of `row` through the member's cursors; returns whether any is present.
fn read_row(cursors: &mut [RowCursor<'_>], row: usize, values: &mut [Option<f64>]) -> Result<bool> {
    let mut present = false;
    for (value, cursor) in values.iter_mut().zip(cursors) {
        *value = match cursor.at(row).expect("the axis and the fields are aligned") {
            SampleRef::Missing => None,
            SampleRef::Float(value) => Some(value),
            SampleRef::Histogram(_) => {
                return Err(DataFusionError::NotImplemented(
                    "merging histogram series with the same label set".into(),
                ));
            }
        };
        present |= value.is_some();
    }
    Ok(present)
}

pub(crate) fn external(error: datatypes::error::Error) -> DataFusionError {
    DataFusionError::External(Box::new(error))
}
