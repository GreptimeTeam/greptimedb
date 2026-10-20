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

use std::collections::hash_map::Entry;
use std::collections::{HashMap, VecDeque};
use std::sync::Arc;

use datafusion::arrow::array::builder::NullBufferBuilder;
use datafusion::arrow::array::{Float64Array, UInt32Array};
use datafusion::error::{DataFusionError, Result};
use datatypes::series::MetricName;
use datatypes::series::histogram::HistogramBatchBuilder;
use datatypes::series::samples::{
    HistogramSamples, SampleRef, Samples, Series, SeriesFields, SeriesSchema,
};
use datatypes::series::time::{EvalGrid, GridRange, TimeAxis};

use crate::histogram_eval::float_histogram::FloatHistogram;

#[cfg(test)]
pub(crate) mod reference;
#[cfg(test)]
mod tests;

/// Builds grid series from rows pushed at absolute grid indices, one piece per run of
/// consecutive positions that hold a value.
///
/// A push names one position and the value each field holds there. Rows are accumulated
/// compactly: a row is kept only when at least one field holds a value at that position, so an
/// empty stretch costs one skipped push and no storage at all. Fields stay row-aligned, so a field
/// with no value at a retained row still gets a null there; the Arrow layout needs one row per
/// retained position, and the position does hold a value.
///
/// [`GridSeriesBuilder::finish`] cuts the accumulated rows into maximal runs of consecutive
/// positions and returns one [`Series`] per run, each a slice sharing the one buffer per field.
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
    /// Batches of finalized rows still to hand out, oldest first.
    ///
    /// A batch keeps its own buffers because a run's offset indexes the buffers it was finalized
    /// with: merging two batches' runs under one `fields` would cut them at the wrong place.
    finished: VecDeque<Finished>,
}

/// Retained rows turned into field buffers a run can be sliced out of.
struct Finished {
    fields: SeriesFields,
    /// Absolute start, compact offset and length of each run, in grid order.
    runs: VecDeque<Run>,
}

/// A maximal run of consecutive retained positions.
struct Run {
    /// Absolute grid index of the run's first row.
    start: usize,
    /// Where the run begins in the compact buffers.
    offset: usize,
    len: usize,
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
            finished: VecDeque::new(),
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
    pub fn has_pending(&self) -> bool {
        !self.indices.is_empty()
            || self
                .finished
                .iter()
                .any(|finished| !finished.runs.is_empty())
    }

    /// Starts the series the following pushes belong to.
    ///
    /// A caller that takes pieces one at a time never reaches [`Self::finish`], so this is what
    /// releases the series before: rows are pushed in increasing order, but the next series may
    /// begin before the previous one ended, since a piece covers only the positions holding a
    /// value.
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
        self.finished.clear();
    }

    /// Hands out the next run of positions holding a value, or `None` when none is left.
    ///
    /// Every run a series is cut into shares the one buffer per field, because cutting is a
    /// slice; which run lands in which piece depends on when the caller asks, since rows pushed
    /// later can only be positions past the ones already taken. What never happens is one piece
    /// spanning a position with no value, or an empty position costing any storage.
    pub fn take_run(&mut self, metric: MetricName) -> Result<Option<Series>> {
        let exhausted = self
            .finished
            .front()
            .is_none_or(|finished| finished.runs.is_empty());
        if exhausted && !self.indices.is_empty() {
            self.finalize()?;
        }
        // The oldest batch is the one whose runs come first on the grid.
        let Some(finished) = self.finished.front_mut() else {
            return Ok(None);
        };
        let Some(run) = finished.runs.pop_front() else {
            return Ok(None);
        };
        let cut = Self::cut(&self.schema, &self.grid, &metric, &finished.fields, &run)?;
        if self.finished.front().is_some_and(|f| f.runs.is_empty()) {
            self.finished.pop_front();
        }
        Ok(Some(cut))
    }

    /// Returns every piece the pushed rows make up and resets the builder for the next series.
    ///
    /// One piece per maximal run of consecutive positions holding a value; an empty vector when
    /// no pushed row held one.
    pub fn finish(&mut self, metric: MetricName) -> Result<Vec<Series>> {
        // Rows pushed since the last finalization become one more batch; every batch is cut
        // against its own buffers, so the pieces an earlier `take_run` left behind reach the
        // caller too.
        if !self.indices.is_empty() {
            self.finalize()?;
        }
        self.next = 0;
        let mut pieces = Vec::new();
        // Batches in the order they were pushed, each cut against its own buffers.
        for finished in std::mem::take(&mut self.finished) {
            for run in finished.runs {
                pieces.push(Self::cut(
                    &self.schema,
                    &self.grid,
                    &metric,
                    &finished.fields,
                    &run,
                )?);
            }
        }
        Ok(pieces)
    }

    /// Finalizes the buffered rows into shared field buffers and cuts them into runs.
    fn finalize(&mut self) -> Result<()> {
        let fields = std::mem::take(&mut self.fields);
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
        let runs = runs_of(&self.indices);
        self.indices.clear();
        self.finished.push_back(Finished { fields, runs });
        Ok(())
    }

    /// Cuts `run` out of `fields` as one series on the grid.
    fn cut(
        schema: &Arc<SeriesSchema>,
        grid: &Arc<EvalGrid>,
        metric: &MetricName,
        fields: &SeriesFields,
        run: &Run,
    ) -> Result<Series> {
        // Slicing offsets into the shared buffers, so a piece costs a descriptor, not a copy.
        let fields = if fields.len() == 1 {
            fields.as_slice()[0]
                .slice(run.offset, run.len)
                .map(SeriesFields::one)
                .map_err(external)?
        } else {
            let sliced = fields
                .iter()
                .map(|field| field.slice(run.offset, run.len).map_err(external))
                .collect::<Result<Vec<_>>>()?;
            SeriesFields::try_from_vec(sliced).map_err(external)?
        };
        let time = GridRange::try_new(grid.clone(), run.start, run.len).map_err(external)?;
        Series::try_new(schema, metric.clone(), TimeAxis::Grid(time), fields).map_err(external)
    }
}

/// Cuts the retained indices into maximal runs of consecutive positions.
fn runs_of(indices: &[usize]) -> VecDeque<Run> {
    let mut runs = VecDeque::new();
    let mut offset = 0;
    while let Some(&start) = indices.get(offset) {
        let mut len = 1;
        while indices.get(offset + len) == Some(&(start + len)) {
            len += 1;
        }
        runs.push_back(Run { start, offset, len });
        offset += len;
    }
    runs
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
    let mut rows = Vec::new();
    let mut values = vec![None; schema.fields.len()];
    let mut merged = vec![None; schema.fields.len()];
    for (labels, members) in groups {
        if let [member] = members[..] {
            output.extend(series[member].take());
            continue;
        }
        rows.clear();
        for &member in &members {
            let member_series = series[member]
                .as_ref()
                .expect("each series is in one group");
            let TimeAxis::Grid(range) = &member_series.time else {
                return Err(DataFusionError::Execution(
                    "merging requires evaluated series".into(),
                ));
            };
            for row in 0..range.len() {
                if read_row(member_series, row, &mut values)? {
                    rows.push((range.start() + row, member, row));
                }
            }
        }
        rows.sort_unstable_by_key(|&(point, _, _)| point);
        let mut start = 0;
        while start < rows.len() {
            let point = rows[start].0;
            let end = start + rows[start..].partition_point(|&(p, _, _)| p == point);
            // Rows of one point merge field by field; only a field present twice collides.
            merged.iter_mut().for_each(|value| *value = None);
            for &(_, member, row) in &rows[start..end] {
                read_row(series[member].as_ref().unwrap(), row, &mut values)?;
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

/// Reads the float fields of `row`; returns whether any is present.
fn read_row(series: &Series, row: usize, values: &mut [Option<f64>]) -> Result<bool> {
    let mut present = false;
    for (value, samples) in values.iter_mut().zip(series.fields.iter()) {
        *value = match samples.sample(row) {
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
