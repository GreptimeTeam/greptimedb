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
    HistogramSamples, SampleRef, Samples, Series, SeriesFields, SeriesSchema,
};
use datatypes::series::time::{EvalGrid, GridRange, TimeAxis};

use crate::histogram_eval::float_histogram::FloatHistogram;

#[cfg(test)]
pub(crate) mod reference;
#[cfg(test)]
mod tests;

/// Builds one series on the schema's evaluation grid from rows pushed at absolute grid indices.
///
/// The output starts at the first row with a valid value in any field and ends at the last such
/// row, so a short-lived series never holds values for the whole grid. Missing rows between
/// valid ones become nulls; NaN is a valid value. A caller that has decided positions hold no
/// value widens the span with [`GridSeriesBuilder::cover_from`] and
/// [`GridSeriesBuilder::cover_through`], which is how a decided-but-empty position reaches a
/// consumer instead of being trimmed away.
pub struct GridSeriesBuilder {
    schema: Arc<SeriesSchema>,
    grid: Arc<EvalGrid>,
    /// Absolute index of the first valid row.
    start: Option<usize>,
    /// Smallest index the next push may use.
    next: usize,
    /// Index after the last valid row; rows from here to the next valid row are pending nulls.
    valid_end: usize,
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
            start: None,
            next: 0,
            valid_end: 0,
            fields: Vec::new(),
        })
    }

    /// Pushes the field values at absolute grid `index`; `None` is a missing value.
    ///
    /// Indices must be strictly increasing; skipped indices are missing rows.
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

    /// Checks the row and pads the gap before it; returns whether the row has a value to write.
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
            return Ok(false);
        }

        if self.start.is_none() {
            self.start = Some(index);
            self.fields = (0..len)
                .map(|_| FieldBuilder {
                    values: Vec::new(),
                    nulls: NullBufferBuilder::new(0),
                    histograms: None,
                })
                .collect();
        } else {
            let gap = index - self.valid_end;
            for field in &mut self.fields {
                field.values.resize(field.values.len() + gap, 0.0);
                field.nulls.append_n_nulls(gap);
                if let Some(column) = &mut field.histograms {
                    column.indices.resize(column.indices.len() + gap, None);
                }
            }
        }
        self.valid_end = index + 1;
        Ok(true)
    }

    /// Declares that the output span begins at `index`, before any value is pushed for it.
    ///
    /// A caller that has already decided the positions before its first value hold no value uses
    /// this, so the span covers them: a span beginning at the first value would report those
    /// positions as ones nothing is known about, and a consumer would never learn they were
    /// decided. Positions must still be pushed in increasing order from here.
    pub fn cover_from(&mut self, index: usize) -> Result<()> {
        if self.start.is_some() || index == self.grid.timestamps().len() {
            return Ok(());
        }
        if index < self.next {
            return Err(DataFusionError::Execution(format!(
                "grid series cannot begin its span at {index}, behind the next row {}",
                self.next
            )));
        }
        self.start = Some(index);
        self.valid_end = index;
        self.next = index;
        self.fields = (0..self.schema.fields.len())
            .map(|_| FieldBuilder {
                values: Vec::new(),
                nulls: NullBufferBuilder::new(0),
                histograms: None,
            })
            .collect();
        Ok(())
    }

    /// Extends the output span to `end`, writing a missing value for every position in between.
    ///
    /// The counterpart of [`Self::cover_from`] for positions decided after the last value.
    pub fn cover_through(&mut self, end: usize) -> Result<()> {
        if self.start.is_none() {
            return Ok(());
        }
        if end < self.valid_end {
            return Err(DataFusionError::Execution(format!(
                "grid series cannot cover through {end}, behind its last row {}",
                self.valid_end
            )));
        }
        let gap = end - self.valid_end;
        for field in &mut self.fields {
            field.values.resize(field.values.len() + gap, 0.0);
            field.nulls.append_n_nulls(gap);
            if let Some(column) = &mut field.histograms {
                column.indices.resize(column.indices.len() + gap, None);
            }
        }
        self.valid_end = end;
        self.next = end;
        Ok(())
    }

    /// Finishes the current series and resets the builder for the next one.
    ///
    /// Returns `None` when no pushed row had a valid value.
    pub fn finish(&mut self, metric: MetricName) -> Result<Option<Series>> {
        let start = self.start.take();
        let fields = std::mem::take(&mut self.fields);
        let valid_end = std::mem::take(&mut self.valid_end);
        self.next = 0;
        let Some(start) = start else {
            return Ok(None);
        };

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
        let time =
            GridRange::try_new(self.grid.clone(), start, valid_end - start).map_err(external)?;
        let fields = SeriesFields::try_from_vec(samples).map_err(external)?;
        Series::try_new(&self.schema, metric, TimeAxis::Grid(time), fields)
            .map(Some)
            .map_err(external)
    }
}

impl FieldBuilder {
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
