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

//! Building evaluated float series that cover only their result range on a shared grid.

use std::collections::HashMap;
use std::collections::hash_map::Entry;
use std::sync::Arc;

use datafusion::arrow::array::Float64Array;
use datafusion::arrow::array::builder::NullBufferBuilder;
use datafusion::error::{DataFusionError, Result};
use datatypes::series::MetricName;
use datatypes::series::samples::{SampleRef, Samples, Series, SeriesFields, SeriesSchema};
use datatypes::series::time::{EvalGrid, GridRange, TimeAxis};

#[cfg(test)]
pub(crate) mod reference;
#[cfg(test)]
mod tests;

/// Builds one float series on the schema's evaluation grid from rows pushed at absolute grid
/// indices.
///
/// The output starts at the first row with a valid value in any field and ends at the last such
/// row, so a short-lived series never holds values for the whole grid. Missing rows between
/// valid ones become nulls; NaN is a valid value.
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
}

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
        if values.len() != self.schema.fields.len() {
            return Err(DataFusionError::Execution(format!(
                "grid series row has {} fields, schema has {}",
                values.len(),
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
        if values.iter().all(Option::is_none) {
            return Ok(());
        }

        if self.start.is_none() {
            self.start = Some(index);
            self.fields = values
                .iter()
                .map(|_| FieldBuilder {
                    values: Vec::new(),
                    nulls: NullBufferBuilder::new(0),
                })
                .collect();
        } else {
            let gap = index - self.valid_end;
            for field in &mut self.fields {
                field.values.resize(field.values.len() + gap, 0.0);
                field.nulls.append_n_nulls(gap);
            }
        }
        for (field, value) in self.fields.iter_mut().zip(values) {
            field.values.push(value.unwrap_or_default());
            field.nulls.append(value.is_some());
        }
        self.valid_end = index + 1;
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
                Samples::from_float(Float64Array::new(field.values.into(), field.nulls.finish()))
            })
            .collect();
        let time =
            GridRange::try_new(self.grid.clone(), start, valid_end - start).map_err(external)?;
        let fields = SeriesFields::try_from_vec(samples).map_err(external)?;
        Series::try_new(&self.schema, metric, TimeAxis::Grid(time), fields)
            .map(Some)
            .map_err(external)
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
