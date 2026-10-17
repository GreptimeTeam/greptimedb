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

//! Local float aggregation over evaluated series. State is keyed by absolute grid index,
//! never by local row number; sparse state and trimmed output do not expand the full grid.

#[cfg(test)]
mod tests;

use std::collections::BTreeMap;
use std::sync::Arc;

use datafusion::error::{DataFusionError, Result};
use datatypes::arrow::array::Float64Builder;
use datatypes::series::MetricName;
use datatypes::series::samples::{
    DenseSamplesRef, SampleRef, Samples, Series, SeriesFields, SeriesSchema,
};
use datatypes::series::time::{EvalGrid, GridRange, TimeAxis};

/// Basic float aggregations, with ordinary-path null and NaN semantics.
#[derive(Clone, Copy, Debug)]
pub enum AggregateOp {
    Sum,
    Count,
    Avg,
    Min,
    Max,
    Group,
}

/// Grouping by static series labels. `All` is the absence of a modifier, not `by()`.
#[derive(Clone, Copy, Debug)]
pub enum Grouping<'a> {
    All,
    By(&'a [&'a str]),
    Without(&'a [&'a str]),
}

/// Aggregates each field at each absolute evaluation instant, retaining interior missing points.
///
/// Empty/all-missing groups are omitted. A point exists if any input field is present: `count`
/// then produces zero for an all-null field, and `group` produces one. Other aggregates leave
/// that field null. Ordinary NaNs count as values, never as missing/stale markers.
///
/// Only float evaluated inputs on the shared schema/grid are admitted. `by` on a value field
/// needs per-point labels and an output schema adapter, and is explicitly rejected. Inputs with
/// different metric names are also rejected: main's name fallback is expression-wide, not a
/// grouping key. Admission must keep these cases on the ordinary path; there is no fallback here.
pub fn aggregate_float(
    schema: &SeriesSchema,
    input: &[Series],
    op: AggregateOp,
    grouping: Grouping<'_>,
) -> Result<Vec<Series>> {
    let grid = validate_input(schema, input, grouping)?;
    let mut groups = BTreeMap::new();
    for series in input {
        let metric = grouping.apply(&series.metric);
        let group = groups.entry(label_key(&metric)).or_insert_with(|| Group {
            metric,
            points: BTreeMap::new(),
        });
        group.accumulate(series, op)?;
    }
    groups
        .into_values()
        .filter_map(|group| group.finish(schema, &grid, op).transpose())
        .collect()
}

/// Schema validation is repeated because Series exposes its fields publicly.
fn validate_input(
    schema: &SeriesSchema,
    input: &[Series],
    grouping: Grouping<'_>,
) -> Result<Arc<EvalGrid>> {
    let grid = schema
        .eval_grid
        .clone()
        .ok_or_else(|| DataFusionError::Execution("Aggregate requires an evaluated grid".into()))?;
    grouping.validate(schema)?;
    for series in input {
        validate_series(schema, series)?;
    }
    if input
        .windows(2)
        .any(|pair| pair[0].metric.name() != pair[1].metric.name())
    {
        return Err(DataFusionError::NotImplemented(
            "aggregate over different metric names requires expression-level name handling".into(),
        ));
    }
    Ok(grid)
}

fn validate_series(schema: &SeriesSchema, series: &Series) -> Result<()> {
    Series::try_new(
        schema,
        series.metric.clone(),
        series.time.clone(),
        series.fields.clone(),
    )
    .map_err(series_error)?;
    validate_float_fields(series)
}

fn validate_float_fields(series: &Series) -> Result<()> {
    for field in series.fields.iter() {
        if field
            .segments()
            .iter()
            .any(|segment| !matches!(segment.dense(), Some(DenseSamplesRef::Float(_))))
        {
            return Err(DataFusionError::NotImplemented(
                "Aggregate supports only float samples".into(),
            ));
        }
    }
    Ok(())
}

impl Grouping<'_> {
    fn validate(self, schema: &SeriesSchema) -> Result<()> {
        if let Self::By(labels) = self
            && schema
                .fields
                .iter()
                .any(|field| labels.contains(&field.name.as_ref()))
        {
            return Err(DataFusionError::NotImplemented(
                "aggregate by value field requires a per-point label/schema adapter".into(),
            ));
        }
        Ok(())
    }

    fn apply(self, input: &MetricName) -> MetricName {
        let mut metric = input.clone();
        match self {
            // Main preserves the single input metric name for a modifier-free aggregate and
            // `without` unless explicitly removed. `by()` differs from no modifier.
            Self::All => metric.keep_only(&["__name__"]),
            Self::By(labels) => metric.keep_only(labels),
            Self::Without(labels) => metric.remove_all(labels),
        }
        metric
    }
}

fn label_key(metric: &MetricName) -> Vec<(Arc<str>, Arc<str>)> {
    let mut labels = Vec::with_capacity(metric.tags().len() + 1);
    if let Some(name) = metric.name() {
        labels.push((Arc::from("__name__"), Arc::from(name)));
    }
    labels.extend(
        metric
            .tags()
            .iter()
            .map(|label| (label.name.clone(), label.value.clone())),
    );
    labels
}

struct Group {
    metric: MetricName,
    points: BTreeMap<usize, Vec<FloatState>>,
}

impl Group {
    fn accumulate(&mut self, series: &Series, op: AggregateOp) -> Result<()> {
        let TimeAxis::Grid(range) = &series.time else {
            return Err(DataFusionError::Execution(
                "Aggregate requires an evaluated grid".into(),
            ));
        };
        for (field_index, field) in series.fields.iter().enumerate() {
            for (offset, sample) in field.iter().enumerate() {
                if let SampleRef::Float(sample) = sample {
                    // Allocating a point on the first present field tracks row existence
                    // separately from the validity/count of each other field.
                    let states = self
                        .points
                        .entry(range.start() + offset)
                        .or_insert_with(|| vec![FloatState::new(op); series.fields.len()]);
                    states[field_index].update(op, sample);
                }
            }
        }
        Ok(())
    }

    fn finish(
        self,
        schema: &SeriesSchema,
        grid: &Arc<EvalGrid>,
        op: AggregateOp,
    ) -> Result<Option<Series>> {
        let Some(((&start, _), (&last, _))) = self
            .points
            .first_key_value()
            .zip(self.points.last_key_value())
        else {
            return Ok(None);
        };
        let end = last + 1;
        let fields = (0..schema.fields.len())
            .map(|field| self.finish_field(field, start..end, op))
            .collect();
        let range = GridRange::try_new(grid.clone(), start, end - start).map_err(series_error)?;
        Series::try_new(
            schema,
            self.metric,
            TimeAxis::Grid(range),
            SeriesFields::try_from_vec(fields).map_err(series_error)?,
        )
        .map(Some)
        .map_err(series_error)
    }

    fn finish_field(
        &self,
        field: usize,
        range: std::ops::Range<usize>,
        op: AggregateOp,
    ) -> Samples {
        let mut builder = Float64Builder::with_capacity(range.len());
        let mut next = range.start;
        for (&point, states) in &self.points {
            builder.append_nulls(point - next);
            builder.append_option(states[field].finish(op));
            next = point + 1;
        }
        Samples::from_float(builder.finish())
    }
}

#[derive(Clone, Copy)]
struct FloatState {
    count: u64,
    aggregate: f64,
}

impl FloatState {
    fn new(op: AggregateOp) -> Self {
        Self {
            count: 0,
            aggregate: if matches!(op, AggregateOp::Min | AggregateOp::Max) {
                f64::NAN
            } else {
                0.0
            },
        }
    }

    fn update(&mut self, op: AggregateOp, sample: f64) {
        self.count += 1;
        match op {
            AggregateOp::Sum | AggregateOp::Avg => self.aggregate += sample,
            AggregateOp::Min if self.aggregate.is_nan() || sample < self.aggregate => {
                self.aggregate = sample
            }
            AggregateOp::Max if self.aggregate.is_nan() || sample > self.aggregate => {
                self.aggregate = sample
            }
            _ => {}
        }
    }

    fn finish(self, op: AggregateOp) -> Option<f64> {
        match op {
            AggregateOp::Count => Some(self.count as f64),
            AggregateOp::Group => Some(1.0),
            _ if self.count == 0 => None,
            AggregateOp::Avg => Some(self.aggregate / self.count as f64),
            _ => Some(self.aggregate),
        }
    }
}

fn series_error(error: datatypes::error::Error) -> DataFusionError {
    DataFusionError::External(Box::new(error))
}
