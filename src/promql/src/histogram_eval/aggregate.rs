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

//! `sum` and `avg` of native histograms within one aggregation group, following the histogram
//! paths of Prometheus `aggregation`.

use std::sync::Arc;

use datafusion::arrow::array::UInt32Array;
use datafusion::error::{DataFusionError, Result};
use datatypes::series::MetricName;
use datatypes::series::histogram::HistogramBatchBuilder;
use datatypes::series::samples::{SampleRef, Samples, Series, SeriesFields, SeriesSchema};
use datatypes::series::time::{GridRange, TimeAxis};

use super::float_histogram::FloatHistogram;
use super::grid_range;

/// A native histogram aggregation.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum HistogramAggregate {
    Sum,
    Avg,
}

/// Aggregates the native histograms of one group at every point, adding them in the order of
/// `group`. A point yields no histogram when the group also has a float sample there, or when
/// its histograms mix exponential and custom buckets. Float-only points are left to the float
/// aggregation. The result carries `metric`.
pub fn aggregate_histograms(
    op: HistogramAggregate,
    schema: &Arc<SeriesSchema>,
    group: &[Series],
    metric: MetricName,
) -> Result<Option<Series>> {
    let grid = schema.eval_grid.clone().ok_or_else(|| {
        DataFusionError::Execution("histogram aggregation requires an evaluation grid".into())
    })?;
    let ranges = group.iter().map(grid_range).collect::<Result<Vec<_>>>()?;
    let start = ranges.iter().map(|range| range.0).min().unwrap_or(0);
    let end = ranges.iter().map(|range| range.1).max().unwrap_or(0);

    let mut points: Vec<Vec<Option<FloatHistogram>>> = Vec::with_capacity(schema.fields.len());
    for field in 0..schema.fields.len() {
        let mut values = Vec::with_capacity(end - start);
        for point in start..end {
            let samples = group
                .iter()
                .zip(&ranges)
                .filter(|(_, (from, to))| (*from..*to).contains(&point))
                .map(|(series, &(from, _))| series.fields.as_slice()[field].sample(point - from));
            values.push(aggregate_point(op, samples)?);
        }
        points.push(values);
    }

    let Some(first) = (0..end - start).find(|&i| points.iter().any(|field| field[i].is_some()))
    else {
        return Ok(None);
    };
    let last = (0..end - start)
        .rfind(|&i| points.iter().any(|field| field[i].is_some()))
        .unwrap_or(first);
    let mut fields = Vec::with_capacity(points.len());
    for values in points {
        let mut builder = HistogramBatchBuilder::new();
        let mut indices = Vec::with_capacity(last + 1 - first);
        for value in &values[first..=last] {
            indices.push(match value {
                Some(h) => Some(h.push_to(&mut builder)?),
                None => None,
            });
        }
        let batch = builder.finish().map_err(external)?;
        fields.push(
            Samples::try_from_histogram(UInt32Array::from(indices), batch).map_err(external)?,
        );
    }
    let range = GridRange::try_new(grid, start + first, last + 1 - first).map_err(external)?;
    Series::try_new(
        schema,
        metric,
        TimeAxis::Grid(range),
        SeriesFields::try_from_vec(fields).map_err(external)?,
    )
    .map(Some)
    .map_err(external)
}

/// One point of one field.
fn aggregate_point<'a>(
    op: HistogramAggregate,
    samples: impl Iterator<Item = SampleRef<'a>>,
) -> Result<Option<FloatHistogram>> {
    let mut value: Option<FloatHistogram> = None;
    let mut compensation: Option<FloatHistogram> = None;
    let mut mean: Option<FloatHistogram> = None;
    let (mut has_float, mut incompatible) = (false, false);
    let mut count = 0.0;
    for sample in samples {
        let h = match sample {
            SampleRef::Missing => continue,
            SampleRef::Float(_) => {
                has_float = true;
                continue;
            }
            SampleRef::Histogram(view) => FloatHistogram::try_from_view(&view)?,
        };
        count += 1.0;
        let Some(current) = value.as_mut() else {
            value = Some(h);
            continue;
        };
        if incompatible {
            continue;
        }
        match op {
            HistogramAggregate::Sum => match current.kahan_add(&h, compensation.take()) {
                Ok((c, _)) => compensation = Some(c),
                Err(_) => incompatible = true,
            },
            HistogramAggregate::Avg => {
                if mean.is_none() {
                    let mut sum = current.clone();
                    match sum.kahan_add(&h, compensation.clone()) {
                        Ok((c, _)) if !sum.has_overflow() => {
                            *current = sum;
                            compensation = Some(c);
                            continue;
                        }
                        Ok(_) => {
                            let mut started = current.clone();
                            started.div(count - 1.0);
                            if let Some(c) = compensation.as_mut() {
                                c.div(count - 1.0);
                            }
                            mean = Some(started);
                        }
                        Err(_) => {
                            incompatible = true;
                            continue;
                        }
                    }
                }
                // Go switches to an incremental mean once the plain sum overflows.
                let q = (count - 1.0) / count;
                if let Some(c) = compensation.as_mut() {
                    c.mul(q);
                }
                let mut to_add = h;
                to_add.div(count);
                let mean = mean.as_mut().expect("set above");
                mean.mul(q);
                match mean.kahan_add(&to_add, compensation.take()) {
                    Ok((c, _)) => compensation = Some(c),
                    Err(_) => incompatible = true,
                }
            }
        }
    }
    let Some(mut value) = value else {
        return Ok(None);
    };
    if has_float || incompatible {
        return Ok(None);
    }
    match op {
        HistogramAggregate::Sum => {
            if let Some(c) = compensation {
                value.add(&c)?;
            }
        }
        HistogramAggregate::Avg => match mean {
            Some(mut mean) => {
                if let Some(c) = compensation {
                    mean.add(&c)?;
                }
                value = mean;
            }
            None => {
                value.div(count);
                if let Some(mut c) = compensation {
                    c.div(count);
                    value.add(&c)?;
                }
            }
        },
    }
    value.compact();
    Ok(Some(value))
}

fn external(error: datatypes::error::Error) -> DataFusionError {
    DataFusionError::External(Box::new(error))
}
