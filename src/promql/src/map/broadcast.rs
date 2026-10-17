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

use datafusion::error::{DataFusionError, Result};
use datatypes::series::MetricName;
use datatypes::series::samples::{
    DenseSamplesRef, FieldSpec, SampleRef, Samples, Series, SeriesSchema,
};
use datatypes::series::time::{GridRange, TimeAxis};

use crate::grid_series::GridSeriesBuilder;
use crate::map::{ArithmeticOp, Comparison, drop_metric_name, series_error};

/// A time-varying, unlabelled single-field scalar, with explicit operand direction.
#[derive(Clone, Copy, Debug)]
pub enum ScalarSeriesOperand<'a> {
    Left(&'a Series),
    Right(&'a Series),
}

/// Applies arithmetic between vector fields and a time-varying scalar, aligned by absolute grid
/// index, and drops the metric name.
///
/// Prometheus scalars exist at every step; a missing scalar point here yields a missing output.
/// The output is trimmed to its first and last valid rows, and `None` means no valid row. The
/// scalar must be unlabelled and single-field on the same grid; histogram/mixed/raw inputs are
/// rejected even outside the overlap. Two unlabelled single-field inputs also evaluate
/// scalar–scalar operations. Same-labelset merging is left to the caller, as in
/// [`crate::map::map_math`].
pub fn map_arithmetic_series(
    schema: &Arc<SeriesSchema>,
    vector: &Series,
    op: ArithmeticOp,
    scalar: ScalarSeriesOperand<'_>,
) -> Result<Option<Series>> {
    let mut metric = vector.metric.clone();
    drop_metric_name(&mut metric);
    map_grid_scalar(schema, vector, scalar, metric, |left, right, _| {
        Some(op.calculate(left, right))
    })
}

/// Compares against a time-varying scalar. Filter keeps vector values and labels; bool emits
/// 0/1 and drops the metric name.
///
/// Shares [`map_arithmetic_series`]'s alignment and trimming contract. Each field is compared
/// independently; a row is kept where any field passes a filter.
pub fn map_compare_series(
    schema: &Arc<SeriesSchema>,
    vector: &Series,
    comparison: Comparison,
    scalar: ScalarSeriesOperand<'_>,
) -> Result<Option<Series>> {
    let mut metric = vector.metric.clone();
    if matches!(comparison, Comparison::Bool(_)) {
        drop_metric_name(&mut metric);
    }
    map_grid_scalar(schema, vector, scalar, metric, |left, right, sample| {
        comparison.apply(left, right, sample)
    })
}

/// Calls `calculate(left, right, vector_sample)` at each index where both operands exist.
fn map_grid_scalar(
    schema: &Arc<SeriesSchema>,
    vector: &Series,
    scalar: ScalarSeriesOperand<'_>,
    metric: MetricName,
    calculate: impl Fn(f64, f64, f64) -> Option<f64>,
) -> Result<Option<Series>> {
    let vector_range = validate_series(schema, vector)?;
    let (scalar_series, scalar_left) = match scalar {
        ScalarSeriesOperand::Left(s) => (s, true),
        ScalarSeriesOperand::Right(s) => (s, false),
    };
    let scalar_range = validate_scalar(schema, scalar_series)?;
    let start = vector_range.start().max(scalar_range.start());
    let end =
        (vector_range.start() + vector_range.len()).min(scalar_range.start() + scalar_range.len());
    let mut builder = GridSeriesBuilder::try_new(schema.clone())?;
    if start >= end {
        return builder.finish(metric);
    }
    let len = end - start;
    let scalar_samples = scalar_series.fields.as_slice()[0]
        .slice(start - scalar_range.start(), len)
        .map_err(series_error)?;
    let fields = vector
        .fields
        .iter()
        .map(|field| field.slice(start - vector_range.start(), len))
        .collect::<std::result::Result<Vec<_>, _>>()
        .map_err(series_error)?;
    let mut fields = fields.iter().map(Samples::iter).collect::<Vec<_>>();
    let mut row = Vec::with_capacity(fields.len());
    for (offset, scalar) in scalar_samples.iter().enumerate() {
        let scalar = float_or_missing(scalar)?;
        row.clear();
        for field in &mut fields {
            let sample = float_or_missing(field.next().expect("sliced to the same length"))?;
            row.push(scalar.zip(sample).and_then(|(scalar, sample)| {
                if scalar_left {
                    calculate(scalar, sample, sample)
                } else {
                    calculate(sample, scalar, sample)
                }
            }));
        }
        builder.push(start + offset, &row)?;
    }
    builder.finish(metric)
}

fn float_or_missing(sample: SampleRef<'_>) -> Result<Option<f64>> {
    match sample {
        SampleRef::Float(sample) => Ok(Some(sample)),
        SampleRef::Missing => Ok(None),
        _ => Err(not_float()),
    }
}

fn validate_scalar<'a>(schema: &SeriesSchema, scalar: &'a Series) -> Result<&'a GridRange> {
    if scalar.metric.name().is_some() || !scalar.metric.tags().is_empty() {
        return Err(DataFusionError::Execution(
            "grid scalar must be unlabelled".into(),
        ));
    }
    let schema = SeriesSchema {
        fields: vec![FieldSpec {
            name: "scalar".into(),
        }]
        .into(),
        eval_grid: schema.eval_grid.clone(),
    };
    validate_series(&schema, scalar)
}

fn validate_series<'a>(schema: &SeriesSchema, series: &'a Series) -> Result<&'a GridRange> {
    let TimeAxis::Grid(range) = &series.time else {
        return Err(DataFusionError::Execution(
            "Map requires an evaluated grid time axis".into(),
        ));
    };
    Series::try_new(
        schema,
        series.metric.clone(),
        series.time.clone(),
        series.fields.clone(),
    )
    .map_err(series_error)?;
    for field in series.fields.iter() {
        if field
            .segments()
            .iter()
            .any(|segment| !matches!(segment.dense(), Some(DenseSamplesRef::Float(_))))
        {
            return Err(not_float());
        }
    }
    Ok(range)
}

fn not_float() -> DataFusionError {
    DataFusionError::NotImplemented("Map supports only float samples".into())
}
