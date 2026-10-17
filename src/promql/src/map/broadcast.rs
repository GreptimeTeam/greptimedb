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
use datafusion_expr::ColumnarValue;
use datatypes::arrow::array::Float64Array;
use datatypes::arrow::compute;
use datatypes::series::samples::{
    DenseSamplesRef, FieldSpec, Samples, Series, SeriesFields, SeriesSchema,
};
use datatypes::series::time::{GridRange, TimeAxis};

use crate::map::{ArithmeticOp, Comparison, apply_power, series_error};

/// A time-varying, unlabelled single-field scalar, with explicit operand direction.
#[derive(Clone, Copy, Debug)]
pub enum ScalarSeriesOperand<'a> {
    Left(&'a Series),
    Right(&'a Series),
}

/// Broadcasts a time-varying scalar across vector fields, aligned by absolute grid index.
///
/// Only the intersection of actual ranges is produced; missing operands stay missing. Each
/// input retains its own chunk boundaries, and processing splits at either side's boundaries.
/// The scalar must be unlabelled and single-field on the same grid; histogram/mixed/raw inputs
/// are rejected even outside the intersection. Two unlabelled single-field inputs also support
/// scalar–scalar evaluation. No vector matching, full-grid expansion or fallback is performed.
pub fn map_arithmetic_series(
    schema: &SeriesSchema,
    vector: &Series,
    op: ArithmeticOp,
    scalar: ScalarSeriesOperand<'_>,
) -> Result<Series> {
    let mut output = map_grid_scalar(schema, vector, scalar, |vector, samples| match scalar {
        ScalarSeriesOperand::Left(_) => op.apply_arrays(samples, vector),
        ScalarSeriesOperand::Right(_) => op.apply_arrays(vector, samples),
    })?;
    output.metric.remove("__name__");
    Ok(output)
}

/// Compares against a time-varying scalar. Filter retains vector samples/name; bool drops name.
///
/// Shares [`map_arithmetic_series`]'s alignment contract and the ordinary path's rejection of
/// non-bool multi-field filters. False filter results are nulls at their original grid positions.
pub fn map_compare_series(
    schema: &SeriesSchema,
    vector: &Series,
    comparison: Comparison,
    scalar: ScalarSeriesOperand<'_>,
) -> Result<Series> {
    if matches!(comparison, Comparison::Filter(_)) && vector.fields.len() != 1 {
        return Err(DataFusionError::NotImplemented(
            "filter on multi-value input".into(),
        ));
    }
    let mut output = map_grid_scalar(schema, vector, scalar, |vector, samples| {
        Ok(compare_arrays(vector, samples, comparison, scalar))
    })?;
    if matches!(comparison, Comparison::Bool(_)) {
        output.metric.remove("__name__");
    }
    Ok(output)
}

fn map_grid_scalar(
    schema: &SeriesSchema,
    vector: &Series,
    scalar: ScalarSeriesOperand<'_>,
    calculate: impl Fn(&Float64Array, &Float64Array) -> Result<Float64Array>,
) -> Result<Series> {
    let vector_range = validate_series(schema, vector)?;
    let scalar = match scalar {
        ScalarSeriesOperand::Left(s) | ScalarSeriesOperand::Right(s) => s,
    };
    let scalar_range = validate_scalar(schema, scalar)?;
    let start = vector_range.start().max(scalar_range.start());
    let end =
        (vector_range.start() + vector_range.len()).min(scalar_range.start() + scalar_range.len());
    let range = GridRange::try_new(
        vector_range.grid().clone(),
        start,
        end.saturating_sub(start),
    )
    .map_err(series_error)?;
    let fields = if range.is_empty() {
        vector
            .fields
            .iter()
            .map(|_| Samples::from_float(Float64Array::new_null(0)))
            .collect()
    } else {
        let scalar_samples = scalar.fields.as_slice()[0]
            .slice(start - scalar_range.start(), range.len())
            .map_err(series_error)?;
        vector
            .fields
            .iter()
            .map(|field| {
                let field = field
                    .slice(start - vector_range.start(), range.len())
                    .map_err(series_error)?;
                map_paired_chunks(&field, &scalar_samples, &calculate)
            })
            .collect::<Result<Vec<_>>>()?
    };
    Series::try_new(
        schema,
        vector.metric.clone(),
        TimeAxis::Grid(range),
        SeriesFields::try_from_vec(fields).map_err(series_error)?,
    )
    .map_err(series_error)
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
        for segment in field.segments() {
            float_segment(segment)?;
        }
    }
    Ok(range)
}

fn float_segment(samples: &Samples) -> Result<&Float64Array> {
    match samples.dense() {
        Some(DenseSamplesRef::Float(floats)) => Ok(floats),
        _ => Err(DataFusionError::NotImplemented(
            "Map supports only float samples".into(),
        )),
    }
}

fn map_paired_chunks(
    vector: &Samples,
    scalar: &Samples,
    calculate: &impl Fn(&Float64Array, &Float64Array) -> Result<Float64Array>,
) -> Result<Samples> {
    let (mut vector_chunk, mut scalar_chunk, mut vector_offset, mut scalar_offset) = (0, 0, 0, 0);
    let mut chunks = Vec::new();
    while let (Some(v), Some(s)) = (
        vector.segments().get(vector_chunk),
        scalar.segments().get(scalar_chunk),
    ) {
        let (v, s) = (float_segment(v)?, float_segment(s)?);
        let len = (v.len() - vector_offset).min(s.len() - scalar_offset);
        chunks.push(Samples::from_float(calculate(
            &v.slice(vector_offset, len),
            &s.slice(scalar_offset, len),
        )?));
        vector_offset += len;
        scalar_offset += len;
        if vector_offset == v.len() {
            vector_chunk += 1;
            vector_offset = 0;
        }
        if scalar_offset == s.len() {
            scalar_chunk += 1;
            scalar_offset = 0;
        }
    }
    Samples::try_from_chunks(chunks).map_err(series_error)
}

impl ArithmeticOp {
    fn apply_arrays(self, left: &Float64Array, right: &Float64Array) -> Result<Float64Array> {
        Ok(match self {
            Self::Add => compute::binary(left, right, |l, r| l + r)?,
            Self::Subtract => compute::binary(left, right, |l, r| l - r)?,
            Self::Multiply => compute::binary(left, right, |l, r| l * r)?,
            Self::Divide => compute::binary(left, right, |l, r| l / r)?,
            Self::Modulo => compute::binary(left, right, |l, r| l % r)?,
            Self::Atan2 => compute::binary(left, right, f64::atan2)?,
            Self::Power => {
                return apply_power(
                    vec![
                        ColumnarValue::Array(Arc::new(left.clone())),
                        ColumnarValue::Array(Arc::new(right.clone())),
                    ],
                    left.len(),
                );
            }
        })
    }
}

fn compare_arrays(
    vector: &Float64Array,
    samples: &Float64Array,
    comparison: Comparison,
    scalar: ScalarSeriesOperand<'_>,
) -> Float64Array {
    vector
        .iter()
        .zip(samples)
        .map(|(vector, sample)| {
            let vector = vector?;
            let sample = sample?;
            let (left, right) = match scalar {
                ScalarSeriesOperand::Left(_) => (sample, vector),
                ScalarSeriesOperand::Right(_) => (vector, sample),
            };
            match comparison {
                Comparison::Bool(op) => Some(f64::from(op.apply(left, right))),
                Comparison::Filter(op) => op.apply(left, right).then_some(vector),
            }
        })
        .collect()
}
