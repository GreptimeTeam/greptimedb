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
use datatypes::series::samples::{FieldSpec, SampleRef, Series, SeriesSchema};
use datatypes::series::time::TimeAxis;

use crate::grid_series::GridSeriesBuilder;
use crate::map::{
    ArithmeticOp, Comparison, MathOp, drop_metric_name, float_sample, validate_float_series,
    validate_grid_series,
};

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
/// The output covers the valid rows and nothing else, and `None` means there is none. A scalar
/// point holding no value is a hole in the one result series, not the end of a piece: the axis
/// names the runs of consecutive valid positions. The scalar must be unlabelled and single-field
/// on the same grid; histogram/mixed/raw inputs are rejected even outside the overlap. Both
/// operands may skip positions: only the positions both carry are evaluated, and the two axes are
/// aligned by intersecting their runs, never by subtracting one axis' first position from the
/// other's indices. Two unlabelled single-field inputs also evaluate scalar–scalar operations.
/// Same-labelset merging is left to the caller, as in [`crate::map::map_math`].
pub fn map_arithmetic_series(
    schema: &Arc<SeriesSchema>,
    vector: &Series,
    op: ArithmeticOp,
    scalar: ScalarSeriesOperand<'_>,
) -> Result<Option<Series>> {
    let metric = drop_metric_name(&vector.metric)?;
    map_grid_scalar(schema, vector, scalar, metric, |left, right, _| {
        Some(op.calculate(left, right))
    })
}

/// Compares against a time-varying scalar. Filter keeps vector values and labels; bool emits
/// 0/1 and drops the metric name.
///
/// Shares [`map_arithmetic_series`]'s alignment and row-keeping contract. Each field is compared
/// independently; a row is kept where any field passes a filter.
pub fn map_compare_series(
    schema: &Arc<SeriesSchema>,
    vector: &Series,
    comparison: Comparison,
    scalar: ScalarSeriesOperand<'_>,
) -> Result<Option<Series>> {
    let metric = if matches!(comparison, Comparison::Bool(_)) {
        drop_metric_name(&vector.metric)?
    } else {
        vector.metric.clone()
    };
    map_grid_scalar(schema, vector, scalar, metric, |left, right, sample| {
        comparison.apply(left, right, sample)
    })
}

/// `max_of(a, b)` and `min_of(a, b)`, which Prometheus evaluates with Go's `math.Max` and
/// `math.Min`.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ScalarFunction {
    MaxOf,
    MinOf,
}

impl ScalarFunction {
    /// Go's `math.Max` / `math.Min`: an infinity of the function's own sign wins over NaN, NaN
    /// over any other value, and `+0` is greater than `-0`.
    pub fn apply(self, a: f64, b: f64) -> f64 {
        let (wins, signed_zero_wins) = match self {
            Self::MaxOf => (f64::INFINITY, false),
            Self::MinOf => (f64::NEG_INFINITY, true),
        };
        if a == wins || b == wins {
            wins
        } else if a.is_nan() || b.is_nan() {
            f64::NAN
        } else if a == 0.0 && a == b {
            if a.is_sign_negative() == signed_zero_wins {
                a
            } else {
                b
            }
        } else if (a > b) == (self == Self::MaxOf) {
            a
        } else {
            b
        }
    }
}

/// Applies `function` to two time-varying scalars, aligned by absolute grid index; a point where
/// either has no value has no result. Both operands must be unlabelled single-field series.
pub fn map_scalar_function(
    schema: &Arc<SeriesSchema>,
    function: ScalarFunction,
    left: &Series,
    right: &Series,
) -> Result<Option<Series>> {
    validate_scalar(schema, left)?;
    map_grid_scalar(
        schema,
        left,
        ScalarSeriesOperand::Right(right),
        left.metric.clone(),
        |left, right, _| Some(function.apply(left, right)),
    )
}

/// A math function whose parameters are scalar expressions, which Prometheus evaluates at every
/// step, with those parameters as time-varying scalars.
#[derive(Clone, Copy, Debug)]
pub enum ParameterizedMath<'a> {
    /// `round(v, to_nearest)`.
    Round { to_nearest: &'a Series },
    /// `clamp(v, min, max)`.
    Clamp { min: &'a Series, max: &'a Series },
    /// `clamp_min(v, min)`.
    ClampMin { min: &'a Series },
    /// `clamp_max(v, max)`.
    ClampMax { max: &'a Series },
}

impl<'a> ParameterizedMath<'a> {
    /// The parameters in argument order.
    fn parameters(self) -> Vec<&'a Series> {
        match self {
            Self::Round { to_nearest } => vec![to_nearest],
            Self::Clamp { min, max } => vec![min, max],
            Self::ClampMin { min } => vec![min],
            Self::ClampMax { max } => vec![max],
        }
    }

    /// The function with the parameter values of one point, in argument order.
    fn at(self, values: &[f64]) -> MathOp {
        match self {
            Self::Round { .. } => MathOp::Round { nearest: values[0] },
            Self::Clamp { .. } => MathOp::Clamp {
                min: values[0],
                max: values[1],
            },
            Self::ClampMin { .. } => MathOp::ClampMin { min: values[0] },
            Self::ClampMax { .. } => MathOp::ClampMax { max: values[0] },
        }
    }
}

/// Applies `function` to every field with its parameters read at each point from time-varying
/// scalars, and drops the metric name, as [`crate::map::map_math`] does with constant parameters;
/// native histogram samples are skipped the same way.
///
/// A point where a parameter has no value has no result. `clamp` with `max < min` at a point has
/// no result there, as Prometheus returns an empty vector for that step. `None` means no point
/// holds a result. Same-labelset merging is left to the caller.
pub fn map_math_with_parameters(
    schema: &Arc<SeriesSchema>,
    series: &Series,
    function: ParameterizedMath<'_>,
) -> Result<Option<Series>> {
    let time = validate_grid_series(schema, series)?;
    let parameters = function.parameters();
    let mut parameter_axes = Vec::with_capacity(parameters.len());
    for parameter in &parameters {
        parameter_axes.push(validate_scalar(schema, parameter)?);
    }
    // Points only increase, so each parameter's row does too and one cursor reads it.
    let mut parameter_cursors: Vec<_> = parameters
        .iter()
        .map(|parameter| parameter.fields.as_slice()[0].cursor())
        .collect();
    let mut field_cursors: Vec<_> = series.fields.iter().map(|field| field.cursor()).collect();
    let mut values = Vec::with_capacity(parameters.len());
    let mut row = Vec::with_capacity(field_cursors.len());
    let mut builder = GridSeriesBuilder::try_new(schema.clone())?;
    for (point, index) in time.positions() {
        values.clear();
        for (axis, cursor) in parameter_axes.iter().zip(&mut parameter_cursors) {
            let value = axis.row_of(point).and_then(|row| {
                float_sample(cursor.at(row).expect("parameter rows only increase"))
            });
            values.extend(value);
        }
        let op = (values.len() == parameters.len())
            .then(|| function.at(&values))
            .filter(|op| !op.is_empty_clamp());
        row.clear();
        for cursor in &mut field_cursors {
            let sample = cursor
                .at(index)
                .expect("the axis and the fields are aligned");
            row.push(match (op, sample) {
                (Some(op), SampleRef::Float(value)) => Some(op.calculate(value)),
                _ => None,
            });
        }
        builder.push(point, &row)?;
    }
    builder.finish(drop_metric_name(&series.metric)?)
}

/// Calls `calculate(left, right, vector_sample)` at each index where both operands exist.
fn map_grid_scalar(
    schema: &Arc<SeriesSchema>,
    vector: &Series,
    scalar: ScalarSeriesOperand<'_>,
    metric: MetricName,
    calculate: impl Fn(f64, f64, f64) -> Option<f64>,
) -> Result<Option<Series>> {
    let vector_time = validate_float_series(schema, vector)?;
    let (scalar_series, scalar_left) = match scalar {
        ScalarSeriesOperand::Left(s) => (s, true),
        ScalarSeriesOperand::Right(s) => (s, false),
    };
    let scalar_time = validate_scalar(schema, scalar_series)?;
    let mut builder = GridSeriesBuilder::try_new(schema.clone())?;
    // The shared runs carry both compact rows, so each operand reads its own field from the row
    // its own axis assigns to the position. A position only one side carries is missing anyway:
    // the pairing would be missing, and the builder keeps no row for it.
    let mut scalar_samples = scalar_series.fields.as_slice()[0].cursor();
    let mut field_samples = vector
        .fields
        .iter()
        .map(|field| field.cursor())
        .collect::<Vec<_>>();
    let mut row = Vec::with_capacity(field_samples.len());
    for pair in vector_time.intersect_runs(scalar_time) {
        for offset in 0..pair.len {
            let scalar = float_sample(
                scalar_samples
                    .at(pair.other_row + offset)
                    .expect("the shared run lies inside the scalar axis"),
            );
            row.clear();
            for field in &mut field_samples {
                let sample = float_sample(
                    field
                        .at(pair.row + offset)
                        .expect("the shared run lies inside the vector axis"),
                );
                row.push(scalar.zip(sample).and_then(|(scalar, sample)| {
                    if scalar_left {
                        calculate(scalar, sample, sample)
                    } else {
                        calculate(sample, scalar, sample)
                    }
                }));
            }
            builder.push(pair.position + offset, &row)?;
        }
    }
    builder.finish(metric)
}

pub(crate) fn validate_scalar<'a>(
    schema: &SeriesSchema,
    scalar: &'a Series,
) -> Result<&'a TimeAxis> {
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
    validate_float_series(&schema, scalar)
}
