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
use datatypes::series::SeriesLabels;
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

/// Applies arithmetic between vector fields and a time-varying scalar on the same grid, and drops
/// the metric name.
///
/// Only positions both operands carry are evaluated; a missing scalar point yields a missing
/// output, and `None` means no output row. The scalar must be unlabelled and single-field, and both
/// inputs must hold floats only, even outside the overlap. Two scalars also work. Same-labelset
/// merging is left to the caller.
pub fn map_arithmetic_series(
    schema: &Arc<SeriesSchema>,
    vector: &Series,
    op: ArithmeticOp,
    scalar: ScalarSeriesOperand<'_>,
) -> Result<Option<Series>> {
    let metric = drop_metric_name(&vector.metric)?;
    let (output, _) = map_grid_scalar(
        schema,
        vector,
        scalar,
        metric,
        NonFloatSample::Impossible,
        |left, right, _| Some(op.calculate(left, right)),
    )?;
    Ok(output)
}

/// Compares against a time-varying scalar. Filter keeps vector values and labels; bool emits
/// 0/1 and drops the metric name.
///
/// Aligns like [`map_arithmetic_series`]. Each field is compared independently; a row is kept
/// where any field passes. Histogram samples are dropped, as in
/// [`crate::map::map_compare_literal`], and the second element reports whether one was.
pub fn map_compare_series(
    schema: &Arc<SeriesSchema>,
    vector: &Series,
    comparison: Comparison,
    scalar: ScalarSeriesOperand<'_>,
) -> Result<(Option<Series>, bool)> {
    let metric = if matches!(comparison, Comparison::Bool(_)) {
        drop_metric_name(&vector.metric)?
    } else {
        vector.metric.clone()
    };
    map_grid_scalar(
        schema,
        vector,
        scalar,
        metric,
        NonFloatSample::Drop,
        |left, right, sample| comparison.apply(left, right, sample),
    )
}

/// `max_of(a, b)` and `min_of(a, b)`, which Prometheus evaluates with Go's `math.Max` and
/// `math.Min`.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum ScalarFunction {
    MaxOf,
    MinOf,
}

impl ScalarFunction {
    /// Go's `math.Max` / `math.Min`: an infinity of the function's own sign wins over NaN, NaN
    /// over any other value, and `+0` is greater than `-0`. Symmetric in its arguments.
    pub(crate) fn apply(self, a: f64, b: f64) -> f64 {
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

/// A math function whose parameters are time-varying scalars. For `max_of` and `min_of` the
/// mapped series is the first scalar operand.
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
    /// `max_of(a, b)`: the mapped series is `a`, `other` is `b`.
    MaxOf { other: &'a Series },
    /// `min_of(a, b)`: the mapped series is `a`, `other` is `b`.
    MinOf { other: &'a Series },
}

impl<'a> ParameterizedMath<'a> {
    fn parameters(self) -> Vec<&'a Series> {
        match self {
            Self::Round { to_nearest } => vec![to_nearest],
            Self::Clamp { min, max } => vec![min, max],
            Self::ClampMin { min } => vec![min],
            Self::ClampMax { max } => vec![max],
            Self::MaxOf { other } | Self::MinOf { other } => vec![other],
        }
    }

    /// Binds one point's parameter values, given in argument order.
    fn at(self, values: &[f64]) -> PointMath {
        match self {
            Self::Round { .. } => PointMath::Math(MathOp::Round { nearest: values[0] }),
            Self::Clamp { .. } => PointMath::Math(MathOp::Clamp {
                min: values[0],
                max: values[1],
            }),
            Self::ClampMin { .. } => PointMath::Math(MathOp::ClampMin { min: values[0] }),
            Self::ClampMax { .. } => PointMath::Math(MathOp::ClampMax { max: values[0] }),
            Self::MaxOf { .. } => PointMath::Scalar {
                function: ScalarFunction::MaxOf,
                other: values[0],
            },
            Self::MinOf { .. } => PointMath::Scalar {
                function: ScalarFunction::MinOf,
                other: values[0],
            },
        }
    }
}

/// A parameterized math function with one point's parameters bound.
#[derive(Clone, Copy, Debug)]
enum PointMath {
    Math(MathOp),
    Scalar {
        function: ScalarFunction,
        other: f64,
    },
}

impl PointMath {
    /// `None` for a `clamp` with `max < min`, which has no result at that point.
    fn calculate(self, value: f64) -> Option<f64> {
        match self {
            Self::Math(op) => (!op.is_empty_clamp()).then(|| op.calculate(value)),
            Self::Scalar { function, other } => Some(function.apply(value, other)),
        }
    }
}

/// Applies `function` to every float field with parameters read per point, and drops the metric
/// name, like [`crate::map::map_math`] with constant parameters; histogram samples are skipped.
///
/// A point where a parameter has no value, or where `clamp` has `max < min` (Prometheus returns an
/// empty vector for that step), has no result. `None` means no point has one. Same-labelset merging
/// is left to the caller.
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
    // Points only increase, so one forward cursor per parameter suffices.
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
        let op = (values.len() == parameters.len()).then(|| function.at(&values));
        row.clear();
        for cursor in &mut field_cursors {
            let sample = cursor
                .at(index)
                .expect("the axis and the fields are aligned");
            row.push(match (op, sample) {
                (Some(op), SampleRef::Float(value)) => op.calculate(value),
                _ => None,
            });
        }
        builder.push(point, &row)?;
    }
    builder.finish(drop_metric_name(&series.metric)?)
}

/// How [`map_grid_scalar`] treats a histogram sample in the vector.
#[derive(Clone, Copy, Debug)]
enum NonFloatSample {
    /// The vector is validated as float-only.
    Impossible,
    /// The position yields nothing and the drop is reported, as Prometheus does for an operator
    /// undefined on histograms.
    Drop,
}

/// Calls `calculate(left, right, vector_sample)` at each position where both operands exist.
fn map_grid_scalar(
    schema: &Arc<SeriesSchema>,
    vector: &Series,
    scalar: ScalarSeriesOperand<'_>,
    metric: SeriesLabels,
    non_float: NonFloatSample,
    calculate: impl Fn(f64, f64, f64) -> Option<f64>,
) -> Result<(Option<Series>, bool)> {
    let vector_time = match non_float {
        NonFloatSample::Impossible => validate_float_series(schema, vector)?,
        NonFloatSample::Drop => validate_grid_series(schema, vector)?,
    };
    let (scalar_series, scalar_left) = match scalar {
        ScalarSeriesOperand::Left(s) => (s, true),
        ScalarSeriesOperand::Right(s) => (s, false),
    };
    let scalar_time = validate_scalar(schema, scalar_series)?;
    let mut builder = GridSeriesBuilder::try_new(schema.clone())?;
    // Each operand reads its own row from the shared run; positions only one side carries yield
    // nothing, so they are skipped.
    let mut scalar_samples = scalar_series.fields.as_slice()[0].cursor();
    let mut field_samples = vector
        .fields
        .iter()
        .map(|field| field.cursor())
        .collect::<Vec<_>>();
    let mut row = Vec::with_capacity(field_samples.len());
    let mut dropped_histogram = false;
    for pair in vector_time.intersect_runs(scalar_time) {
        for offset in 0..pair.len {
            let scalar = float_sample(
                scalar_samples
                    .at(pair.other_row + offset)
                    .expect("the shared run lies inside the scalar axis"),
            );
            row.clear();
            for field in &mut field_samples {
                let sample = match field
                    .at(pair.row + offset)
                    .expect("the shared run lies inside the vector axis")
                {
                    SampleRef::Float(value) => Some(value),
                    SampleRef::Missing => None,
                    SampleRef::Histogram(_) => {
                        match non_float {
                            NonFloatSample::Drop => dropped_histogram = true,
                            NonFloatSample::Impossible => {
                                unreachable!("validated as float")
                            }
                        }
                        None
                    }
                };
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
    Ok((builder.finish(metric)?, dropped_histogram))
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
