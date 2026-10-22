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

//! Pointwise kernels for evaluated series, independent of planning and execution nodes.
//!
//! Expression column naming and omission of missing points belong to the planner/output adapter;
//! these kernels retain the shared field schema and represent missing grid positions by validity.

mod broadcast;
mod comparison;
mod go_math;
#[cfg(test)]
mod tests;

pub(crate) use broadcast::validate_scalar;
pub use broadcast::{
    ParameterizedMath, ScalarFunction, ScalarSeriesOperand, map_arithmetic_series,
    map_compare_series, map_math_with_parameters, map_scalar_function,
};
pub use comparison::{Comparison, ComparisonOp, map_compare, scalar_arithmetic, scalar_compare};

use std::f64::consts::PI;
use std::sync::Arc;

use datafusion::error::{DataFusionError, Result};
use datatypes::arrow::array::Float64Array;
use datatypes::series::MetricName;
use datatypes::series::samples::{
    DenseSamplesRef, SampleRef, Samples, Series, SeriesFields, SeriesSchema,
};
use datatypes::series::time::{GridRange, TimeAxis};

use crate::functions::Round;
use crate::grid_series::GridSeriesBuilder;

/// Float math supported by the evaluated-series kernel.
#[derive(Clone, Copy, Debug)]
pub enum MathOp {
    /// Absolute value.
    Abs,
    /// Rounds towards positive infinity.
    Ceil,
    /// Rounds towards negative infinity.
    Floor,
    /// IEEE square root, returning NaN for negative input.
    Sqrt,
    /// Rounds to a constant multiple; use `1.0` for PromQL's default argument.
    Round {
        nearest: f64,
    },
    Exp,
    Ln,
    Log2,
    Log10,
    Sin,
    Cos,
    Tan,
    Asin,
    Acos,
    Atan,
    Sinh,
    Cosh,
    Tanh,
    Asinh,
    Acosh,
    Atanh,
    Deg,
    Rad,
    Sgn,
    Clamp {
        min: f64,
        max: f64,
    },
    ClampMin {
        min: f64,
    },
    ClampMax {
        max: f64,
    },
}

/// Float arithmetic between a vector and a scalar, as in Prometheus.
#[derive(Clone, Copy, Debug)]
pub enum ArithmeticOp {
    Add,
    Subtract,
    Multiply,
    Divide,
    Modulo,
    Power,
    Atan2,
}

/// The scalar's position in a binary expression; noncommutative operators retain this order.
#[derive(Clone, Copy, Debug)]
pub enum ScalarOperand {
    Left(f64),
    Right(f64),
}

/// Applies float math to every field of one evaluated series and drops the metric name, like
/// Prometheus' `simpleFloatFunc`.
///
/// The explicit schema and actual grid range are preserved. Fields are processed in their own
/// chunk boundaries, without expanding to the full grid or densifying chunked samples. Missing
/// samples stay missing; ordinary NaNs remain values. An empty or all-missing input is valid.
///
/// Eval must resolve raw staleness before calling this kernel. Evaluated samples use validity,
/// not NaN payloads, for missingness: arithmetic can produce the reserved stale bit pattern too.
/// Raw time axes are rejected.
///
/// Native histogram samples are skipped, as Prometheus' math functions process only float
/// samples: a point holding a histogram has no result in that field, and a series with no float
/// sample left has none at all. Float-only fields keep their chunks; a field with histograms is
/// rebuilt point by point.
///
/// Returns `None` for `clamp` with `max < min`, which yields no series in Prometheus.
///
/// Distinct inputs can share a label set once the name is dropped. Merging them, or failing
/// when both have a value at one step, is left to the caller (Prometheus
/// `mergeSeriesWithSameLabelset`).
pub fn map_math(schema: &SeriesSchema, series: &Series, op: MathOp) -> Result<Option<Series>> {
    if op.is_empty_clamp() {
        validate_grid_series(schema, series)?;
        return Ok(None);
    }
    if !is_float_series(series) {
        let time = validate_grid_series(schema, series)?;
        let mut builder = GridSeriesBuilder::try_new(Arc::new(schema.clone()))?;
        let mut cursors: Vec<_> = series.fields.iter().map(|field| field.cursor()).collect();
        let mut row = Vec::with_capacity(cursors.len());
        for (point, index) in time.positions() {
            row.clear();
            for cursor in &mut cursors {
                row.push(
                    match cursor
                        .at(index)
                        .expect("the axis and the fields are aligned")
                    {
                        SampleRef::Float(value) => Some(op.calculate(value)),
                        SampleRef::Missing | SampleRef::Histogram(_) => None,
                    },
                );
            }
            builder.push(point, &row)?;
        }
        return builder.finish(drop_metric_name(&series.metric)?);
    }
    let mut output = map_float_fields(schema, series, |floats| op.apply(floats))?;
    output.metric = drop_metric_name(&output.metric)?;
    Ok(Some(output))
}

/// Negates every float field and drops the metric name, like Prometheus' unary minus.
///
/// Shares the evaluated-grid, missingness, unsupported-type and same-labelset contract of
/// [`map_math`].
pub fn map_negate(schema: &SeriesSchema, series: &Series) -> Result<Series> {
    let mut output = map_float_fields(schema, series, |floats| floats.unary(|v| -v))?;
    output.metric = drop_metric_name(&output.metric)?;
    Ok(output)
}

/// Applies arithmetic against a constant scalar to every field and drops the metric name.
///
/// Shares the evaluated-grid, missingness, unsupported-type and same-labelset contract of
/// [`map_math`]. Results follow Prometheus `vectorElemBinop`, including Go's `math.Pow`,
/// `math.Mod` and `math.Atan2`.
pub fn map_arithmetic(
    schema: &SeriesSchema,
    series: &Series,
    op: ArithmeticOp,
    scalar: ScalarOperand,
) -> Result<Series> {
    let mut output = map_float_fields(schema, series, |floats| op.apply(floats, scalar))?;
    output.metric = drop_metric_name(&output.metric)?;
    Ok(output)
}

/// Produces an unlabelled scalar over exactly the requested evaluated range.
///
/// Scalar results require a single-field schema. No points outside `range` are allocated.
pub fn constant_series(schema: &SeriesSchema, range: GridRange, scalar: f64) -> Result<Series> {
    let fields = SeriesFields::one(Samples::from_float(Float64Array::from(vec![
        scalar;
        range.len()
    ])));
    Series::try_new(
        schema,
        MetricName::try_new(None, vec![]).map_err(series_error)?,
        TimeAxis::Grid(range),
        fields,
    )
    .map_err(series_error)
}

/// Evaluates `pi()` without a vector input, preserving the requested short grid range.
pub fn map_pi(schema: &SeriesSchema, range: GridRange) -> Result<Series> {
    constant_series(schema, range, PI)
}

fn map_float_fields(
    schema: &SeriesSchema,
    series: &Series,
    map: impl Fn(&Float64Array) -> Float64Array,
) -> Result<Series> {
    validate_float_series(schema, series)?;
    let fields = series
        .fields
        .iter()
        .map(|samples| map_samples(samples, &map))
        .collect::<Result<Vec<_>>>()?;
    let fields = SeriesFields::try_from_vec(fields).map_err(series_error)?;
    Series::try_new(schema, series.metric.clone(), series.time.clone(), fields)
        .map_err(series_error)
}

/// Prometheus `DropReserved(schema.IsMetadataLabel)` drops the type and unit labels with the
/// name. Empty values are dropped too: Prometheus has no empty labels, and keeping them would
/// hide a same-labelset collision from the caller.
pub(crate) fn drop_metric_name(metric: &MetricName) -> Result<MetricName> {
    rebuild_labels(metric, None, |name| {
        !matches!(name, "__type__" | "__unit__")
    })
}

/// Drops labels with empty values, which Prometheus treats as absent.
pub(crate) fn drop_empty_labels(metric: &MetricName) -> Result<MetricName> {
    let name = metric.name().filter(|name| !name.is_empty()).map(Arc::from);
    rebuild_labels(metric, name, |_| true)
}

/// One rebuild: removing labels one by one copies the label array each time.
fn rebuild_labels(
    metric: &MetricName,
    name: Option<Arc<str>>,
    keep: impl Fn(&str) -> bool,
) -> Result<MetricName> {
    let tags = metric
        .tags()
        .iter()
        .filter(|label| !label.value.is_empty() && keep(&label.name))
        .cloned()
        .collect();
    MetricName::try_new(name, tags).map_err(series_error)
}

impl MathOp {
    fn apply(self, floats: &Float64Array) -> Float64Array {
        floats.unary(|sample| self.calculate(sample))
    }

    /// The function applied to one sample.
    pub(crate) fn calculate(self, v: f64) -> f64 {
        match self {
            Self::Exp => go_math::exp(v),
            Self::Ln => go_math::log(v),
            Self::Log2 => go_math::log2(v),
            Self::Log10 => go_math::log10(v),
            Self::Sin => go_math::sin(v),
            Self::Cos => go_math::cos(v),
            Self::Tan => go_math::tan(v),
            Self::Asin => v.asin(),
            Self::Acos => go_math::acos(v),
            Self::Atan => v.atan(),
            Self::Sinh => go_math::sinh(v),
            Self::Cosh => go_math::cosh(v),
            Self::Tanh => v.tanh(),
            Self::Asinh => v.asinh(),
            Self::Acosh => v.acosh(),
            Self::Atanh => v.atanh(),
            // Prometheus' operation order, which rounds differently from to_degrees/to_radians.
            Self::Deg => v * 180.0 / PI,
            Self::Rad => v * PI / 180.0,
            // Zeros and NaN are returned unchanged, so -0 keeps its sign.
            Self::Sgn => {
                if v < 0.0 {
                    -1.0
                } else if v > 0.0 {
                    1.0
                } else {
                    v
                }
            }
            Self::Clamp { min, max } => go_max(min, go_min(max, v)),
            Self::ClampMin { min } => go_max(min, v),
            Self::ClampMax { max } => go_min(max, v),
            Self::Abs => v.abs(),
            Self::Ceil => v.ceil(),
            Self::Floor => v.floor(),
            Self::Sqrt => v.sqrt(),
            Self::Round { nearest } => Round::round_to_nearest(v, nearest),
        }
    }

    /// Prometheus returns an empty vector when `max < min`; NaN bounds do not compare.
    fn is_empty_clamp(self) -> bool {
        match self {
            Self::Clamp { min, max } => max < min,
            _ => false,
        }
    }
}

/// Go `math.Max`: +Inf wins over NaN, and +0 over -0.
fn go_max(x: f64, y: f64) -> f64 {
    if x == f64::INFINITY || y == f64::INFINITY {
        f64::INFINITY
    } else if x.is_nan() || y.is_nan() {
        f64::NAN
    } else if x == 0.0 && x == y {
        if x.is_sign_negative() { y } else { x }
    } else if x > y {
        x
    } else {
        y
    }
}

/// Go `math.Min`: -Inf wins over NaN, and -0 over +0.
fn go_min(x: f64, y: f64) -> f64 {
    if x == f64::NEG_INFINITY || y == f64::NEG_INFINITY {
        f64::NEG_INFINITY
    } else if x.is_nan() || y.is_nan() {
        f64::NAN
    } else if x == 0.0 && x == y {
        if x.is_sign_negative() { x } else { y }
    } else if x < y {
        x
    } else {
        y
    }
}

impl ArithmeticOp {
    /// Prometheus `vectorElemBinop` for two floats.
    pub fn calculate(self, left: f64, right: f64) -> f64 {
        match self {
            Self::Add => left + right,
            Self::Subtract => left - right,
            Self::Multiply => left * right,
            Self::Divide => left / right,
            Self::Modulo => go_math::fmod(left, right),
            Self::Power => go_math::pow(left, right),
            Self::Atan2 => go_math::atan2(left, right),
        }
    }

    fn apply(self, floats: &Float64Array, scalar: ScalarOperand) -> Float64Array {
        match scalar {
            ScalarOperand::Left(scalar) => floats.unary(|sample| self.calculate(scalar, sample)),
            ScalarOperand::Right(scalar) => floats.unary(|sample| self.calculate(sample, scalar)),
        }
    }
}

fn map_samples(samples: &Samples, map: &impl Fn(&Float64Array) -> Float64Array) -> Result<Samples> {
    let chunks = samples
        .segments()
        .iter()
        .map(|segment| map_float_segment(segment, map))
        .collect::<Result<Vec<_>>>()?;
    Samples::try_from_chunks(chunks).map_err(series_error)
}

fn map_float_segment(
    segment: &Samples,
    map: &impl Fn(&Float64Array) -> Float64Array,
) -> Result<Samples> {
    let Some(DenseSamplesRef::Float(floats)) = segment.dense() else {
        return Err(DataFusionError::NotImplemented(
            "Series operators support only float samples; histogram/mixed samples are not supported"
                .into(),
        ));
    };
    Ok(Samples::from_float(map(floats)))
}

fn series_error(error: datatypes::error::Error) -> DataFusionError {
    DataFusionError::External(Box::new(error))
}

/// Checks an evaluated float series up front, so no segment of another type yields a result.
///
/// Returns the validated axis: it is addressed on a grid, so the caller reads the rows it carries
/// with the axis traversal rather than assuming a contiguous range.
pub(crate) fn validate_float_series<'a>(
    schema: &SeriesSchema,
    series: &'a Series,
) -> Result<&'a TimeAxis> {
    let time = validate_grid_series(schema, series)?;
    if !is_float_series(series) {
        return Err(DataFusionError::NotImplemented(
            "Series operators support only float samples; histogram/mixed samples are not supported"
                .into(),
        ));
    }
    Ok(time)
}

/// Whether every field of `series` holds float samples only.
fn is_float_series(series: &Series) -> bool {
    series.fields.iter().all(|field| {
        field
            .segments()
            .iter()
            .all(|segment| matches!(segment.dense(), Some(DenseSamplesRef::Float(_))))
    })
}

/// Checks `series` against `schema` and its evaluation grid, whatever its sample types.
pub(crate) fn validate_grid_series<'a>(
    schema: &SeriesSchema,
    series: &'a Series,
) -> Result<&'a TimeAxis> {
    let time = &series.time;
    if time.grid().is_none() {
        return Err(DataFusionError::Execution(
            "Series operators require an evaluated grid time axis".into(),
        ));
    }
    Series::try_new(
        schema,
        series.metric.clone(),
        series.time.clone(),
        series.fields.clone(),
    )
    .map_err(series_error)?;
    Ok(time)
}

/// A sample of a series accepted by [`validate_float_series`].
fn float_sample(sample: SampleRef<'_>) -> Option<f64> {
    match sample {
        SampleRef::Float(sample) => Some(sample),
        SampleRef::Missing => None,
        _ => unreachable!("validated as float"),
    }
}
