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

pub use broadcast::{ScalarSeriesOperand, map_arithmetic_series, map_compare_series};
pub use comparison::{Comparison, ComparisonOp, map_compare, scalar_arithmetic, scalar_compare};

use std::f64::consts::PI;

use datafusion::error::{DataFusionError, Result};
use datatypes::arrow::array::Float64Array;
use datatypes::series::MetricName;
use datatypes::series::samples::{DenseSamplesRef, Samples, Series, SeriesFields, SeriesSchema};
use datatypes::series::time::{GridRange, TimeAxis};

use crate::functions::Round;

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
/// Raw time axes and histogram/mixed segments are rejected. Unsupported query types must be
/// admitted to the ordinary path before execution; this function neither falls back nor returns
/// partial results.
///
/// Returns `None` for `clamp` with `max < min`, which yields no series in Prometheus.
///
/// Distinct inputs can share a label set once the name is dropped. Merging them, or failing
/// when both have a value at one step, is left to the caller (Prometheus
/// `mergeSeriesWithSameLabelset`).
pub fn map_math(schema: &SeriesSchema, series: &Series, op: MathOp) -> Result<Option<Series>> {
    let mut output = map_float_fields(schema, series, |floats| op.apply(floats))?;
    if op.is_empty_clamp() {
        return Ok(None);
    }
    drop_metric_name(&mut output.metric);
    Ok(Some(output))
}

/// Negates every float field and drops the metric name, like Prometheus' unary minus.
///
/// Shares the evaluated-grid, missingness, unsupported-type and same-labelset contract of
/// [`map_math`].
pub fn map_negate(schema: &SeriesSchema, series: &Series) -> Result<Series> {
    let mut output = map_float_fields(schema, series, |floats| floats.unary(|v| -v))?;
    drop_metric_name(&mut output.metric);
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
    drop_metric_name(&mut output.metric);
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
    if !matches!(series.time, TimeAxis::Grid(_)) {
        return Err(DataFusionError::Execution(
            "Map requires an evaluated grid time axis".into(),
        ));
    }
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
/// name.
pub(crate) fn drop_metric_name(metric: &mut MetricName) {
    metric.remove_all(&["__name__", "__type__", "__unit__"]);
}

impl MathOp {
    fn apply(self, floats: &Float64Array) -> Float64Array {
        match self {
            Self::Exp => floats.unary(go_math::exp),
            Self::Ln => floats.unary(go_math::log),
            Self::Log2 => floats.unary(go_math::log2),
            Self::Log10 => floats.unary(go_math::log10),
            Self::Sin => floats.unary(go_math::sin),
            Self::Cos => floats.unary(go_math::cos),
            Self::Tan => floats.unary(go_math::tan),
            Self::Asin => floats.unary(go_math::asin),
            Self::Acos => floats.unary(go_math::acos),
            Self::Atan => floats.unary(go_math::atan),
            Self::Sinh => floats.unary(go_math::sinh),
            Self::Cosh => floats.unary(go_math::cosh),
            Self::Tanh => floats.unary(go_math::tanh),
            Self::Asinh => floats.unary(go_math::asinh),
            Self::Acosh => floats.unary(go_math::acosh),
            Self::Atanh => floats.unary(go_math::atanh),
            // Prometheus' operation order, which rounds differently from to_degrees/to_radians.
            Self::Deg => floats.unary(|v| v * 180.0 / PI),
            Self::Rad => floats.unary(|v| v * PI / 180.0),
            // Zeros and NaN are returned unchanged, so -0 keeps its sign.
            Self::Sgn => floats.unary(|v| {
                if v < 0.0 {
                    -1.0
                } else if v > 0.0 {
                    1.0
                } else {
                    v
                }
            }),
            Self::Clamp { min, max } => clamp(floats, min, max),
            Self::ClampMin { min } => clamp(floats, min, f64::INFINITY),
            Self::ClampMax { max } => clamp(floats, f64::NEG_INFINITY, max),
            Self::Abs => floats.unary(f64::abs),
            Self::Ceil => floats.unary(f64::ceil),
            Self::Floor => floats.unary(f64::floor),
            Self::Sqrt => floats.unary(f64::sqrt),
            Self::Round { nearest } => {
                floats.unary(|sample| Round::round_to_nearest(sample, nearest))
            }
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

fn clamp(floats: &Float64Array, min: f64, max: f64) -> Float64Array {
    floats.unary(|sample| go_max(min, go_min(max, sample)))
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
    pub(crate) fn calculate(self, left: f64, right: f64) -> f64 {
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
            "Map supports only float samples; histogram/mixed samples are not supported".into(),
        ));
    };
    Ok(Samples::from_float(map(floats)))
}

pub(super) fn series_error(error: datatypes::error::Error) -> DataFusionError {
    DataFusionError::External(Box::new(error))
}
