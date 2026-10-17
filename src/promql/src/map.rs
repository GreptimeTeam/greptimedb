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

mod comparison;
#[cfg(test)]
mod tests;

pub use comparison::{Comparison, ComparisonOp, map_compare, scalar_arithmetic, scalar_compare};

use std::sync::Arc;

use common_function::function::Function;
use common_function::scalars::math::ClampFunction;
use datafusion::error::{DataFusionError, Result};
use datafusion::functions::math;
use datafusion_common::ScalarValue;
use datafusion_common::config::ConfigOptions;
use datafusion_expr::{ColumnarValue, ScalarFunctionArgs};
use datatypes::arrow::array::{AsArray, Float64Array};
use datatypes::arrow::datatypes::{DataType, Field, Float64Type};
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

/// Float arithmetic against a constant scalar, with ordinary-path semantics.
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

/// Applies float math to every field of one evaluated series, preserving its labels.
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
pub fn map_math(schema: &SeriesSchema, series: &Series, op: MathOp) -> Result<Series> {
    // Ordinary math calls preserve the metric name (unlike binary arithmetic): the HTTP
    // adapter's collect_metric_names follows Call arguments. Keep that compatibility here.
    map_float_fields(schema, series, |floats| op.apply(floats))
}

/// Negates every float field and removes the metric name, like the ordinary unary path.
///
/// Shares the evaluated-grid, missingness and unsupported-type contract of [`map_math`].
pub fn map_negate(schema: &SeriesSchema, series: &Series) -> Result<Series> {
    let mut output = map_float_fields(schema, series, |floats| Ok(floats.unary(|v| -v)))?;
    output.metric.remove("__name__");
    Ok(output)
}

/// Applies arithmetic against a constant scalar to every field, removing the metric name.
///
/// Shares the evaluated-grid, missingness and unsupported-type contract of [`map_math`].
/// Division and remainder use IEEE float semantics, but power retains the ordinary UDF's
/// error for a zero base raised to a negative exponent. Any error fails the whole series.
pub fn map_arithmetic(
    schema: &SeriesSchema,
    series: &Series,
    op: ArithmeticOp,
    scalar: ScalarOperand,
) -> Result<Series> {
    let mut output = map_float_fields(schema, series, |floats| op.apply(floats, scalar))?;
    output.metric.remove("__name__");
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
    constant_series(schema, range, std::f64::consts::PI)
}

fn map_float_fields(
    schema: &SeriesSchema,
    series: &Series,
    map: impl Fn(&Float64Array) -> Result<Float64Array>,
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

impl MathOp {
    fn apply(self, floats: &Float64Array) -> Result<Float64Array> {
        Ok(match self {
            Self::Exp => floats.unary(f64::exp),
            Self::Ln => floats.unary(f64::ln),
            Self::Log2 => floats.unary(f64::log2),
            Self::Log10 => floats.unary(f64::log10),
            Self::Sin => floats.unary(f64::sin),
            Self::Cos => floats.unary(f64::cos),
            Self::Tan => floats.unary(f64::tan),
            Self::Asin => floats.unary(f64::asin),
            Self::Acos => floats.unary(f64::acos),
            Self::Atan => floats.unary(f64::atan),
            Self::Sinh => floats.unary(f64::sinh),
            Self::Cosh => floats.unary(f64::cosh),
            Self::Tanh => floats.unary(f64::tanh),
            Self::Asinh => floats.unary(f64::asinh),
            Self::Acosh => floats.unary(f64::acosh),
            Self::Atanh => floats.unary(f64::atanh),
            Self::Deg => floats.unary(f64::to_degrees),
            Self::Rad => floats.unary(f64::to_radians),
            Self::Sgn => floats.unary(|v| if v == 0.0 { 0.0 } else { v.signum() }),
            Self::Clamp { min, max } => return map_clamp(floats, min, max),
            Self::ClampMin { min } => return map_clamp(floats, min, f64::INFINITY),
            Self::ClampMax { max } => return map_clamp(floats, f64::NEG_INFINITY, max),
            Self::Abs => floats.unary(f64::abs),
            Self::Ceil => floats.unary(f64::ceil),
            Self::Floor => floats.unary(f64::floor),
            Self::Sqrt => floats.unary(f64::sqrt),
            Self::Round { nearest } => {
                floats.unary(|sample| Round::round_to_nearest(sample, nearest))
            }
        })
    }
}

fn map_clamp(floats: &Float64Array, min: f64, max: f64) -> Result<Float64Array> {
    // Keep the ordinary path's bounds errors, NaN comparisons, and signed-zero behavior.
    let args = vec![
        ColumnarValue::Array(Arc::new(floats.clone())),
        ColumnarValue::Scalar(ScalarValue::Float64(Some(min))),
        ColumnarValue::Scalar(ScalarValue::Float64(Some(max))),
    ];
    let field = Arc::new(Field::new("value", DataType::Float64, true));
    let output = ClampFunction::default().invoke_with_args(ScalarFunctionArgs {
        arg_fields: vec![field.clone(); 3],
        args,
        number_rows: floats.len(),
        return_field: field,
        config_options: Arc::new(ConfigOptions::default()),
    })?;
    Ok(output
        .into_array(floats.len())?
        .as_primitive::<Float64Type>()
        .clone())
}

impl ArithmeticOp {
    fn apply(self, floats: &Float64Array, scalar: ScalarOperand) -> Result<Float64Array> {
        Ok(match self {
            Self::Add => map_binary(floats, scalar, |left, right| left + right),
            Self::Subtract => map_binary(floats, scalar, |left, right| left - right),
            Self::Multiply => map_binary(floats, scalar, |left, right| left * right),
            Self::Divide => map_binary(floats, scalar, |left, right| left / right),
            Self::Modulo => map_binary(floats, scalar, |left, right| left % right),
            Self::Atan2 => map_binary(floats, scalar, f64::atan2),
            Self::Power => return map_power(floats, scalar),
        })
    }
}

fn map_binary(
    floats: &Float64Array,
    scalar: ScalarOperand,
    calculate: impl Fn(f64, f64) -> f64,
) -> Float64Array {
    match scalar {
        ScalarOperand::Left(scalar) => floats.unary(|sample| calculate(scalar, sample)),
        ScalarOperand::Right(scalar) => floats.unary(|sample| calculate(sample, scalar)),
    }
}

fn map_power(floats: &Float64Array, scalar: ScalarOperand) -> Result<Float64Array> {
    let samples = ColumnarValue::Array(Arc::new(floats.clone()));
    let literal = |scalar| ColumnarValue::Scalar(ScalarValue::Float64(Some(scalar)));
    let args = match scalar {
        ScalarOperand::Left(scalar) => vec![literal(scalar), samples],
        ScalarOperand::Right(scalar) => vec![samples, literal(scalar)],
    };
    // The existing UDF owns power's domain checks and null handling. A direct powf map would
    // incorrectly accept 0 ^ negative.
    let operand_field = Arc::new(Field::new("operand", DataType::Float64, true));
    let output = math::power().invoke_with_args(ScalarFunctionArgs {
        arg_fields: vec![operand_field; 2],
        args,
        number_rows: floats.len(),
        return_field: Arc::new(Field::new("power", DataType::Float64, true)),
        config_options: Arc::new(ConfigOptions::default()),
    })?;
    Ok(output
        .into_array(floats.len())?
        .as_primitive::<Float64Type>()
        .clone())
}

fn map_samples(
    samples: &Samples,
    map: &impl Fn(&Float64Array) -> Result<Float64Array>,
) -> Result<Samples> {
    let chunks = samples
        .segments()
        .iter()
        .map(|segment| map_float_segment(segment, map))
        .collect::<Result<Vec<_>>>()?;
    Samples::try_from_chunks(chunks).map_err(series_error)
}

fn map_float_segment(
    segment: &Samples,
    map: &impl Fn(&Float64Array) -> Result<Float64Array>,
) -> Result<Samples> {
    let Some(DenseSamplesRef::Float(floats)) = segment.dense() else {
        return Err(DataFusionError::NotImplemented(
            "Map supports only float samples; histogram/mixed samples are not supported".into(),
        ));
    };
    Ok(Samples::from_float(map(floats)?))
}

fn series_error(error: datatypes::error::Error) -> DataFusionError {
    DataFusionError::External(Box::new(error))
}
