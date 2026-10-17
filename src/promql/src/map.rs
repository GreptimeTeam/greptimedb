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

#[cfg(test)]
mod tests;

use datafusion::error::{DataFusionError, Result};
use datatypes::arrow::array::Float64Array;
use datatypes::series::samples::{DenseSamplesRef, Samples, Series, SeriesFields, SeriesSchema};
use datatypes::series::time::TimeAxis;

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
    Round { nearest: f64 },
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
    if !matches!(series.time, TimeAxis::Grid(_)) {
        return Err(DataFusionError::Execution(
            "Map math requires an evaluated grid time axis".into(),
        ));
    }
    let fields = series
        .fields
        .iter()
        .map(|samples| map_samples(samples, op))
        .collect::<Result<Vec<_>>>()?;
    let fields = SeriesFields::try_from_vec(fields).map_err(series_error)?;
    // Ordinary math calls preserve the metric name (unlike binary arithmetic): the HTTP
    // adapter's collect_metric_names follows Call arguments. Keep that compatibility here.
    Series::try_new(schema, series.metric.clone(), series.time.clone(), fields)
        .map_err(series_error)
}

impl MathOp {
    fn apply(self, floats: &Float64Array) -> Float64Array {
        match self {
            Self::Abs => floats.unary(f64::abs),
            Self::Ceil => floats.unary(f64::ceil),
            Self::Floor => floats.unary(f64::floor),
            Self::Sqrt => floats.unary(f64::sqrt),
            Self::Round { nearest } => {
                floats.unary(|sample| Round::round_to_nearest(sample, nearest))
            }
        }
    }
}

fn map_samples(samples: &Samples, op: MathOp) -> Result<Samples> {
    let chunks = samples
        .segments()
        .iter()
        .map(|segment| map_float_segment(segment, op))
        .collect::<Result<Vec<_>>>()?;
    Samples::try_from_chunks(chunks).map_err(series_error)
}

fn map_float_segment(segment: &Samples, op: MathOp) -> Result<Samples> {
    let Some(DenseSamplesRef::Float(floats)) = segment.dense() else {
        return Err(DataFusionError::NotImplemented(
            "Map math supports only float samples; histogram/mixed samples are not supported"
                .into(),
        ));
    };
    Ok(Samples::from_float(op.apply(floats)))
}

fn series_error(error: datatypes::error::Error) -> DataFusionError {
    DataFusionError::External(Box::new(error))
}
