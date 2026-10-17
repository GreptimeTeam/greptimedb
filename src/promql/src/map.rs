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

use std::sync::Arc;

use datafusion::error::{DataFusionError, Result};
use datatypes::series::samples::{
    DenseSamplesRef, SampleRef, Samples, Series, SeriesFields, SeriesSchema,
};
use datatypes::series::time::TimeAxis;

use crate::functions::Round;
use crate::grid_series::{GridSeriesBuilder, METADATA_LABELS};

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

/// Applies float math to every field of one evaluated series.
///
/// This follows Prometheus `simpleFloatFunc`: the metadata labels (`__name__`, `__type__`,
/// `__unit__`) are dropped and only float samples are mapped. Histogram samples are skipped
/// rather than rejected, because a field's sample types are known only at run time
/// (`abs(rate(native_histogram[5m]))` holds histograms only after evaluation); the result is
/// `None` when no float value survives.
///
/// The explicit schema and actual grid range are preserved, except that trailing and leading
/// rows without any value are trimmed when histograms leave holes. Fields are processed in their
/// own chunk boundaries, without expanding to the full grid or densifying chunked samples.
/// Missing samples stay missing; ordinary NaNs remain values.
///
/// Dropping the reserved labels means two input series that differ only in them now share a
/// label set, which Prometheus rejects for one vector. Callers must run
/// [`merge_same_labelset`](crate::grid_series::merge_same_labelset) on the results, as the
/// planning/output layer does for other label-rewriting kernels.
///
/// Eval must resolve raw staleness before calling this kernel. Evaluated samples use validity,
/// not NaN payloads, for missingness: arithmetic can produce the reserved stale bit pattern too.
/// Raw time axes are rejected.
pub fn map_math(schema: &SeriesSchema, series: &Series, op: MathOp) -> Result<Option<Series>> {
    let TimeAxis::Grid(range) = &series.time else {
        return Err(DataFusionError::Execution(
            "Map math requires an evaluated grid time axis".into(),
        ));
    };
    let mut metric = series.metric.clone();
    metric.remove_all(&METADATA_LABELS);

    let all_floats = series.fields.iter().all(|samples| {
        samples
            .segments()
            .iter()
            .all(|segment| matches!(segment.dense(), Some(DenseSamplesRef::Float(_))))
    });
    if all_floats {
        // Dense floats can still be entirely null. Prometheus emits no series then, and the
        // per-point path below reports the same input as `None`.
        if !series
            .fields
            .iter()
            .any(|samples| samples.segments().iter().any(has_float_value))
        {
            return Ok(None);
        }
        let fields = series
            .fields
            .iter()
            .map(|samples| {
                let segments = samples
                    .segments()
                    .iter()
                    .map(|segment| {
                        let Some(DenseSamplesRef::Float(floats)) = segment.dense() else {
                            unreachable!("checked above");
                        };
                        Samples::from_float(floats.unary(|value| apply(op, value)))
                    })
                    .collect();
                Samples::try_from_chunks(segments).map_err(series_error)
            })
            .collect::<Result<Vec<_>>>()?;
        let fields = SeriesFields::try_from_vec(fields).map_err(series_error)?;
        return Series::try_new(schema, metric, series.time.clone(), fields)
            .map(Some)
            .map_err(series_error);
    }

    // Histogram samples leave holes, which may move the ends of the series.
    let mut builder = GridSeriesBuilder::try_new(Arc::new(schema.clone()))?;
    // One cursor per field. `sample(row)` restarts its walk from the first chunk on every row,
    // which costs O(rows x chunks) on chunked input; advancing an iterator visits each segment
    // once and lands on the same sample for every row.
    let mut cursors = series
        .fields
        .iter()
        .map(|samples| samples.iter())
        .collect::<Vec<_>>();
    let mut values = vec![None; series.fields.len()];
    for row in 0..range.len() {
        for (value, cursor) in values.iter_mut().zip(cursors.iter_mut()) {
            *value = match cursor.next() {
                Some(SampleRef::Float(sample)) => Some(apply(op, sample)),
                // A histogram point has no float to map, and a missing point stays missing.
                _ => None,
            };
        }
        builder.push(range.start() + row, &values)?;
    }
    builder.finish(metric)
}

/// The float math `op` applies to one value.
fn apply(op: MathOp, value: f64) -> f64 {
    match op {
        MathOp::Abs => value.abs(),
        MathOp::Ceil => value.ceil(),
        MathOp::Floor => value.floor(),
        MathOp::Sqrt => value.sqrt(),
        MathOp::Round { nearest } => Round::round_to_nearest(value, nearest),
    }
}

/// Whether `segment` holds a float value; every segment is dense here.
fn has_float_value(segment: &Samples) -> bool {
    let Some(DenseSamplesRef::Float(floats)) = segment.dense() else {
        return false;
    };
    floats.iter().any(|value| value.is_some())
}

fn series_error(error: datatypes::error::Error) -> DataFusionError {
    DataFusionError::External(Box::new(error))
}
