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

use datafusion::error::Result;
use datatypes::series::samples::{SampleRef, Series, SeriesSchema};

use crate::grid_series::GridSeriesBuilder;
use crate::map::{ScalarOperand, drop_metric_name, validate_grid_series};

/// IEEE comparisons: NaN only satisfies `NotEqual`, and signed zeros compare equal.
#[derive(Clone, Copy, Debug)]
pub enum ComparisonOp {
    Equal,
    NotEqual,
    Less,
    LessEqual,
    Greater,
    GreaterEqual,
}

/// Filter keeps the vector value and labels; bool emits 0/1 and drops the metric name.
#[derive(Clone, Copy, Debug)]
pub enum Comparison {
    Filter(ComparisonOp),
    Bool(ComparisonOp),
}

/// Compares a vector with a constant scalar, dropping the samples that have no comparison rather
/// than failing the query.
///
/// Prometheus compares float samples only: a histogram has no ordering against a scalar, so the
/// position it holds answers nothing. The existing planner says so with an info and answers no
/// value there (`NativeHistogramDrop`, `prom_token_to_binary_expr_builder` in
/// `query::promql::planner`), which is the behaviour mirrored here. The second element of the
/// answer reports whether a histogram was dropped, so the caller can record that info once per
/// chunk instead of once per position.
///
/// The literal form of a filter: `None` still means nothing passed, so the caller hands on no
/// value for this chunk.
pub fn map_compare_literal(
    schema: &Arc<SeriesSchema>,
    series: &Series,
    comparison: Comparison,
    scalar: ScalarOperand,
) -> Result<(Option<Series>, bool)> {
    // Whatever kinds the fields carry: a histogram position is dropped, not rejected, so the axis
    // and the schema are checked without requiring float samples.
    let time = validate_grid_series(schema, series)?;
    let mut builder = GridSeriesBuilder::try_new(schema.clone())?;
    let mut dropped_histogram = false;
    let mut fields = series
        .fields
        .iter()
        .map(|field| field.iter())
        .collect::<Vec<_>>();
    let mut row = Vec::with_capacity(fields.len());
    for (position, _) in time.positions() {
        row.clear();
        for field in &mut fields {
            row.push(match field.next().expect("validated length") {
                SampleRef::Float(sample) => {
                    let (left, right) = match scalar {
                        ScalarOperand::Left(scalar) => (scalar, sample),
                        ScalarOperand::Right(scalar) => (sample, scalar),
                    };
                    comparison.apply(left, right, sample)
                }
                SampleRef::Histogram(_) => {
                    dropped_histogram = true;
                    None
                }
                SampleRef::Missing => None,
            });
        }
        builder.push(position, &row)?;
    }
    let metric = if matches!(comparison, Comparison::Bool(_)) {
        drop_metric_name(&series.metric)?
    } else {
        series.metric.clone()
    };
    Ok((builder.finish(metric)?, dropped_histogram))
}

impl Comparison {
    /// The output for one `left op right` pair, where `vector` is the vector operand's value.
    pub(crate) fn apply(self, left: f64, right: f64, vector: f64) -> Option<f64> {
        match self {
            Self::Filter(op) => op.apply(left, right).then_some(vector),
            Self::Bool(op) => Some(f64::from(op.apply(left, right))),
        }
    }
}

impl std::fmt::Display for Comparison {
    /// The operator as the query wrote it, which is how the existing planner names it in the info
    /// it records for a dropped histogram sample.
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let op = match self {
            Self::Filter(op) | Self::Bool(op) => op,
        };
        f.write_str(match op {
            ComparisonOp::Equal => "==",
            ComparisonOp::NotEqual => "!=",
            ComparisonOp::Less => "<",
            ComparisonOp::LessEqual => "<=",
            ComparisonOp::Greater => ">",
            ComparisonOp::GreaterEqual => ">=",
        })
    }
}

impl ComparisonOp {
    /// Whether `left op right` holds, by the same IEEE rules the kernels use.
    ///
    /// Public because a compiler that folds a constant comparison has to fold it with the same
    /// answer the kernel would compute: NaN satisfies only `NotEqual`, and signed zeros compare
    /// equal. Defining the six rules a second time is how the folded number and the evaluated one
    /// would start to disagree.
    #[inline]
    pub fn apply(self, left: f64, right: f64) -> bool {
        match self {
            Self::Equal => left == right,
            Self::NotEqual => left != right,
            Self::Less => left < right,
            Self::LessEqual => left <= right,
            Self::Greater => left > right,
            Self::GreaterEqual => left >= right,
        }
    }
}
