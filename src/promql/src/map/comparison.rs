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
use datatypes::series::samples::{Series, SeriesSchema};
use datatypes::series::time::GridRange;

use crate::grid_series::GridSeriesBuilder;
use crate::map::{
    ArithmeticOp, ScalarOperand, constant_series, drop_metric_name, float_sample, map_arithmetic,
    validate_float_series,
};

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

/// Compares a vector with a constant scalar, like Prometheus `VectorscalarBinop`.
///
/// Each field is compared independently; a row is kept where any field passes a filter. The
/// output is trimmed to its first and last kept rows, and an empty result means nothing passed.
/// A filter keeps a subset of the input's rows, so a passed-over stretch ends a piece and one
/// series can produce several. Raw axes and histogram/mixed inputs share
/// [`crate::map::map_math`]'s rejection contract.
pub fn map_compare(
    schema: &Arc<SeriesSchema>,
    series: &Series,
    comparison: Comparison,
    scalar: ScalarOperand,
) -> Result<Vec<Series>> {
    let range = validate_float_series(schema, series)?;
    let mut builder = GridSeriesBuilder::try_new(schema.clone())?;
    let mut fields = series
        .fields
        .iter()
        .map(|field| field.iter())
        .collect::<Vec<_>>();
    let mut row = Vec::with_capacity(fields.len());
    for index in range.start()..range.start() + range.len() {
        row.clear();
        for field in &mut fields {
            let sample = float_sample(field.next().expect("validated length"));
            row.push(sample.and_then(|sample| {
                let (left, right) = match scalar {
                    ScalarOperand::Left(scalar) => (scalar, sample),
                    ScalarOperand::Right(scalar) => (sample, scalar),
                };
                comparison.apply(left, right, sample)
            }));
        }
        builder.push(index, &row)?;
    }
    let metric = if matches!(comparison, Comparison::Bool(_)) {
        drop_metric_name(&series.metric)?
    } else {
        series.metric.clone()
    };
    builder.finish(metric)
}

/// Evaluates scalar–scalar arithmetic as an unlabelled, single-field series on `range`.
pub fn scalar_arithmetic(
    schema: &SeriesSchema,
    range: GridRange,
    op: ArithmeticOp,
    operands: [f64; 2],
) -> Result<Series> {
    let left = constant_series(schema, range, operands[0])?;
    map_arithmetic(schema, &left, op, ScalarOperand::Right(operands[1]))
}

/// Evaluates scalar–scalar comparison, which requires bool in PromQL, on `range`.
pub fn scalar_compare(
    schema: &SeriesSchema,
    range: GridRange,
    op: ComparisonOp,
    operands: [f64; 2],
) -> Result<Series> {
    constant_series(schema, range, f64::from(op.apply(operands[0], operands[1])))
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

impl ComparisonOp {
    fn apply(self, left: f64, right: f64) -> bool {
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
