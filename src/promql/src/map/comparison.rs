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

use datafusion::error::{DataFusionError, Result};
use datatypes::arrow::array::Float64Array;
use datatypes::series::samples::{Series, SeriesSchema};
use datatypes::series::time::GridRange;

use crate::map::{ArithmeticOp, ScalarOperand, constant_series, map_arithmetic, map_float_fields};

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

/// Filter keeps the vector value and name; bool emits 0/1 and removes the metric name.
#[derive(Clone, Copy, Debug)]
pub enum Comparison {
    Filter(ComparisonOp),
    Bool(ComparisonOp),
}

/// Compares a vector with a constant scalar without losing its absolute grid positions.
///
/// False filter results become Missing, not compacted rows. Like main, non-bool filtering
/// rejects ordinary multi-field input; bool compares each field independently. Raw axes and
/// histogram/mixed inputs share [`crate::map::map_math`]'s rejection contract.
pub fn map_compare(
    schema: &SeriesSchema,
    series: &Series,
    comparison: Comparison,
    scalar: ScalarOperand,
) -> Result<Series> {
    if matches!(comparison, Comparison::Filter(_)) && series.fields.len() != 1 {
        return Err(DataFusionError::NotImplemented(
            "filter on multi-value input".into(),
        ));
    }
    let mut output =
        map_float_fields(
            schema,
            series,
            |floats| Ok(comparison.apply(floats, scalar)),
        )?;
    if matches!(comparison, Comparison::Bool(_)) {
        output.metric.remove("__name__");
    }
    Ok(output)
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
    let left = constant_series(schema, range, operands[0])?;
    map_compare(
        schema,
        &left,
        Comparison::Bool(op),
        ScalarOperand::Right(operands[1]),
    )
}

impl Comparison {
    fn apply(self, floats: &Float64Array, scalar: ScalarOperand) -> Float64Array {
        floats
            .iter()
            .map(|sample| {
                let sample = sample?;
                let (left, right) = match scalar {
                    ScalarOperand::Left(scalar) => (scalar, sample),
                    ScalarOperand::Right(scalar) => (sample, scalar),
                };
                match self {
                    Self::Filter(op) => op.apply(left, right).then_some(sample),
                    Self::Bool(op) => Some(f64::from(op.apply(left, right))),
                }
            })
            .collect()
    }
}

impl ComparisonOp {
    pub(crate) fn apply(self, left: f64, right: f64) -> bool {
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
