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

//! Precise single-column predicates and record batch filtering.

mod predicate;
#[cfg(test)]
mod tests;
mod timestamp;

use std::sync::Arc;

use datafusion::error::Result as DfResult;
use datafusion::logical_expr::{Expr, Literal, Operator};
use datafusion::physical_plan::PhysicalExpr;
use datafusion_common::arrow::array::{ArrayRef, Datum};
use datafusion_common::arrow::buffer::BooleanBuffer;
use datafusion_common::cast::{as_boolean_array, as_null_array};
use datafusion_common::{DataFusionError, ScalarValue, internal_err};
use datatypes::arrow::array::{Array, BooleanArray, BooleanBufferBuilder, RecordBatch};
use datatypes::arrow::compute::filter_record_batch;
use datatypes::arrow::datatypes::DataType;
use datatypes::data_type::{ConcreteDataType, DataType as _};
use datatypes::value::Value;
use datatypes::vectors::VectorRef;
use snafu::ResultExt;

pub use self::predicate::{regexp_is_match_dictionary, regexp_is_match_scalar};
pub use self::timestamp::timestamp_scalar_value;
use crate::error::{Result, ToArrowScalarSnafu};
use crate::filter::predicate::SimplePredicate;

/// Evaluates supported single-column predicates with precompiled regular expressions.
/// Compound predicates preserve SQL null semantics until conversion to a scan mask.
#[derive(Debug, Clone)]
pub struct SimpleFilterEvaluator {
    column_name: String,
    predicate: SimplePredicate,
}

impl SimpleFilterEvaluator {
    pub fn new<T: Literal>(column_name: String, lit: T, op: Operator) -> Option<Self> {
        if !matches!(
            op,
            Operator::Eq
                | Operator::NotEq
                | Operator::Lt
                | Operator::LtEq
                | Operator::Gt
                | Operator::GtEq
        ) {
            return None;
        }
        let Expr::Literal(value, _) = lit.lit() else {
            return None;
        };
        Some(Self {
            column_name,
            predicate: SimplePredicate::Comparison {
                literal: value.to_scalar().ok()?,
                op,
                regex: None,
                regex_negative: false,
            },
        })
    }

    pub fn try_new(predicate: &Expr) -> Option<Self> {
        Self::try_new_with_column_type(predicate, &|_| None)
    }

    /// Recognizes single-column filters, including lossless string dictionary casts
    /// when the caller can resolve the column's type.
    pub fn try_new_with_column_type(
        predicate: &Expr,
        column_type: &impl Fn(&str) -> Option<DataType>,
    ) -> Option<Self> {
        let columns = predicate.column_refs();
        if columns.len() != 1 {
            return None;
        }
        let column_name = columns.into_iter().next()?.name.clone();
        let column_type = column_type(&column_name);
        Some(Self {
            column_name,
            predicate: SimplePredicate::try_new(predicate, column_type.as_ref())?,
        })
    }

    /// Returns the referenced column name.
    pub fn column_name(&self) -> &str {
        &self.column_name
    }

    pub fn is_eq(&self) -> bool {
        matches!(
            self.predicate,
            SimplePredicate::Comparison {
                op: Operator::Eq,
                ..
            }
        )
    }
    pub fn is_not_eq(&self) -> bool {
        matches!(
            self.predicate,
            SimplePredicate::Comparison {
                op: Operator::NotEq,
                ..
            }
        )
    }
    pub fn is_lt(&self) -> bool {
        matches!(
            self.predicate,
            SimplePredicate::Comparison {
                op: Operator::Lt,
                ..
            }
        )
    }
    pub fn is_lt_eq(&self) -> bool {
        matches!(
            self.predicate,
            SimplePredicate::Comparison {
                op: Operator::LtEq,
                ..
            }
        )
    }
    pub fn is_gt(&self) -> bool {
        matches!(
            self.predicate,
            SimplePredicate::Comparison {
                op: Operator::Gt,
                ..
            }
        )
    }
    pub fn is_gt_eq(&self) -> bool {
        matches!(
            self.predicate,
            SimplePredicate::Comparison {
                op: Operator::GtEq,
                ..
            }
        )
    }

    /// Returns true for an OR chain of equality comparisons.
    pub fn is_or_eq_chain(&self) -> bool {
        matches!(self.predicate, SimplePredicate::OrEqChain { .. })
    }

    /// Returns the literal of a bare comparison, without discarding compound conditions.
    pub fn literal_value(&self) -> Option<Value> {
        let SimplePredicate::Comparison { literal, .. } = &self.predicate else {
            return None;
        };
        Value::try_from(ScalarValue::try_from_array(literal.get().0, 0).ok()?).ok()
    }

    /// Returns equality-OR literals, or None for other predicates or unsupported values.
    pub fn literal_list_values(&self) -> Option<Vec<Value>> {
        let SimplePredicate::OrEqChain { literals } = &self.predicate else {
            return None;
        };
        literals
            .iter()
            .map(|literal| {
                Value::try_from(ScalarValue::try_from_array(literal.get().0, 0).ok()?).ok()
            })
            .collect()
    }

    pub fn evaluate_scalar(&self, input: &ScalarValue) -> Result<bool> {
        let input = input
            .to_scalar()
            .with_context(|_| ToArrowScalarSnafu { v: input.clone() })?;
        Ok(self.evaluate_datum(&input, 1)?.value(0))
    }

    pub fn evaluate_array(&self, input: &ArrayRef) -> Result<BooleanBuffer> {
        self.evaluate_datum(input, input.len())
    }

    pub fn evaluate_vector(&self, input: &VectorRef) -> Result<BooleanBuffer> {
        self.evaluate_array(&input.to_arrow_array())
    }

    fn evaluate_datum(&self, input: &impl Datum, len: usize) -> Result<BooleanBuffer> {
        let result = self.predicate.evaluate(input, len)?;
        Ok(boolean_array_to_scan_mask(&result).values().clone())
    }

    /// Adapts timestamp predicates to an older SST's timestamp unit without rounding
    /// away boundary rows. Nonrepresentable equality literals cannot match a row.
    pub fn cast_timestamp_unit(&self, target: &ConcreteDataType) -> Option<TimestampUnitCast> {
        let DataType::Timestamp(target_unit, _) = target.as_arrow_type() else {
            return None;
        };
        Some(match self.predicate.cast_timestamp_unit(target_unit)? {
            SimplePredicate::Constant(Some(true)) => TimestampUnitCast::Matched,
            SimplePredicate::Constant(_) => TimestampUnitCast::Pruned,
            predicate => TimestampUnitCast::Filter(Self {
                column_name: self.column_name.clone(),
                predicate,
            }),
        })
    }
}

/// The result of casting a [`SimpleFilterEvaluator`] to a different timestamp
/// unit, preserving the predicate's semantics on rows stored in that unit.
/// See [`SimpleFilterEvaluator::cast_timestamp_unit`].
#[derive(Debug, Clone)]
pub enum TimestampUnitCast {
    /// The cast filter; evaluates column values stored in the target unit.
    Filter(SimpleFilterEvaluator),
    /// No value in the target unit satisfies the filter.
    Pruned,
    /// Every value in the target unit satisfies the filter.
    Matched,
}

/// Evaluate the predicate on the input [RecordBatch], and return a new [RecordBatch].
/// Copy from datafusion::physical_plan::src::filter.rs
pub fn batch_filter(
    batch: &RecordBatch,
    predicate: &Arc<dyn PhysicalExpr>,
) -> DfResult<RecordBatch> {
    predicate
        .evaluate(batch)
        .and_then(|v| v.into_array(batch.num_rows()))
        .and_then(|array| {
            let filter_array = match as_boolean_array(&array) {
                Ok(boolean_array) => Ok(boolean_array.clone()),
                Err(_) => {
                    let Ok(null_array) = as_null_array(&array) else {
                        return internal_err!(
                            "Cannot create filter_array from non-boolean predicates"
                        );
                    };

                    // if the predicate is null, then the result is also null
                    Ok::<BooleanArray, DataFusionError>(BooleanArray::new_null(null_array.len()))
                }
            }?;
            Ok(filter_record_batch(
                batch,
                &boolean_array_to_scan_mask(&filter_array),
            )?)
        })
}

/// Converts nullable SQL predicate values to a scan mask, where `NULL` is `false`.
fn boolean_array_to_scan_mask(array: &BooleanArray) -> BooleanArray {
    if array.null_count() == 0 {
        return array.clone();
    }

    let mut values = BooleanBufferBuilder::new(array.len());
    for index in 0..array.len() {
        values.append(array.is_valid(index) && array.value(index));
    }
    BooleanArray::new(values.into(), None)
}
