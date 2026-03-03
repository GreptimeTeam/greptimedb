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

use datatypes::prelude::ConcreteDataType;
use datatypes::value::Value;
use partition::expr::{Operand, PartitionExpr, RestrictedOp};
use rand::Rng;
use snafu::ensure;

use crate::context::TableContext;
use crate::error::{self, Result};
use crate::ir::{Ident, generate_random_value};

/// A partitioning scheme that divides a single column into multiple ranges based on provided bounds.
///
/// `SimplePartitions` is designed for range-based partitioning on a given column using explicit boundary values.
/// Each partition is represented by an interval. With `n` bounds, there are `n+1` resulting partitions.
///
/// # Example
///
/// Partitioning by the column `"age"` with bounds `[10, 20]` generates the following partitions:
/// - Partition 1: `age < 10`
/// - Partition 2: `age >= 10 AND age < 20`
/// - Partition 3: `age >= 20`
///
/// # Fields
/// - `column_name`: The name of the column used for partitioning.
/// - `bounds`: The partition boundary values; must be sorted for correct partitioning logic.
#[derive(Clone)]
pub struct SimplePartitions {
    /// The column to partition by.
    pub column_name: Ident,
    /// The boundaries that define each partition range.
    ///
    /// With `k` bounds, the following partitions are created:
    /// - `< bound[0]`
    /// - `[bound[0], bound[1])`
    /// - ...
    /// - `>= bound[k-1]`
    pub bounds: Vec<Value>,
}

impl SimplePartitions {
    pub fn new(column_name: Ident, bounds: Vec<Value>) -> Self {
        Self {
            column_name,
            bounds,
        }
    }

    /// Generates partition expressions for the defined bounds on a single column.
    ///
    /// Returns a vector of `PartitionExpr` representing all resulting partitions.
    pub fn generate(&self) -> Result<Vec<PartitionExpr>> {
        ensure!(
            !self.bounds.is_empty(),
            error::UnexpectedSnafu {
                violated: "partition bounds must not be empty".to_string(),
            }
        );
        let mut sorted = self.bounds.clone();
        sorted.sort();
        let mut exprs = Vec::with_capacity(sorted.len() + 1);
        let first_bound = sorted[0].clone();
        let column_name = self.column_name.value.clone();
        exprs.push(PartitionExpr::new(
            Operand::Column(column_name.clone()),
            RestrictedOp::Lt,
            Operand::Value(first_bound),
        ));
        for bound_idx in 1..sorted.len() {
            exprs.push(PartitionExpr::new(
                Operand::Expr(PartitionExpr::new(
                    Operand::Column(column_name.clone()),
                    RestrictedOp::GtEq,
                    Operand::Value(sorted[bound_idx - 1].clone()),
                )),
                RestrictedOp::And,
                Operand::Expr(PartitionExpr::new(
                    Operand::Column(column_name.clone()),
                    RestrictedOp::Lt,
                    Operand::Value(sorted[bound_idx].clone()),
                )),
            ));
        }
        let last_bound = sorted.last().unwrap().clone();
        exprs.push(PartitionExpr::new(
            Operand::Column(column_name.clone()),
            RestrictedOp::GtEq,
            Operand::Value(last_bound),
        ));

        Ok(exprs)
    }

    /// Reconstructs a `SimplePartitions` instance from a slice of `PartitionExpr`.
    ///
    /// This extracts the upper-bound partition values for a given column from a list of
    /// partition expressions (typically produced by `generate`). Only expressions that
    /// define an upper bound for the column are included. Must not be passed an empty slice.
    ///
    /// # Arguments
    ///
    /// * `column_name` - The name of the column to partition by.
    /// * `exprs` - A list of partition expressions, each specifying a partition's bounding condition.
    ///
    /// # Errors
    ///
    /// Returns an error if `exprs` is empty or if any expression cannot be parsed for bounds.
    ///
    /// # Returns
    ///
    /// A [`SimplePartitions`] value, where the bounds vector contains each extracted upper bound.
    pub fn from_exprs(column_name: Ident, exprs: &[PartitionExpr]) -> Result<Self> {
        ensure!(
            !exprs.is_empty(),
            error::UnexpectedSnafu {
                violated: "partition exprs must not be empty".to_string(),
            }
        );
        let mut bounds = Vec::new();
        for expr in exprs {
            if let Some(bound) = extract_upper_bound(&column_name.value, expr)? {
                bounds.push(bound);
            }
        }
        Ok(Self::new(column_name, bounds))
    }

    /// Reconstructs a `SimplePartitions` instance from a `TableContext`.
    pub fn from_table_ctx(table_ctx: &TableContext) -> Result<Self> {
        let partition_def = table_ctx
            .partition
            .as_ref()
            .expect("expected partition def");
        Self::from_exprs(partition_def.columns[0].clone(), &partition_def.exprs)
    }

    /// Inserts a new bound into the partition bounds and returns the index of the new bound.
    pub fn insert_bound(&mut self, bound: Value) -> Result<usize> {
        ensure!(
            !self.bounds.contains(&bound),
            error::UnexpectedSnafu {
                violated: format!("duplicate bound: {bound}"),
            }
        );
        self.bounds.push(bound.clone());
        self.bounds.sort();

        let insert_pos = self.bounds.binary_search(&bound).unwrap();
        Ok(insert_pos)
    }

    /// Removes a bound at the specified index and returns the removed bound.
    pub fn remove_bound(&mut self, idx: usize) -> Result<Value> {
        ensure!(
            idx < self.bounds.len(),
            error::UnexpectedSnafu {
                violated: format!("index out of bounds: {idx}"),
            }
        );
        Ok(self.bounds.remove(idx))
    }
}

fn extract_upper_bound(column: &str, expr: &PartitionExpr) -> Result<Option<Value>> {
    match expr.op {
        RestrictedOp::Lt | RestrictedOp::LtEq => {
            let value = extract_column_value(column, expr)?;
            Ok(Some(value))
        }
        RestrictedOp::Gt | RestrictedOp::GtEq => Ok(None),
        RestrictedOp::And => {
            let left = extract_expr_operand(expr.lhs.as_ref())?;
            let right = extract_expr_operand(expr.rhs.as_ref())?;
            let (left_op, left_value) = extract_bound_operand(column, left)?;
            let (right_op, right_value) = extract_bound_operand(column, right)?;
            let upper = match (is_upper_op(&left_op), is_upper_op(&right_op)) {
                (true, false) => Some(left_value),
                (false, true) => Some(right_value),
                _ => None,
            };
            Ok(upper)
        }
        _ => error::UnexpectedSnafu {
            violated: format!("unsupported partition op: {:?}", expr.op),
        }
        .fail(),
    }
}

fn extract_expr_operand(operand: &Operand) -> Result<&PartitionExpr> {
    match operand {
        Operand::Expr(expr) => Ok(expr),
        _ => error::UnexpectedSnafu {
            violated: "expected partition expr operand".to_string(),
        }
        .fail(),
    }
}

fn extract_bound_operand(column: &str, expr: &PartitionExpr) -> Result<(RestrictedOp, Value)> {
    let lhs = expr.lhs.as_ref();
    let rhs = expr.rhs.as_ref();
    match (lhs, rhs) {
        (Operand::Column(col), Operand::Value(value)) if col == column => {
            Ok((expr.op.clone(), value.clone()))
        }
        _ => error::UnexpectedSnafu {
            violated: format!("unexpected partition expr for column: {column}"),
        }
        .fail(),
    }
}

fn extract_column_value(column: &str, expr: &PartitionExpr) -> Result<Value> {
    let (op, value) = extract_bound_operand(column, expr)?;
    ensure!(
        is_upper_op(&op),
        error::UnexpectedSnafu {
            violated: format!("expected upper bound op, got: {:?}", op),
        }
    );
    Ok(value)
}

fn is_upper_op(op: &RestrictedOp) -> bool {
    matches!(op, RestrictedOp::Lt | RestrictedOp::LtEq)
}

/// Generates a unique partition bound that is not in the given bounds.
pub fn generate_unique_bound<R: Rng + 'static>(
    rng: &mut R,
    datatype: &ConcreteDataType,
    bounds: &[Value],
) -> Result<Value> {
    for _ in 0..16 {
        let candidate = generate_random_value(rng, datatype, None);
        if !bounds.contains(&candidate) {
            return Ok(candidate);
        }
    }
    error::UnexpectedSnafu {
        violated: "unable to generate unique partition bound".to_string(),
    }
    .fail()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_simple_partitions() {
        let partitions =
            SimplePartitions::new(Ident::new("age"), vec![Value::from(10), Value::from(20)]);
        let exprs = partitions.generate().unwrap();
        assert_eq!(exprs.len(), 3);
        assert_eq!(exprs[0].to_string(), "age < 10");
        assert_eq!(exprs[1].to_string(), "age >= 10 AND age < 20");
        assert_eq!(exprs[2].to_string(), "age >= 20");
    }

    #[test]
    fn test_simple_partitions_from_exprs() {
        let partitions =
            SimplePartitions::new(Ident::new("age"), vec![Value::from(10), Value::from(20)]);
        let exprs = partitions.generate().unwrap();
        let partitions = SimplePartitions::from_exprs(Ident::new("age"), &exprs).unwrap();
        assert_eq!(partitions.bounds, vec![Value::from(10), Value::from(20)]);
    }
}
