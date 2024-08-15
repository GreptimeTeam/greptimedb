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

//! for declare Expression in dataflow, including map, reduce, id and join(TODO!) etc.

mod df_func;
pub(crate) mod error;
mod func;
mod id;
mod linear;
mod relation;
mod scalar;
mod signature;

use arrow::array::BooleanArray;
use datatypes::prelude::{ConcreteDataType, DataType};
use datatypes::value::Value;
use datatypes::vectors::VectorRef;
pub(crate) use df_func::{DfScalarFunction, RawDfScalarFn};
pub(crate) use error::{EvalError, InvalidArgumentSnafu};
pub(crate) use func::{BinaryFunc, UnaryFunc, UnmaterializableFunc, VariadicFunc};
pub(crate) use id::{GlobalId, Id, LocalId};
use itertools::Itertools;
pub(crate) use linear::{MapFilterProject, MfpPlan, SafeMfpPlan};
pub(crate) use relation::{AggregateExpr, AggregateFunc};
pub(crate) use scalar::{ScalarExpr, TypedExpr};
use snafu::{ensure, OptionExt, ResultExt};

use crate::expr::error::{DataTypeSnafu, TypeMismatchSnafu};

/// A batch of vectors with the same length but without schema, only useful in dataflow
///
/// somewhere cheap to clone since it just contains a list of VectorRef(which is a `Arc`).
#[derive(Debug, Clone)]
pub struct Batch {
    batch: Vec<VectorRef>,
    row_count: usize,
    /// describe if corresponding rows in batch is insert or delete, None means all rows are insert
    diffs: Option<VectorRef>,
}

impl Default for Batch {
    fn default() -> Self {
        Self::empty()
    }
}

impl Batch {
    pub fn empty() -> Self {
        Self {
            batch: vec![],
            row_count: 0,
            diffs: None,
        }
    }
    pub fn new(batch: Vec<VectorRef>, row_count: usize) -> Self {
        Self {
            batch,
            row_count,
            diffs: None,
        }
    }

    pub fn batch(&self) -> &[VectorRef] {
        &self.batch
    }

    pub fn batch_mut(&mut self) -> &mut Vec<VectorRef> {
        &mut self.batch
    }

    pub fn row_count(&self) -> usize {
        self.row_count
    }

    pub fn column_count(&self) -> usize {
        self.batch.len()
    }

    pub fn get_row(&self, idx: usize) -> Result<Vec<Value>, EvalError> {
        ensure!(
            idx < self.row_count,
            InvalidArgumentSnafu {
                reason: format!(
                    "Expect row index to be less than {}, found {}",
                    self.row_count, idx
                )
            }
        );
        Ok(self.batch.iter().map(|v| v.get(idx)).collect_vec())
    }

    /// Slices the `Batch`, returning a new `Batch`.
    ///
    /// # Panics
    /// This function panics if `offset + length > self.row_count()`.
    pub fn slice(&self, offset: usize, length: usize) -> Batch {
        let batch = self
            .batch()
            .iter()
            .map(|v| v.slice(offset, length))
            .collect_vec();
        Batch::new(batch, length)
    }

    /// append another batch to self
    pub fn append_batch(&mut self, other: Batch) -> Result<(), EvalError> {
        ensure!(
            self.batch.len() == other.batch.len(),
            InvalidArgumentSnafu {
                reason: format!(
                    "Expect two batch to have same numbers of column, found {} and {} columns",
                    self.batch.len(),
                    other.batch.len()
                )
            }
        );

        let batch_builders = self
            .batch
            .iter()
            .map(|v| {
                v.data_type()
                    .create_mutable_vector(self.row_count() + other.row_count())
            })
            .collect_vec();

        let mut result = vec![];
        let zelf_row_count = self.row_count();
        let other_row_count = other.row_count();
        for (idx, mut builder) in batch_builders.into_iter().enumerate() {
            builder
                .extend_slice_of(self.batch()[idx].as_ref(), 0, zelf_row_count)
                .context(DataTypeSnafu {
                    msg: "Failed to extend vector",
                })?;
            builder
                .extend_slice_of(other.batch()[idx].as_ref(), 0, other_row_count)
                .context(DataTypeSnafu {
                    msg: "Failed to extend vector",
                })?;
            result.push(builder.to_vector());
        }
        self.batch = result;
        self.row_count = zelf_row_count + other_row_count;
        Ok(())
    }
}

pub fn to_bool_array(input: &VectorRef) -> Result<&BooleanArray, EvalError> {
    input.as_any().downcast_ref::<BooleanArray>().context({
        TypeMismatchSnafu {
            expected: ConcreteDataType::boolean_datatype(),
            actual: input.data_type(),
        }
    })
}
