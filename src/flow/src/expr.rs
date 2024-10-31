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
pub(crate) mod func;
mod id;
mod linear;
pub(crate) mod relation;
mod scalar;
mod signature;

use arrow::compute::FilterBuilder;
use datatypes::prelude::DataType;
use datatypes::value::Value;
use datatypes::vectors::{BooleanVector, Helper, VectorRef};
pub(crate) use df_func::{DfScalarFunction, RawDfScalarFn};
pub(crate) use error::{EvalError, InvalidArgumentSnafu};
pub(crate) use func::{BinaryFunc, UnaryFunc, UnmaterializableFunc, VariadicFunc};
pub(crate) use id::{GlobalId, Id, LocalId};
use itertools::Itertools;
pub(crate) use linear::{MapFilterProject, MfpPlan, SafeMfpPlan};
pub(crate) use relation::{Accum, Accumulator, AggregateExpr, AggregateFunc};
pub(crate) use scalar::{ScalarExpr, TypedExpr};
use snafu::{ensure, ResultExt};

use crate::expr::error::{ArrowSnafu, DataTypeSnafu};
use crate::repr::Diff;

pub const TUMBLE_START: &str = "tumble_start";
pub const TUMBLE_END: &str = "tumble_end";

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

impl PartialEq for Batch {
    fn eq(&self, other: &Self) -> bool {
        let mut batch_eq = true;
        if self.batch.len() != other.batch.len() {
            return false;
        }
        for (left, right) in self.batch.iter().zip(other.batch.iter()) {
            batch_eq = batch_eq
                && <dyn arrow::array::Array>::eq(&left.to_arrow_array(), &right.to_arrow_array());
        }

        let diff_eq = match (&self.diffs, &other.diffs) {
            (Some(left), Some(right)) => {
                <dyn arrow::array::Array>::eq(&left.to_arrow_array(), &right.to_arrow_array())
            }
            (None, None) => true,
            _ => false,
        };
        batch_eq && diff_eq && self.row_count == other.row_count
    }
}

impl Eq for Batch {}

impl Default for Batch {
    fn default() -> Self {
        Self::empty()
    }
}

impl Batch {
    pub fn try_from_rows(rows: Vec<crate::repr::Row>) -> Result<Self, EvalError> {
        if rows.is_empty() {
            return Ok(Self::empty());
        }
        let len = rows.len();
        let mut builder = rows
            .first()
            .unwrap()
            .iter()
            .map(|v| v.data_type().create_mutable_vector(len))
            .collect_vec();
        for row in rows {
            ensure!(
                row.len() == builder.len(),
                InvalidArgumentSnafu {
                    reason: format!(
                        "row length not match, expect {}, found {}",
                        builder.len(),
                        row.len()
                    )
                }
            );
            for (idx, value) in row.iter().enumerate() {
                builder[idx]
                    .try_push_value_ref(value.as_value_ref())
                    .context(DataTypeSnafu {
                        msg: "Failed to convert rows to columns",
                    })?;
            }
        }

        let columns = builder.into_iter().map(|mut b| b.to_vector()).collect_vec();
        let batch = Self::try_new(columns, len)?;
        Ok(batch)
    }

    pub fn empty() -> Self {
        Self {
            batch: vec![],
            row_count: 0,
            diffs: None,
        }
    }
    pub fn try_new(batch: Vec<VectorRef>, row_count: usize) -> Result<Self, EvalError> {
        ensure!(
            batch.iter().map(|v| v.len()).all_equal()
                && batch.first().map(|v| v.len() == row_count).unwrap_or(true),
            InvalidArgumentSnafu {
                reason: "All columns should have same length".to_string()
            }
        );
        Ok(Self {
            batch,
            row_count,
            diffs: None,
        })
    }

    pub fn new_unchecked(batch: Vec<VectorRef>, row_count: usize) -> Self {
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

    pub fn set_row_count(&mut self, row_count: usize) {
        self.row_count = row_count;
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
        let mut ret = Vec::with_capacity(self.column_count());
        ret.extend(self.batch.iter().map(|v| v.get(idx)));
        Ok(ret)
    }

    /// Slices the `Batch`, returning a new `Batch`.
    pub fn slice(&self, offset: usize, length: usize) -> Result<Batch, EvalError> {
        let batch = self
            .batch()
            .iter()
            .map(|v| v.slice(offset, length))
            .collect_vec();
        Batch::try_new(batch, length)
    }

    /// append another batch to self
    ///
    /// NOTE: This is expensive since it will create new vectors for each column
    pub fn append_batch(&mut self, other: Batch) -> Result<(), EvalError> {
        ensure!(
            self.batch.len() == other.batch.len()
                || self.batch.is_empty()
                || other.batch.is_empty(),
            InvalidArgumentSnafu {
                reason: format!(
                    "Expect two batch to have same numbers of column, found {} and {} columns",
                    self.batch.len(),
                    other.batch.len()
                )
            }
        );

        if self.batch.is_empty() {
            self.batch = other.batch;
            self.row_count = other.row_count;
            return Ok(());
        } else if other.batch.is_empty() {
            return Ok(());
        }

        let dts = if self.batch.is_empty() {
            other.batch.iter().map(|v| v.data_type()).collect_vec()
        } else {
            self.batch.iter().map(|v| v.data_type()).collect_vec()
        };

        let batch_builders = dts
            .iter()
            .map(|dt| dt.create_mutable_vector(self.row_count() + other.row_count()))
            .collect_vec();

        let mut result = vec![];
        let self_row_count = self.row_count();
        let other_row_count = other.row_count();
        for (idx, mut builder) in batch_builders.into_iter().enumerate() {
            builder
                .extend_slice_of(self.batch()[idx].as_ref(), 0, self_row_count)
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
        self.row_count = self_row_count + other_row_count;
        Ok(())
    }

    /// filter the batch with given predicate
    pub fn filter(&self, predicate: &BooleanVector) -> Result<Self, EvalError> {
        let len = predicate.as_boolean_array().true_count();
        let filter_builder = FilterBuilder::new(predicate.as_boolean_array()).optimize();
        let filter_pred = filter_builder.build();
        let filtered = self
            .batch()
            .iter()
            .map(|col| filter_pred.filter(col.to_arrow_array().as_ref()))
            .try_collect::<_, Vec<_>, _>()
            .context(ArrowSnafu {
                context: "Failed to filter val batches",
            })?;
        let res_vector = Helper::try_into_vectors(&filtered).context(DataTypeSnafu {
            msg: "can't convert arrow array to vector",
        })?;
        Self::try_new(res_vector, len)
    }
}

/// Vector with diff to note the insert and delete
pub(crate) struct VectorDiff {
    vector: VectorRef,
    diff: Option<VectorRef>,
}

impl From<VectorRef> for VectorDiff {
    fn from(vector: VectorRef) -> Self {
        Self { vector, diff: None }
    }
}

impl VectorDiff {
    fn len(&self) -> usize {
        self.vector.len()
    }

    fn try_new(vector: VectorRef, diff: Option<VectorRef>) -> Result<Self, EvalError> {
        ensure!(
            diff.as_ref()
                .map_or(true, |diff| diff.len() == vector.len()),
            InvalidArgumentSnafu {
                reason: "Length of vector and diff should be the same"
            }
        );
        Ok(Self { vector, diff })
    }
}

impl IntoIterator for VectorDiff {
    type Item = (Value, Diff);
    type IntoIter = VectorDiffIter;

    fn into_iter(self) -> Self::IntoIter {
        VectorDiffIter {
            vector: self.vector,
            diff: self.diff,
            idx: 0,
        }
    }
}

/// iterator for VectorDiff
pub(crate) struct VectorDiffIter {
    vector: VectorRef,
    diff: Option<VectorRef>,
    idx: usize,
}

impl std::iter::Iterator for VectorDiffIter {
    type Item = (Value, Diff);

    fn next(&mut self) -> Option<Self::Item> {
        if self.idx >= self.vector.len() {
            return None;
        }
        let value = self.vector.get(self.idx);
        // +1 means insert, -1 means delete, and default to +1 insert when diff is not provided
        let diff = if let Some(diff) = self.diff.as_ref() {
            if let Ok(diff_at) = diff.get(self.idx).try_into() {
                diff_at
            } else {
                common_telemetry::warn!("Invalid diff value at index {}", self.idx);
                return None;
            }
        } else {
            1
        };

        self.idx += 1;
        Some((value, diff))
    }
}
