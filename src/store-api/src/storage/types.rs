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

//! Common types.

use datatypes::arrow::array::{BooleanArray, Datum, UInt64Array};

/// Represents a sequence number of data in storage. The offset of logstore can be used
/// as a sequence number.
pub type SequenceNumber = u64;

/// A range of sequence numbers.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum SequenceRange {
    Gt {
        /// Exclusive lower bound
        min: SequenceNumber,
    },
    LtEq {
        /// Inclusive upper bound
        max: SequenceNumber,
    },
    GtLtEq {
        /// Exclusive lower bound
        min: SequenceNumber,
        /// Inclusive upper bound
        max: SequenceNumber,
    },
}

impl SequenceRange {
    pub fn new(min: Option<SequenceNumber>, max: Option<SequenceNumber>) -> Option<Self> {
        match (min, max) {
            (Some(min), Some(max)) => Some(SequenceRange::GtLtEq { min, max }),
            (Some(min), None) => Some(SequenceRange::Gt { min }),
            (None, Some(max)) => Some(SequenceRange::LtEq { max }),
            (None, None) => None,
        }
    }

    pub fn filter(
        &self,
        seqs: &dyn Datum,
    ) -> Result<BooleanArray, datatypes::arrow::error::ArrowError> {
        match self {
            SequenceRange::Gt { min } => {
                let min = UInt64Array::new_scalar(*min);
                let pred = datafusion_common::arrow::compute::kernels::cmp::gt(seqs, &min)?;
                Ok(pred)
            }
            SequenceRange::LtEq { max } => {
                let max = UInt64Array::new_scalar(*max);
                let pred = datafusion_common::arrow::compute::kernels::cmp::lt_eq(seqs, &max)?;
                Ok(pred)
            }
            SequenceRange::GtLtEq { min, max } => {
                let min = UInt64Array::new_scalar(*min);
                let max = UInt64Array::new_scalar(*max);
                let pred_min = datafusion_common::arrow::compute::kernels::cmp::gt(seqs, &min)?;
                let pred_max = datafusion_common::arrow::compute::kernels::cmp::lt_eq(seqs, &max)?;
                datafusion_common::arrow::compute::kernels::boolean::and(&pred_min, &pred_max)
            }
        }
    }
}
