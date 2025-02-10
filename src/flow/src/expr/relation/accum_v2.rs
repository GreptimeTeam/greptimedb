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

//! new accumulator trait that is more flexible and can be used in the future for more complex accumulators

use std::any::type_name;
use std::fmt::Display;

use datatypes::value::Value;
use datatypes::vectors::VectorRef;
use serde::{Deserialize, Serialize};
use snafu::ensure;

use crate::expr::error::{InternalSnafu, TryFromValueSnafu};
use crate::expr::EvalError;

///  Basically a copy of datafusion's Accumulator, but with a few modifications
/// to accomodate our needs in flow and keep the upgradability of datafusion
pub trait AccumulatorV2: Send + Sync + std::fmt::Debug {
    /// Updates the accumulator’s state from its input.
    fn update_batch(&mut self, values: &[VectorRef]) -> Result<(), EvalError>;

    /// Returns the current aggregate value, NOT consuming the internal state, so it can be called multiple times.
    fn evaluate(&self) -> Result<Value, EvalError>;

    /// Returns the allocated size required for this accumulator, in bytes, including Self.
    fn size(&self) -> usize;

    /// Returns the intermediate state of the accumulator, consuming the intermediate state.
    fn into_state(self) -> Result<Vec<Value>, EvalError>;

    /// Merges the states of multiple accumulators into this accumulator.
    /// The states array passed was formed by concatenating the results of calling `Self::into_state` on zero or more other Accumulator instances.
    fn merge_batch(&mut self, states: &[VectorRef]) -> Result<(), EvalError>;

    /// Retracts (removed) an update (caused by the given inputs) to accumulator’s state.
    ///
    /// currently unused, but will be used in the future for i.e. windowed aggregates
    fn retract_batch(&mut self, _values: &[VectorRef]) -> Result<(), EvalError> {
        InternalSnafu {
            reason: format!(
                "retract_batch not implemented for this accumulator {:?}",
                self
            ),
        }
        .fail()
    }

    /// Does the accumulator support incrementally updating its value by removing values.
    fn supports_retract_batch(&self) -> bool {
        false
    }
}

fn fail_accum<T>() -> EvalError {
    InternalSnafu {
        reason: format!(
            "list of values exhausted before a accum of type {} can be build from it",
            type_name::<T>()
        ),
    }
    .build()
}

fn err_try_from_val<T: Display>(reason: T) -> EvalError {
    TryFromValueSnafu {
        msg: reason.to_string(),
    }
    .build()
}
