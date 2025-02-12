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

use datafusion::logical_expr::Accumulator as DfAccumulator;
use datatypes::prelude::{ConcreteDataType as CDT, DataType};
use datatypes::value::Value;
use datatypes::vectors::VectorRef;
use snafu::{ensure, OptionExt, ResultExt};

use crate::expr::error::{DataTypeSnafu, DatafusionSnafu, InternalSnafu, TryFromValueSnafu};
use crate::expr::EvalError;

///  Basically a copy of datafusion's Accumulator, but with a few modifications
/// to accomodate our needs in flow and keep the upgradability of datafusion
pub trait AccumulatorV2: Send + Sync + std::fmt::Debug {
    /// Updates the accumulator’s state from its input.
    fn update_batch(&mut self, values: &[VectorRef]) -> Result<(), EvalError>;

    /// Returns the current aggregate value, NOT consuming the internal state, so it can be called multiple times.
    fn evaluate(&mut self) -> Result<Value, EvalError>;

    /// Returns the allocated size required for this accumulator, in bytes, including Self.
    fn size(&self) -> usize;

    /// Returns the intermediate state of the accumulator, consuming the intermediate state.
    ///
    /// note that Value::Null's type is unknown, so (Value, ConcreteDataType) is used instead of just Value
    fn into_state(self) -> Result<Vec<(Value, CDT)>, EvalError>;

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

/// Adapter for several hand-picked datafusion accumulators that can be used in flow
///
/// i.e: can call evaluate multiple times.
#[derive(Debug)]
pub struct DfAccumulatorAdapter {
    /// accumulator that is wrapped in a mutex to allow for evaluation
    inner: Box<dyn DfAccumulator>,
}

pub trait AcceptDfAccumulator: DfAccumulator {}

impl AcceptDfAccumulator for datafusion::functions_aggregate::min_max::MaxAccumulator {}

impl AcceptDfAccumulator for datafusion::functions_aggregate::min_max::MinAccumulator {}

impl DfAccumulatorAdapter {
    /// create a new accumulator from a datafusion accumulator without checking if it is supported in flow
    pub fn new_unchecked(acc: Box<dyn DfAccumulator>) -> Self {
        Self { inner: acc }
    }

    pub fn new<T: AcceptDfAccumulator + 'static>(acc: T) -> Self {
        Self::new_unchecked(Box::new(acc))
    }
}

impl AccumulatorV2 for DfAccumulatorAdapter {
    fn update_batch(&mut self, values: &[VectorRef]) -> Result<(), EvalError> {
        let values = values
            .iter()
            .map(|v| v.to_arrow_array().clone())
            .collect::<Vec<_>>();
        self.inner.update_batch(&values).context(DatafusionSnafu {
            context: "failed to update batch: {}",
        })
    }

    fn evaluate(&mut self) -> Result<Value, EvalError> {
        // TODO(discord9): find a way to confirm internal state is not consumed
        let value = self.inner.evaluate().context(DatafusionSnafu {
            context: "failed to evaluate accumulator: {}",
        })?;
        let value = Value::try_from(value).context(DataTypeSnafu {
            msg: "failed to convert evaluate result from `ScalarValue` to `Value`",
        })?;
        Ok(value)
    }

    fn size(&self) -> usize {
        self.inner.size()
    }

    fn into_state(mut self) -> Result<Vec<(Value, CDT)>, EvalError> {
        let state = self.inner.state().context(DatafusionSnafu {
            context: "failed to get state: {}",
        })?;
        let state = state
            .into_iter()
            .map(|v| -> Result<_, _> {
                let dt = CDT::try_from(&v.data_type())?;
                let val = Value::try_from(v)?;
                Ok((val, dt))
            })
            .collect::<Result<Vec<_>, _>>()
            .with_context(|_| DataTypeSnafu {
                msg: "failed to convert `ScalarValue` state to `Value`",
            })?;
        Ok(state)
    }

    fn merge_batch(&mut self, states: &[VectorRef]) -> Result<(), EvalError> {
        let states = states
            .iter()
            .map(|v| v.to_arrow_array().clone())
            .collect::<Vec<_>>();
        self.inner.merge_batch(&states).context(DatafusionSnafu {
            context: "failed to merge batch",
        })
    }
}

/// Convert a list of states(from `Accumulator::into_state`)
/// to a batch of vectors(that can be feed to `Accumulator::merge_batch`)
fn states_to_batch(states: Vec<Vec<Value>>, dts: Vec<CDT>) -> Result<Vec<VectorRef>, EvalError> {
    if states.is_empty() || states[0].is_empty() {
        return Ok(vec![]);
    }
    ensure!(
        states.iter().map(|v| v.len()).all(|l| l == states[0].len()),
        InternalSnafu {
            reason: "states have different lengths"
        }
    );
    let state_len = states[0].len();
    ensure!(
        state_len == dts.len(),
        InternalSnafu {
            reason: format!(
                "states and data types have different lengths: {} != {}",
                state_len,
                dts.len()
            )
        }
    );
    let mut ret = dts
        .into_iter()
        .map(|dt| dt.create_mutable_vector(state_len))
        .collect::<Vec<_>>();
    for i in 0..state_len {
        for state in states.iter() {
            // the j-th state's i-th value
            let val = &state[i];
            ret.get_mut(i)
                .with_context(|| InternalSnafu {
                    reason: format!("failed to get mutable vector at index {}", i),
                })?
                .push_value_ref(val.as_value_ref());
        }
    }
    let ret = ret.into_iter().map(|mut v| v.to_vector()).collect();
    Ok(ret)
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
