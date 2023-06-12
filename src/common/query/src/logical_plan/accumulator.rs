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

//! Accumulator module contains the trait definition for aggregation function's accumulators.

use std::fmt::Debug;
use std::sync::Arc;

use datafusion_common::Result as DfResult;
use datafusion_expr::Accumulator as DfAccumulator;
use datatypes::arrow::array::ArrayRef;
use datatypes::prelude::*;
use datatypes::vectors::{Helper as VectorHelper, VectorRef};
use snafu::ResultExt;

use crate::error::{self, Error, FromScalarValueSnafu, IntoVectorSnafu, Result};
use crate::prelude::*;

pub type AggregateFunctionCreatorRef = Arc<dyn AggregateFunctionCreator>;

/// An accumulator represents a stateful object that lives throughout the evaluation of multiple rows and
/// generically accumulates values.
///
/// An accumulator knows how to:
/// * update its state from inputs via `update_batch`
/// * convert its internal state to a vector of scalar values
/// * update its state from multiple accumulators' states via `merge_batch`
/// * compute the final value from its internal state via `evaluate`
///
/// Modified from DataFusion.
pub trait Accumulator: Send + Sync + Debug {
    /// Returns the state of the accumulator at the end of the accumulation.
    // in the case of an average on which we track `sum` and `n`, this function should return a vector
    // of two values, sum and n.
    fn state(&self) -> Result<Vec<Value>>;

    /// updates the accumulator's state from a vector of arrays.
    fn update_batch(&mut self, values: &[VectorRef]) -> Result<()>;

    /// updates the accumulator's state from a vector of states.
    fn merge_batch(&mut self, states: &[VectorRef]) -> Result<()>;

    /// returns its value based on its current state.
    fn evaluate(&self) -> Result<Value>;
}

/// An `AggregateFunctionCreator` dynamically creates `Accumulator`.
///
/// An `AggregateFunctionCreator` often has a companion struct, that
/// can store the input data types (impl [AggrFuncTypeStore]), and knows the output and states
/// types of an Accumulator.
pub trait AggregateFunctionCreator: AggrFuncTypeStore {
    /// Create a function that can create a new accumulator with some input data type.
    fn creator(&self) -> AccumulatorCreatorFunction;

    /// Get the Accumulator's output data type.
    fn output_type(&self) -> Result<ConcreteDataType>;

    /// Get the Accumulator's state data types.
    fn state_types(&self) -> Result<Vec<ConcreteDataType>>;
}

/// `AggrFuncTypeStore` stores the aggregate function's input data's types.
///
/// When creating Accumulator generically, we have to know the input data's types.
/// However, DataFusion does not provide the input data's types at the time of creating Accumulator.
/// To solve the problem, we store the datatypes upfront here.
pub trait AggrFuncTypeStore: Send + Sync + Debug {
    /// Get the input data types of the Accumulator.
    fn input_types(&self) -> Result<Vec<ConcreteDataType>>;

    /// Store the input data types that are provided by DataFusion at runtime (when it is evaluating
    /// return type function).
    fn set_input_types(&self, input_types: Vec<ConcreteDataType>) -> Result<()>;
}

pub fn make_accumulator_function(
    creator: Arc<dyn AggregateFunctionCreator>,
) -> AccumulatorFunctionImpl {
    Arc::new(move || {
        let input_types = creator.input_types()?;
        let creator = creator.creator();
        creator(&input_types)
    })
}

pub fn make_return_function(creator: Arc<dyn AggregateFunctionCreator>) -> ReturnTypeFunction {
    Arc::new(move |input_types| {
        creator.set_input_types(input_types.to_vec())?;

        let output_type = creator.output_type()?;
        Ok(Arc::new(output_type))
    })
}

pub fn make_state_function(creator: Arc<dyn AggregateFunctionCreator>) -> StateTypeFunction {
    Arc::new(move |_| Ok(Arc::new(creator.state_types()?)))
}

/// A wrapper type for our Accumulator to DataFusion's Accumulator,
/// so to make our Accumulator able to be executed by DataFusion query engine.
#[derive(Debug)]
pub struct DfAccumulatorAdaptor {
    accumulator: Box<dyn Accumulator>,
    creator: AggregateFunctionCreatorRef,
}

impl DfAccumulatorAdaptor {
    pub fn new(accumulator: Box<dyn Accumulator>, creator: AggregateFunctionCreatorRef) -> Self {
        Self {
            accumulator,
            creator,
        }
    }
}

impl DfAccumulator for DfAccumulatorAdaptor {
    fn state(&self) -> DfResult<Vec<ScalarValue>> {
        let state_values = self.accumulator.state()?;
        let state_types = self.creator.state_types()?;
        if state_values.len() != state_types.len() {
            return error::BadAccumulatorImplSnafu {
                err_msg: format!("Accumulator {self:?} returned state values size do not match its state types size."),
            }
            .fail()?;
        }
        Ok(state_values
            .into_iter()
            .zip(state_types.iter())
            .map(|(v, t)| v.try_to_scalar_value(t).context(error::ToScalarValueSnafu))
            .collect::<Result<Vec<_>>>()?)
    }

    fn update_batch(&mut self, values: &[ArrayRef]) -> DfResult<()> {
        let vectors = VectorHelper::try_into_vectors(values).context(FromScalarValueSnafu)?;
        self.accumulator.update_batch(&vectors)?;
        Ok(())
    }

    fn merge_batch(&mut self, states: &[ArrayRef]) -> DfResult<()> {
        let mut vectors = Vec::with_capacity(states.len());
        for array in states.iter() {
            vectors.push(
                VectorHelper::try_into_vector(array).context(IntoVectorSnafu {
                    data_type: array.data_type().clone(),
                })?,
            );
        }
        self.accumulator.merge_batch(&vectors)?;
        Ok(())
    }

    fn evaluate(&self) -> DfResult<ScalarValue> {
        let value = self.accumulator.evaluate()?;
        let output_type = self.creator.output_type()?;
        let scalar_value = value
            .try_to_scalar_value(&output_type)
            .context(error::ToScalarValueSnafu)
            .map_err(Error::from)?;
        Ok(scalar_value)
    }

    fn size(&self) -> usize {
        0
    }
}
