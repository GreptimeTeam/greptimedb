//! Accumulator module contains the trait definition for aggregation function's accumulators.

use std::fmt::Debug;
use std::sync::Arc;

use arrow::array::ArrayRef;
use datafusion_common::Result as DfResult;
use datafusion_expr::Accumulator as DfAccumulator;
use datatypes::prelude::*;
use datatypes::vectors::Helper as VectorHelper;
use datatypes::vectors::VectorRef;
use snafu::ResultExt;

use crate::error::{Error, FromScalarValueSnafu, IntoVectorSnafu, Result};
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
/// DataFusion does not provide the input data's types when creating Accumulator, we have to stores
/// it somewhere else ourself. So an `AggregateFunctionCreator` often has a companion struct, that
/// can store the input data types, and knows the output and states types of an Accumulator.
/// That's how we create the Accumulator generically.
pub trait AggregateFunctionCreator: Send + Sync + Debug {
    /// Create a function that can create a new accumulator with some input data type.
    fn creator(&self) -> AccumulatorCreatorFunction;

    /// Get the input data type of the Accumulator.
    fn input_types(&self) -> Result<Vec<ConcreteDataType>>;

    /// Store the input data type that is provided by DataFusion at runtime.
    fn set_input_types(&self, input_types: Vec<ConcreteDataType>) -> Result<()>;

    /// Get the Accumulator's output data type.
    fn output_type(&self) -> Result<ConcreteDataType>;

    /// Get the Accumulator's state data types.
    fn state_types(&self) -> Result<Vec<ConcreteDataType>>;
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

/// A wrapper newtype for our Accumulator to DataFusion's Accumulator,
/// so to make our Accumulator able to be executed by DataFusion query engine.
#[derive(Debug)]
pub struct DfAccumulatorAdaptor(pub Box<dyn Accumulator>);

impl DfAccumulator for DfAccumulatorAdaptor {
    fn state(&self) -> DfResult<Vec<ScalarValue>> {
        let state = self.0.state()?;
        Ok(state.into_iter().map(ScalarValue::from).collect())
    }

    fn update_batch(&mut self, values: &[ArrayRef]) -> DfResult<()> {
        let vectors = VectorHelper::try_into_vectors(values)
            .context(FromScalarValueSnafu)
            .map_err(Error::from)?;
        self.0.update_batch(&vectors).map_err(|e| e.into())
    }

    fn merge_batch(&mut self, states: &[ArrayRef]) -> DfResult<()> {
        let mut vectors = Vec::with_capacity(states.len());
        for array in states.iter() {
            vectors.push(
                VectorHelper::try_into_vector(array)
                    .context(IntoVectorSnafu {
                        data_type: array.data_type().clone(),
                    })
                    .map_err(Error::from)?,
            );
        }
        self.0.merge_batch(&vectors).map_err(|e| e.into())
    }

    fn evaluate(&self) -> DfResult<ScalarValue> {
        Ok(ScalarValue::from(self.0.evaluate()?))
    }
}
