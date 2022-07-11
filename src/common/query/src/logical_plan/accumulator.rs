//! Accumulator module contains the trait definition for aggregation function's accumulators.

use std::fmt::Debug;

use arrow::array::ArrayRef;
use datafusion_common::Result as DfResult;
use datafusion_common::ScalarValue;
use datafusion_expr::Accumulator as DfAccumulator;
use datatypes::error::Result as DtResult;
use datatypes::vectors::Helper as VectorHelper;
use datatypes::vectors::VectorRef;
use snafu::ResultExt;

use crate::error::{Error, FromScalarValueSnafu, Result};

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
    fn state(&self) -> Result<Vec<ScalarValue>>;

    /// updates the accumulator's state from a vector of arrays.
    fn update_batch(&mut self, values: &[VectorRef]) -> Result<()>;

    /// updates the accumulator's state from a vector of states.
    fn merge_batch(&mut self, states: &[VectorRef]) -> Result<()>;

    /// returns its value based on its current state.
    fn evaluate(&self) -> Result<ScalarValue>;
}

/// A wrapper newtype for our Accumulator to DataFusion's Accumulator,
/// so to make our Accumulator able to be executed by DataFusion query engine.
#[derive(Debug)]
pub struct DfAccumulatorWrapper(pub Box<dyn Accumulator>);

impl DfAccumulator for DfAccumulatorWrapper {
    fn state(&self) -> DfResult<Vec<ScalarValue>> {
        self.0.state().map_err(|e| e.into())
    }

    fn update_batch(&mut self, values: &[ArrayRef]) -> DfResult<()> {
        let vectors = try_into_vectors(values)?;
        self.0.update_batch(&vectors).map_err(|e| e.into())
    }

    fn merge_batch(&mut self, states: &[ArrayRef]) -> DfResult<()> {
        let vectors = try_into_vectors(states)?;
        self.0.merge_batch(&vectors).map_err(|e| e.into())
    }

    fn evaluate(&self) -> DfResult<ScalarValue> {
        self.0.evaluate().map_err(|e| e.into())
    }
}

fn try_into_vectors(arrays: &[ArrayRef]) -> Result<Vec<VectorRef>> {
    arrays
        .iter()
        .map(VectorHelper::try_into_vector)
        .collect::<DtResult<Vec<VectorRef>>>()
        .context(FromScalarValueSnafu)
        .map_err(Error::from)
}
