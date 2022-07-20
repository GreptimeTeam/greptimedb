//! Udaf module contains functions and structs supporting user-defined aggregate functions.
//!
//! Modified from DataFusion.

use std::fmt::{self, Debug, Formatter};
use std::sync::Arc;

use arrow::datatypes::DataType as ArrowDataType;
use datafusion_expr::AccumulatorFunctionImplementation as DfAccumulatorFunctionImplementation;
use datafusion_expr::AggregateUDF as DfAggregateUdf;
use datafusion_expr::StateTypeFunction as DfStateTypeFunction;
use datatypes::prelude::*;

use crate::function::{
    to_df_return_type, AccumulatorFunctionImpl, ReturnTypeFunction, StateTypeFunction,
};
use crate::logical_plan::accumulator::DfAccumulatorAdaptor;
use crate::signature::Signature;

/// Logical representation of a user-defined aggregate function (UDAF)
/// A UDAF is different from a UDF in that it is stateful across batches.
#[derive(Clone)]
pub struct AggregateUdf {
    /// name
    pub name: String,
    /// signature
    pub signature: Signature,
    /// Return type
    pub return_type: ReturnTypeFunction,
    /// actual implementation
    pub accumulator: AccumulatorFunctionImpl,
    /// the accumulator's state's description as a function of the return type
    pub state_type: StateTypeFunction,
}

impl Debug for AggregateUdf {
    fn fmt(&self, f: &mut Formatter) -> fmt::Result {
        f.debug_struct("AggregateUDF")
            .field("name", &self.name)
            .field("signature", &self.signature)
            .field("fun", &"<FUNC>")
            .finish()
    }
}

impl PartialEq for AggregateUdf {
    fn eq(&self, other: &Self) -> bool {
        self.name == other.name && self.signature == other.signature
    }
}

impl AggregateUdf {
    /// Create a new AggregateUDF
    pub fn new(
        name: &str,
        signature: Signature,
        return_type: ReturnTypeFunction,
        accumulator: AccumulatorFunctionImpl,
        state_type: StateTypeFunction,
    ) -> Self {
        Self {
            name: name.to_owned(),
            signature,
            return_type,
            accumulator,
            state_type,
        }
    }
}

impl From<AggregateUdf> for DfAggregateUdf {
    fn from(udaf: AggregateUdf) -> Self {
        DfAggregateUdf::new(
            &udaf.name,
            &udaf.signature.into(),
            &to_df_return_type(udaf.return_type),
            &to_df_accumulator_func(udaf.accumulator),
            &to_df_state_type(udaf.state_type),
        )
    }
}

fn to_df_accumulator_func(func: AccumulatorFunctionImpl) -> DfAccumulatorFunctionImplementation {
    Arc::new(move || {
        let acc = func()?;
        Ok(Box::new(DfAccumulatorAdaptor(acc)))
    })
}

fn to_df_state_type(func: StateTypeFunction) -> DfStateTypeFunction {
    let df_func = move |data_type: &ArrowDataType| {
        // DataFusion DataType -> ConcreteDataType
        let concrete_data_type = ConcreteDataType::from_arrow_type(data_type);

        // evaluate ConcreteDataType
        let eval_result = (func)(&concrete_data_type);

        // ConcreteDataType -> DataFusion DataType
        eval_result
            .map(|ts| Arc::new(ts.iter().map(|t| t.as_arrow_type()).collect()))
            .map_err(|e| e.into())
    };
    Arc::new(df_func)
}
