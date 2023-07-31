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

//! Udaf module contains functions and structs supporting user-defined aggregate functions.
//!
//! Modified from DataFusion.

use std::fmt::{self, Debug, Formatter};
use std::sync::Arc;

use datafusion_expr::{
    AccumulatorFactoryFunction, AggregateUDF as DfAggregateUdf,
    StateTypeFunction as DfStateTypeFunction,
};
use datatypes::arrow::datatypes::DataType as ArrowDataType;
use datatypes::prelude::*;

use crate::function::{
    to_df_return_type, AccumulatorFunctionImpl, ReturnTypeFunction, StateTypeFunction,
};
use crate::logical_plan::accumulator::DfAccumulatorAdaptor;
use crate::logical_plan::AggregateFunctionCreatorRef;
use crate::signature::Signature;

/// Logical representation of a user-defined aggregate function (UDAF)
/// A UDAF is different from a UDF in that it is stateful across batches.
#[derive(Clone)]
pub struct AggregateFunction {
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
    /// the creator that creates aggregate functions
    creator: AggregateFunctionCreatorRef,
}

impl Debug for AggregateFunction {
    fn fmt(&self, f: &mut Formatter) -> fmt::Result {
        f.debug_struct("AggregateUDF")
            .field("name", &self.name)
            .field("signature", &self.signature)
            .field("fun", &"<FUNC>")
            .finish()
    }
}

impl PartialEq for AggregateFunction {
    fn eq(&self, other: &Self) -> bool {
        self.name == other.name && self.signature == other.signature
    }
}

impl AggregateFunction {
    /// Create a new AggregateUDF
    pub fn new(
        name: String,
        signature: Signature,
        return_type: ReturnTypeFunction,
        accumulator: AccumulatorFunctionImpl,
        state_type: StateTypeFunction,
        creator: AggregateFunctionCreatorRef,
    ) -> Self {
        Self {
            name,
            signature,
            return_type,
            accumulator,
            state_type,
            creator,
        }
    }
}

impl From<AggregateFunction> for DfAggregateUdf {
    fn from(udaf: AggregateFunction) -> Self {
        DfAggregateUdf::new(
            &udaf.name,
            &udaf.signature.into(),
            &to_df_return_type(udaf.return_type),
            &to_df_accumulator_func(udaf.accumulator, udaf.creator.clone()),
            &to_df_state_type(udaf.state_type),
        )
    }
}

fn to_df_accumulator_func(
    accumulator: AccumulatorFunctionImpl,
    creator: AggregateFunctionCreatorRef,
) -> AccumulatorFactoryFunction {
    Arc::new(move |_| {
        let accumulator = accumulator()?;
        let creator = creator.clone();
        Ok(Box::new(DfAccumulatorAdaptor::new(accumulator, creator)) as _)
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
