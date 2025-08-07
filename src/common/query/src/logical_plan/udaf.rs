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

use std::any::Any;
use std::fmt::{self, Debug, Formatter};
use std::sync::Arc;

use datafusion::arrow::datatypes::Field;
use datafusion_common::Result;
use datafusion_expr::function::{AccumulatorArgs, StateFieldsArgs};
use datafusion_expr::{
    Accumulator, AccumulatorFactoryFunction, AggregateUDF as DfAggregateUdf, AggregateUDFImpl,
};
use datatypes::arrow::datatypes::{DataType as ArrowDataType, FieldRef};
use datatypes::data_type::DataType;

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

struct DfUdafAdapter {
    name: String,
    signature: datafusion_expr::Signature,
    return_type_func: datafusion_expr::ReturnTypeFunction,
    accumulator: AccumulatorFactoryFunction,
    creator: AggregateFunctionCreatorRef,
}

impl Debug for DfUdafAdapter {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        f.debug_struct("DfUdafAdapter")
            .field("name", &self.name)
            .field("signature", &self.signature)
            .finish()
    }
}

impl AggregateUDFImpl for DfUdafAdapter {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn name(&self) -> &str {
        &self.name
    }

    fn signature(&self) -> &datafusion_expr::Signature {
        &self.signature
    }

    fn return_type(&self, arg_types: &[ArrowDataType]) -> Result<ArrowDataType> {
        (self.return_type_func)(arg_types).map(|x| x.as_ref().clone())
    }

    fn accumulator(&self, acc_args: AccumulatorArgs) -> Result<Box<dyn Accumulator>> {
        (self.accumulator)(acc_args)
    }

    fn state_fields(&self, args: StateFieldsArgs) -> Result<Vec<FieldRef>> {
        let state_types = self.creator.state_types()?;
        let fields = state_types
            .into_iter()
            .enumerate()
            .map(|(i, t)| {
                let name = format!("{}_{i}", args.name);
                Arc::new(Field::new(name, t.as_arrow_type(), true))
            })
            .collect::<Vec<_>>();
        Ok(fields)
    }
}

impl From<AggregateFunction> for DfAggregateUdf {
    fn from(udaf: AggregateFunction) -> Self {
        DfAggregateUdf::new_from_impl(DfUdafAdapter {
            name: udaf.name,
            signature: udaf.signature.into(),
            return_type_func: to_df_return_type(udaf.return_type),
            accumulator: to_df_accumulator_func(udaf.accumulator, udaf.creator.clone()),
            creator: udaf.creator,
        })
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
