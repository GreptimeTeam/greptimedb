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

//! Wrapper for making aggregate functions out of state/merge functions of original aggregate functions.
//!
//! i.e. for a aggregate function `foo`, we will have a state function `foo_state` and a merge function `foo_merge`.
//!
//! `foo_state` i's input args is the same as `foo`'s, and its output is a state object.
//! Note that `foo_state` might have multiple output columns(might need special handling in the future).
//! `foo_merge`'s input args is the same as `foo_state`'s, and its output is the same as `foo`'s.
//!

use std::sync::Arc;

use arrow_schema::Fields;
use datafusion_common::{DFSchema, ScalarValue};
use datafusion_expr::{Accumulator, Aggregate, AggregateUDF, AggregateUDFImpl};
use datatypes::arrow::datatypes::{DataType, Field};
use substrait::{DFLogicalSubstraitConvertor, SubstraitPlan};

use crate::query_engine::DefaultSerializer;

/// Wrappr to make an aggregate function out of a state function.
#[derive(Debug)]
pub struct AggregateStateFunctionWrapper {
    inner: AggregateUDF,
    name: String,
    /// The index of the state in the output of the state function.
    state_index: usize,
}

pub struct WrapperArgs<'a> {
    /// The name of the aggregate function.
    pub name: &'a str,
    /// The input types of the aggregate function.
    pub input_types: &'a [DataType],
    /// The return type of the aggregate function.
    pub return_type: &'a DataType,
    /// The ordering fields of the aggregate function.
    pub ordering_fields: &'a [Field],

    /// Whether the aggregate function is distinct.
    pub is_distinct: bool,
    /// The index of the state in the output of the state function.
    state_index: usize,
}

impl AggregateStateFunctionWrapper {
    pub fn new<'a>(inner: AggregateUDF, args: WrapperArgs<'a>) -> Self {
        let name = format!("__{}_state_udaf", args.name);
        Self {
            inner,
            name,
            state_index: args.state_index,
        }
    }

    pub fn inner(&self) -> &AggregateUDF {
        &self.inner
    }
}

impl AggregateUDFImpl for AggregateStateFunctionWrapper {
    fn accumulator(
        &self,
        acc_args: datafusion_expr::function::AccumulatorArgs,
    ) -> datafusion_common::Result<Box<dyn Accumulator>> {
        let inner = self.inner.accumulator(acc_args)?;
        Ok(Box::new(AggregateStateAccumulatorWrapper::new(
            inner,
            self.state_index,
        )))
    }

    fn as_any(&self) -> &dyn std::any::Any {
        self.inner.inner().as_any()
    }
    fn name(&self) -> &str {
        self.name.as_str()
    }

    /// Return state_field[idx] as the output type.
    ///
    fn return_type(&self, arg_types: &[DataType]) -> datafusion_common::Result<DataType> {
        todo!()
    }
    fn signature(&self) -> &datafusion_expr::Signature {
        self.inner.signature()
    }

    fn state_fields(
        &self,
        args: datafusion_expr::function::StateFieldsArgs,
    ) -> datafusion_common::Result<Vec<Field>> {
        self.inner.state_fields(args)
    }
}

/// The wrapper's input is the same as the original aggregate function's input,
/// and the output is the state function's output.
#[derive(Debug)]
pub struct AggregateStateAccumulatorWrapper {
    inner: Box<dyn Accumulator>,
    /// The index of the state in the output of the state function.
    state_index: usize,
}

impl AggregateStateAccumulatorWrapper {
    pub fn new(inner: Box<dyn Accumulator>, state_index: usize) -> Self {
        Self { inner, state_index }
    }
}

impl Accumulator for AggregateStateAccumulatorWrapper {
    fn evaluate(&mut self) -> datafusion_common::Result<ScalarValue> {
        let state = self.inner.state()?;
        let col = state.get(self.state_index).ok_or_else(|| {
            datafusion_common::DataFusionError::Internal(format!(
                "State index {} out of bounds for state: {:?}",
                self.state_index, state
            ))
        })?;
        Ok(col.clone())
    }

    fn merge_batch(
        &mut self,
        states: &[datatypes::arrow::array::ArrayRef],
    ) -> datafusion_common::Result<()> {
        self.inner.merge_batch(states)
    }

    fn update_batch(
        &mut self,
        values: &[datatypes::arrow::array::ArrayRef],
    ) -> datafusion_common::Result<()> {
        self.inner.update_batch(values)
    }

    fn size(&self) -> usize {
        self.inner.size()
    }

    fn state(&mut self) -> datafusion_common::Result<Vec<ScalarValue>> {
        self.inner.state()
    }
}

#[derive(Debug)]
pub struct AggregateMergeFunctionWrapper {
    inner: AggregateUDF,
    arg_types: Vec<DataType>,
    name: String,
}
impl AggregateMergeFunctionWrapper {
    pub fn new(inner: AggregateUDF, input_arg_types: Vec<DataType>) -> Self {
        let name = format!("__{}_merge_udaf", inner.name());
        Self {
            inner,
            arg_types: input_arg_types,
            name,
        }
    }

    pub fn inner(&self) -> &AggregateUDF {
        &self.inner
    }
}
impl AggregateUDFImpl for AggregateMergeFunctionWrapper {
    fn accumulator(
        &self,
        acc_args: datafusion_expr::function::AccumulatorArgs,
    ) -> datafusion_common::Result<Box<dyn Accumulator>> {
        let inner = self.inner.accumulator(acc_args)?;
        todo!()
    }

    fn as_any(&self) -> &dyn std::any::Any {
        self.inner.inner().as_any()
    }
    fn name(&self) -> &str {
        self.name.as_str()
    }

    fn return_type(&self, _arg_types: &[DataType]) -> datafusion_common::Result<DataType> {
        // The return type is the same as the original aggregate function's return type.
        self.inner.return_type(&self.arg_types)
    }
    fn signature(&self) -> &datafusion_expr::Signature {
        // TODO: use original function's state fields as signature.
        todo!()
    }

    fn state_fields(
        &self,
        args: datafusion_expr::function::StateFieldsArgs,
    ) -> datafusion_common::Result<Vec<Field>> {
        self.inner.state_fields(args)
    }
}
