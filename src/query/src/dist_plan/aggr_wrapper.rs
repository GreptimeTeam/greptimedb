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

use datafusion_common::ScalarValue;
use datafusion_expr::function::StateFieldsArgs;
use datafusion_expr::{Accumulator, AggregateUDF, AggregateUDFImpl};
use datatypes::arrow::datatypes::{DataType, Field};

/// Wrappr to make an aggregate function out of a state function.
#[derive(Debug)]
pub struct AggregateStateFunctionWrapper {
    inner: AggregateUDF,
    name: String,
    /// The index of the state in the output of the state function.
    state_index: usize,
    /// The ordering fields of the aggregate function.
    pub ordering_fields: Vec<Field>,

    /// Whether the aggregate function is distinct.
    pub is_distinct: bool,
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
        let name = format!("__{}_state_udaf_{}", args.name, args.state_index);
        Self {
            inner,
            name,
            state_index: args.state_index,
            ordering_fields: args.ordering_fields.to_vec(),
            is_distinct: args.is_distinct,
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
        Ok(Box::new(StateAccumWrapper::new(inner, self.state_index)))
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
        let old_return_type = self.inner.return_type(arg_types)?;
        let state_fields_args = StateFieldsArgs {
            name: &self.name,
            input_types: arg_types,
            return_type: &old_return_type,
            ordering_fields: &self.ordering_fields,
            is_distinct: self.is_distinct,
        };
        let state_fields = self.inner.state_fields(state_fields_args)?;
        let ret = state_fields
            .get(self.state_index)
            .ok_or_else(|| {
                datafusion_common::DataFusionError::Internal(format!(
                    "State index {} out of bounds for state fields: {:?}",
                    self.state_index, state_fields
                ))
            })?
            .data_type()
            .clone();
        Ok(ret)
    }

    /// The state function's output fields are the same as the original aggregate function's state fields.
    fn state_fields(
        &self,
        args: datafusion_expr::function::StateFieldsArgs,
    ) -> datafusion_common::Result<Vec<Field>> {
        self.inner.state_fields(args)
    }

    /// The state function's signature is the same as the original aggregate function's signature,
    fn signature(&self) -> &datafusion_expr::Signature {
        self.inner.signature()
    }
}

/// The wrapper's input is the same as the original aggregate function's input,
/// and the output is the state function's output.
#[derive(Debug)]
pub struct StateAccumWrapper {
    inner: Box<dyn Accumulator>,
    /// The index of the state in the output of the state function.
    state_index: usize,
}

impl StateAccumWrapper {
    pub fn new(inner: Box<dyn Accumulator>, state_index: usize) -> Self {
        Self { inner, state_index }
    }
}

impl Accumulator for StateAccumWrapper {
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
pub struct MergeWrapper {
    inner: AggregateUDF,
    name: String,
    /// The signature of the merge function.
    /// It is the `state_fields` of the original aggregate function.
    merge_signature: datafusion_expr::Signature,
    state_fields: Vec<Field>,
    final_return_type: DataType,
}
impl MergeWrapper {
    pub fn new<'a>(inner: AggregateUDF, args: WrapperArgs<'a>) -> datafusion_common::Result<Self> {
        let name = format!("__{}_merge_udaf", inner.name());
        let state_fields_args = StateFieldsArgs {
            name: inner.name(),
            input_types: args.input_types,
            return_type: args.return_type,
            ordering_fields: args.ordering_fields,
            is_distinct: args.is_distinct,
        };
        let state_fields = inner.state_fields(state_fields_args)?;
        let tys = state_fields
            .iter()
            .map(|f| f.data_type().clone())
            .collect::<Vec<_>>();
        let signature =
            datafusion_expr::Signature::exact(tys, datafusion_expr::Volatility::Immutable);
        Ok(Self {
            inner,
            name,
            merge_signature: signature,
            state_fields,
            final_return_type: args.return_type.clone(),
        })
    }

    pub fn inner(&self) -> &AggregateUDF {
        &self.inner
    }
}

impl AggregateUDFImpl for MergeWrapper {
    fn accumulator(
        &self,
        acc_args: datafusion_expr::function::AccumulatorArgs,
    ) -> datafusion_common::Result<Box<dyn Accumulator>> {
        let inner = self.inner.accumulator(acc_args)?;
        Ok(Box::new(MergeAccum::new(inner)))
    }

    fn as_any(&self) -> &dyn std::any::Any {
        self.inner.inner().as_any()
    }
    fn name(&self) -> &str {
        self.name.as_str()
    }

    fn return_type(&self, _arg_types: &[DataType]) -> datafusion_common::Result<DataType> {
        // The return type is the same as the original aggregate function's return type.
        // notice here return type is fixed, instead of using the input types to determine the return type.
        // as the input type now is actually `state_fields`'s data type, and can't be use to determine the output type
        Ok(self.final_return_type.clone())
    }
    fn signature(&self) -> &datafusion_expr::Signature {
        &self.merge_signature
    }

    fn state_fields(
        &self,
        _args: datafusion_expr::function::StateFieldsArgs,
    ) -> datafusion_common::Result<Vec<Field>> {
        Ok(self.state_fields.clone())
    }
}

#[derive(Debug)]
pub struct MergeAccum {
    inner: Box<dyn Accumulator>,
}

impl MergeAccum {
    pub fn new(inner: Box<dyn Accumulator>) -> Self {
        Self { inner }
    }
}

impl Accumulator for MergeAccum {
    fn evaluate(&mut self) -> datafusion_common::Result<ScalarValue> {
        self.inner.evaluate()
    }

    fn merge_batch(&mut self, states: &[arrow::array::ArrayRef]) -> datafusion_common::Result<()> {
        self.inner.merge_batch(states)
    }

    fn update_batch(&mut self, values: &[arrow::array::ArrayRef]) -> datafusion_common::Result<()> {
        // input is also states
        self.inner.merge_batch(values)
    }

    fn size(&self) -> usize {
        self.inner.size()
    }

    fn state(&mut self) -> datafusion_common::Result<Vec<ScalarValue>> {
        self.inner.state()
    }
}
