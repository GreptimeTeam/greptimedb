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

/// A wrapper to make an aggregate function out of the state and merge functions of the original aggregate function.
/// It contains the original aggregate function, the state functions, and the merge function.
///
/// Notice state functions may have multiple output columns, and the merge function is used to merge the states of the state functions.
pub struct StateMergeWrapper {
    /// The original aggregate function.
    original: AggregateUDF,
    /// The state functions of the aggregate function.
    /// Each state function corresponds to a state in the state field of the original aggregate function.
    state_functions: Vec<StateWrapper>,
    /// The merge function of the aggregate function.
    /// It is used to merge the states of the state functions.
    merge_function: MergeWrapper,
}

#[derive(Debug, Clone, Copy)]
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
}

impl<'a> From<WrapperArgs<'a>> for StateFieldsArgs<'a> {
    fn from(args: WrapperArgs<'a>) -> Self {
        Self {
            name: args.name,
            input_types: args.input_types,
            return_type: args.return_type,
            ordering_fields: args.ordering_fields,
            is_distinct: args.is_distinct,
        }
    }
}

impl StateMergeWrapper {
    pub fn new<'a>(
        original: AggregateUDF,
        args: WrapperArgs<'a>,
    ) -> datafusion_common::Result<Self> {
        let state_functions = (0..original.state_fields(args.into())?.len())
            .map(|i| StateWrapper::new(original.clone(), args, i))
            .collect::<Vec<_>>();
        let merge_function = MergeWrapper::new(original.clone(), args)?;
        Ok(Self {
            original,
            state_functions,
            merge_function,
        })
    }

    pub fn original(&self) -> &AggregateUDF {
        &self.original
    }

    pub fn state_functions(&self) -> &[StateWrapper] {
        &self.state_functions
    }

    pub fn merge_function(&self) -> &MergeWrapper {
        &self.merge_function
    }
}

/// Wrappr to make an aggregate function out of a state function.
#[derive(Debug)]
pub struct StateWrapper {
    inner: AggregateUDF,
    name: String,
    /// The index of the state in the output of the state function.
    state_index: usize,
    /// The ordering fields of the aggregate function.
    pub ordering_fields: Vec<Field>,

    /// Whether the aggregate function is distinct.
    pub is_distinct: bool,
}

impl StateWrapper {
    /// `state_index`: The index of the state in the output of the state function.
    pub fn new<'a>(inner: AggregateUDF, args: WrapperArgs<'a>, state_index: usize) -> Self {
        let name = format!("__{}_state_col_{}_udaf", args.name, state_index);
        Self {
            inner,
            name,
            state_index,
            ordering_fields: args.ordering_fields.to_vec(),
            is_distinct: args.is_distinct,
        }
    }

    pub fn inner(&self) -> &AggregateUDF {
        &self.inner
    }
}

impl AggregateUDFImpl for StateWrapper {
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
        let state_fields_args = args.into();
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

#[cfg(test)]
mod tests {
    use super::*;
}
