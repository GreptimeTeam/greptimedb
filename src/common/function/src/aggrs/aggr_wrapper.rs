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

use arrow::array::StructArray;
use datafusion_common::ScalarValue;
use datafusion_expr::function::StateFieldsArgs;
use datafusion_expr::{Accumulator, AggregateUDF, AggregateUDFImpl, Signature};
use datatypes::arrow::datatypes::{DataType, Field};

use crate::aggrs::aggr_wrapper::type_lookup::StateTypeLookup;

mod type_lookup;

/// Returns the name of the state function for the given aggregate function name.
/// The state function is used to compute the state of the aggregate function.
/// The state function's name is in the format `__<aggr_name>_state
pub fn aggr_state_func_name(aggr_name: &str) -> String {
    format!("__{}_state", aggr_name)
}

/// Returns the name of the merge function for the given aggregate function name.
/// The merge function is used to merge the states of the state functions.
/// The merge function's name is in the format `__<aggr_name>_merge
pub fn aggr_merge_func_name(aggr_name: &str) -> String {
    format!("__{}_merge", aggr_name)
}

/// A wrapper to make an aggregate function out of the state and merge functions of the original aggregate function.
/// It contains the original aggregate function, the state functions, and the merge function.
///
/// Notice state functions may have multiple output columns, and the merge function is used to merge the states of the state functions.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StateMergeWrapper {
    /// The original aggregate function.
    original: AggregateUDF,
    /// The state functions of the aggregate function.
    /// Each state function corresponds to a state in the state field of the original aggregate function.
    state_function: StateWrapper,
    /// The merge function of the aggregate function.
    /// It is used to merge the states of the state functions.
    merge_function: MergeWrapper,
}

impl StateMergeWrapper {
    pub fn new(
        original: AggregateUDF,
        state_to_input_types: StateTypeLookup,
    ) -> datafusion_common::Result<Self> {
        let state_function = StateWrapper::new(original.clone())?;
        let merge_function = MergeWrapper::new(original.clone(), state_to_input_types)?;
        Ok(Self {
            original,
            state_function,
            merge_function,
        })
    }

    pub fn original(&self) -> &AggregateUDF {
        &self.original
    }

    pub fn state_function(&self) -> &StateWrapper {
        &self.state_function
    }

    pub fn merge_function(&self) -> &MergeWrapper {
        &self.merge_function
    }
}

/// Wrapper to make an aggregate function out of a state function.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StateWrapper {
    inner: AggregateUDF,
    name: String,
}

impl StateWrapper {
    /// `state_index`: The index of the state in the output of the state function.
    pub fn new(inner: AggregateUDF) -> datafusion_common::Result<Self> {
        let name = aggr_state_func_name(inner.name());
        Ok(Self { inner, name })
    }

    pub fn inner(&self) -> &AggregateUDF {
        &self.inner
    }

    /// Deduce the return type of the original aggregate function
    /// based on the accumulator arguments.
    ///
    pub fn deduce_aggr_return_type(
        &self,
        acc_args: &datafusion_expr::function::AccumulatorArgs,
    ) -> datafusion_common::Result<DataType> {
        let input_exprs = acc_args.exprs;
        let input_schema = acc_args.schema;
        let input_types = input_exprs
            .iter()
            .map(|e| e.data_type(input_schema))
            .collect::<Result<Vec<_>, _>>()?;
        let return_type = self.inner.return_type(&input_types)?;
        Ok(return_type)
    }
}

impl AggregateUDFImpl for StateWrapper {
    fn accumulator<'a, 'b>(
        &'a self,
        acc_args: datafusion_expr::function::AccumulatorArgs<'b>,
    ) -> datafusion_common::Result<Box<dyn Accumulator>> {
        // fix and recover proper acc args for the original aggregate function.

        let inner = {
            let old_return_type = self.deduce_aggr_return_type(&acc_args)?;
            let acc_args = datafusion_expr::function::AccumulatorArgs {
                return_type: &old_return_type,
                schema: acc_args.schema,
                ignore_nulls: acc_args.ignore_nulls,
                ordering_req: acc_args.ordering_req,
                is_reversed: acc_args.is_reversed,
                name: acc_args.name,
                is_distinct: acc_args.is_distinct,
                exprs: acc_args.exprs,
            };
            self.inner.accumulator(acc_args)?
        };
        Ok(Box::new(StateAccum::new(inner)))
    }

    fn as_any(&self) -> &dyn std::any::Any {
        self
    }
    fn name(&self) -> &str {
        self.name.as_str()
    }

    /// Return state_fields as the output struct type.
    ///
    fn return_type(&self, arg_types: &[DataType]) -> datafusion_common::Result<DataType> {
        let old_return_type = self.inner.return_type(arg_types)?;
        let state_fields_args = StateFieldsArgs {
            name: self.inner().name(),
            input_types: arg_types,
            return_type: &old_return_type,
            // TODO(discord9): how to get this?, probably ok?
            ordering_fields: &[],
            is_distinct: false,
        };
        let state_fields = self.inner.state_fields(state_fields_args)?;
        let struct_field = DataType::Struct(state_fields.into());
        Ok(struct_field)
    }

    /// The state function's output fields are the same as the original aggregate function's state fields.
    fn state_fields(
        &self,
        args: datafusion_expr::function::StateFieldsArgs,
    ) -> datafusion_common::Result<Vec<Field>> {
        let old_return_type = self.inner.return_type(args.input_types)?;
        let state_fields_args = StateFieldsArgs {
            name: args.name,
            input_types: args.input_types,
            return_type: &old_return_type,
            ordering_fields: args.ordering_fields,
            is_distinct: args.is_distinct,
        };
        self.inner.state_fields(state_fields_args)
    }

    /// The state function's signature is the same as the original aggregate function's signature,
    fn signature(&self) -> &Signature {
        self.inner.signature()
    }

    /// Coerce types also do nothing, as optimzer should be able to already make struct types
    fn coerce_types(&self, arg_types: &[DataType]) -> datafusion_common::Result<Vec<DataType>> {
        self.inner.coerce_types(arg_types)
    }
}

/// The wrapper's input is the same as the original aggregate function's input,
/// and the output is the state function's output.
#[derive(Debug)]
pub struct StateAccum {
    inner: Box<dyn Accumulator>,
}

impl StateAccum {
    pub fn new(inner: Box<dyn Accumulator>) -> Self {
        Self { inner }
    }
}

impl Accumulator for StateAccum {
    fn evaluate(&mut self) -> datafusion_common::Result<ScalarValue> {
        let state = self.inner.state()?;
        let fields = state
            .iter()
            .enumerate()
            .map(|(i, s)| Field::new(format!("col_{i}"), s.data_type(), true))
            .collect::<Vec<_>>();
        let array = state
            .iter()
            .map(|s| s.to_array())
            .collect::<Result<Vec<_>, _>>()?;
        let struct_array = StructArray::try_new(fields.into(), array, None)?;
        Ok(ScalarValue::Struct(Arc::new(struct_array)))
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

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MergeWrapper {
    inner: AggregateUDF,
    name: String,
    merge_signature: Signature,
    state_to_input_types: StateTypeLookup,
}
impl MergeWrapper {
    pub fn new(
        inner: AggregateUDF,
        state_to_input_types: StateTypeLookup,
    ) -> datafusion_common::Result<Self> {
        let name = aggr_merge_func_name(inner.name());
        // the input type is actually struct type, which is the state fields of the original aggregate function.
        let merge_signature = Signature::user_defined(datafusion_expr::Volatility::Immutable);

        Ok(Self {
            inner,
            name,
            merge_signature,
            state_to_input_types,
        })
    }

    pub fn inner(&self) -> &AggregateUDF {
        &self.inner
    }

    fn get_old_input_type(
        &self,
        state_types: &[DataType],
    ) -> datafusion_common::Result<Vec<DataType>> {
        self.state_to_input_types
            .get_input_types(state_types)?
            .ok_or_else(|| {
                datafusion_common::DataFusionError::Internal(format!(
                    "No input types found for state fields: {:?}",
                    state_types
                ))
            })
    }
}

impl AggregateUDFImpl for MergeWrapper {
    fn accumulator<'a, 'b>(
        &'a self,
        acc_args: datafusion_expr::function::AccumulatorArgs<'b>,
    ) -> datafusion_common::Result<Box<dyn Accumulator>> {
        let state_types = acc_args
            .exprs
            .iter()
            .map(|e| e.data_type(acc_args.schema))
            .collect::<datafusion_common::Result<Vec<_>>>()?;
        let input_types = self.get_old_input_type(&state_types)?;
        let input_schema = arrow_schema::Schema::new(
            input_types
                .iter()
                .enumerate()
                .map(|(i, t)| Field::new(format!("original_input[{i}]"), t.clone(), true))
                .collect::<Vec<_>>(),
        );
        let input_exprs = (0..input_types.len())
            .map(|i| {
                Arc::new(datafusion::physical_expr::expressions::Column::new(
                    &format!("original_input[{i}]"),
                    i,
                )) as Arc<dyn datafusion_physical_expr::PhysicalExpr>
            })
            .collect::<Vec<_>>();
        let correct_acc_args = datafusion_expr::function::AccumulatorArgs {
            return_type: &self.inner.return_type(&input_types)?,
            schema: &input_schema,
            ignore_nulls: acc_args.ignore_nulls,
            ordering_req: acc_args.ordering_req,
            is_reversed: acc_args.is_reversed,
            name: acc_args.name,
            is_distinct: acc_args.is_distinct,
            exprs: &input_exprs,
        };
        let inner = self.inner.accumulator(correct_acc_args)?;
        Ok(Box::new(MergeAccum::new(inner)))
    }

    fn as_any(&self) -> &dyn std::any::Any {
        self
    }
    fn name(&self) -> &str {
        self.name.as_str()
    }

    /// Notice here the `arg_types` is actually the `state_fields`'s data types,
    /// so return fixed return type instead of using `arg_types` to determine the return type.
    fn return_type(&self, arg_types: &[DataType]) -> datafusion_common::Result<DataType> {
        // The return type is the same as the original aggregate function's return type.
        // notice here return type is fixed, instead of using the input types to determine the return type.
        // as the input type now is actually `state_fields`'s data type, and can't be use to determine the output type
        let input_types = self
            .state_to_input_types
            .get_input_types(arg_types)?
            .ok_or_else(|| {
                datafusion_common::DataFusionError::Internal(format!(
                    "No input types found for state fields: {:?}",
                    arg_types
                ))
            })?;
        let ret_type = self.inner.return_type(&input_types)?;
        Ok(ret_type)
    }
    fn signature(&self) -> &Signature {
        &self.merge_signature
    }

    /// Coerce types also do nothing, as optimzer should be able to already make struct types
    fn coerce_types(&self, arg_types: &[DataType]) -> datafusion_common::Result<Vec<DataType>> {
        // just check if the arg_types are only one and is struct array
        if arg_types.len() != 1 || !matches!(arg_types.first(), Some(DataType::Struct(_))) {
            return Err(datafusion_common::DataFusionError::Internal(format!(
                "Expected one struct type as input, got: {:?}",
                arg_types
            )));
        }
        Ok(arg_types.to_vec())
    }

    /// Just return the original aggregate function's state fields.
    fn state_fields(
        &self,
        args: datafusion_expr::function::StateFieldsArgs,
    ) -> datafusion_common::Result<Vec<Field>> {
        // current input types is actually the state fields of the original aggregate function,
        let input_types = self.get_old_input_type(args.input_types)?;
        let old_return_type = self.inner.return_type(&input_types)?;
        // reconstruct `StateFieldsArgs` with the original aggregate function's name and return type.
        let state_fields_args = StateFieldsArgs {
            name: self.inner().name(),
            input_types: args.input_types,
            return_type: &old_return_type,
            ordering_fields: args.ordering_fields,
            is_distinct: args.is_distinct,
        };
        let state_fields = self.inner.state_fields(state_fields_args)?;
        Ok(state_fields)
    }
}

/// The merge accumulator, which modify `update_batch`'s behavior to accept one struct array which
/// include the state fields of original aggregate function, and merge said states into original accumulator
/// the output is the same as original aggregate function
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
        let value = values.first().ok_or_else(|| {
            datafusion_common::DataFusionError::Internal("No values provided for merge".to_string())
        })?;
        // The input values are states from other accumulators, so we merge them.
        let struct_arr = value
            .as_any()
            .downcast_ref::<StructArray>()
            .ok_or_else(|| {
                datafusion_common::DataFusionError::Internal(format!(
                    "Expected StructArray, got: {:?}",
                    value.data_type()
                ))
            })?;
        let state_columns = struct_arr.columns();
        self.inner.merge_batch(state_columns)
    }

    fn size(&self) -> usize {
        self.inner.size()
    }

    fn state(&mut self) -> datafusion_common::Result<Vec<ScalarValue>> {
        self.inner.state()
    }
}

#[cfg(test)]
mod tests;
