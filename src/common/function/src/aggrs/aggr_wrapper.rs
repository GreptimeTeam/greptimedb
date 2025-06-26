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
            .map(|i| StateWrapper::new(original.clone()))
            .collect::<Result<Vec<_>, _>>()?;
        let merge_function = MergeWrapper::new(original.clone())?;
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
        Ok(Box::new(StateAccumWrapper::new(inner)))
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
            name: &self.inner().name(),
            input_types: arg_types,
            return_type: &old_return_type,
            // TODO: how to get this?, probably ok?
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
}

/// The wrapper's input is the same as the original aggregate function's input,
/// and the output is the state function's output.
#[derive(Debug)]
pub struct StateAccumWrapper {
    inner: Box<dyn Accumulator>,
}

impl StateAccumWrapper {
    pub fn new(inner: Box<dyn Accumulator>) -> Self {
        Self { inner }
    }
}

impl Accumulator for StateAccumWrapper {
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

fn guess_state_fields(
    original: &AggregateUDF,
    input_types: &[DataType],
) -> datafusion_common::Result<Vec<Field>> {
    let state_fields_args = StateFieldsArgs {
        name: original.name(),
        input_types,
        return_type: &original.return_type(input_types)?,
        ordering_fields: &[],
        is_distinct: false,
    };
    original.state_fields(state_fields_args)
}

/// Guess the input types of the state function based on the original aggregate function's state types.
/// It's not necessarily the same, but it's consisent enough for oringinal aggregate function to behave correctly.
fn guess_input_type_by_state_type(
    original: &AggregateUDF,
    state_types: &[DataType],
) -> datafusion_common::Result<Vec<DataType>> {
    // first try if state_types == input_types
    if let Ok(ret_ty) = original.return_type(state_types) {
        // check if result state tys is correct
        let args = StateFieldsArgs {
            name: original.name(),
            input_types: state_types,
            return_type: &ret_ty,
            ordering_fields: &[],
            is_distinct: false,
        };
        if let Ok(state_fields) = original.state_fields(args) {
            // check for type
            let real_state_types = state_fields
                .iter()
                .map(|f| f.data_type().clone())
                .collect::<Vec<_>>();
            if real_state_types == state_types {
                return Ok(state_types.to_vec());
            }
        }
    }
    // if above doesn't work, we try to guess the input types based on the state types.
    todo!()
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MergeWrapper {
    inner: AggregateUDF,
    name: String,
    merge_signature: Signature,
}
impl MergeWrapper {
    pub fn new(inner: AggregateUDF) -> datafusion_common::Result<Self> {
        let name = aggr_merge_func_name(&inner.name());
        let merge_signature = Signature::user_defined(datafusion_expr::Volatility::Immutable);

        Ok(Self {
            inner,
            name,
            merge_signature,
        })
    }

    pub fn inner(&self) -> &AggregateUDF {
        &self.inner
    }
}

impl AggregateUDFImpl for MergeWrapper {
    fn accumulator<'a, 'b>(
        &'a self,
        acc_args: datafusion_expr::function::AccumulatorArgs<'b>,
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

    /// Notice here the `arg_types` is actually the `state_fields`'s data types,
    /// so return fixed return type instead of using `arg_types` to determine the return type.
    fn return_type(&self, _arg_types: &[DataType]) -> datafusion_common::Result<DataType> {
        // The return type is the same as the original aggregate function's return type.
        // notice here return type is fixed, instead of using the input types to determine the return type.
        // as the input type now is actually `state_fields`'s data type, and can't be use to determine the output type
        todo!()
    }
    fn signature(&self) -> &Signature {
        &self.merge_signature
    }

    fn coerce_types(&self, arg_types: &[DataType]) -> datafusion_common::Result<Vec<DataType>> {
        Ok(arg_types.to_vec())
    }

    /// Just return the original aggregate function's state fields.
    fn state_fields(
        &self,
        args: datafusion_expr::function::StateFieldsArgs,
    ) -> datafusion_common::Result<Vec<Field>> {
        let old_return_type = self.inner.return_type(args.input_types)?;
        let state_fields_args = StateFieldsArgs {
            name: &self.inner().name(),
            input_types: args.input_types,
            return_type: &old_return_type,
            ordering_fields: args.ordering_fields,
            is_distinct: args.is_distinct,
        };
        let state_fields = self.inner.state_fields(state_fields_args)?;
        Ok(state_fields)
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
        // The input values are states from other accumulators, so we merge them.
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
    use std::sync::Arc;

    use arrow::array::{Float64Array, Int64Array, UInt64Array};
    use datafusion_expr::function::AccumulatorArgs;
    use datafusion_expr::Signature;
    use datafusion_physical_expr::LexOrdering;
    use pretty_assertions::assert_eq;

    use super::*;

    #[test]
    fn test_sum_udaf() {
        let sum = datafusion::functions_aggregate::sum::sum_udaf();
        let sum = (*sum).clone();

        let args = WrapperArgs {
            name: sum.name(),
            input_types: &[DataType::Int64],
            return_type: &sum.return_type(&[DataType::Int64]).unwrap(),
            ordering_fields: &[],
            is_distinct: false,
        };

        let wrapper = StateMergeWrapper::new(sum.clone(), args).unwrap();
        let expected = StateMergeWrapper {
            original: sum.clone(),
            state_functions: vec![StateWrapper {
                inner: sum.clone(),
                name: "__sum_state_udaf".to_string(),
            }],
            merge_function: MergeWrapper {
                inner: sum.clone(),
                name: "__sum_merge_udaf".to_string(),
                merge_signature: Signature::exact(
                    vec![DataType::Int64],
                    datafusion_expr::Volatility::Immutable,
                ),
            },
        };
        assert_eq!(wrapper, expected);

        // evaluate the state function
        let input = Int64Array::from(vec![Some(1), Some(2), None, Some(3)]);
        let values = vec![Arc::new(input) as arrow::array::ArrayRef];

        let state_func = wrapper.state_functions().first().unwrap();
        let accum_args = AccumulatorArgs {
            return_type: &sum.return_type(&[DataType::Int64]).unwrap(),
            schema: &arrow_schema::Schema::new(vec![Field::new("col", DataType::Int64, true)]),
            ignore_nulls: false,
            ordering_req: LexOrdering::empty(),
            is_reversed: false,
            name: state_func.name(),
            is_distinct: false,
            exprs: &[Arc::new(
                datafusion::physical_expr::expressions::Column::new("col", 0),
            )],
        };
        let mut state_accum = state_func.accumulator(accum_args).unwrap();

        state_accum.update_batch(&values).unwrap();
        let state = state_accum.state().unwrap();
        assert_eq!(state.len(), 1);
        assert_eq!(state[0], ScalarValue::Int64(Some(6)));

        let eval_res = state_accum.evaluate().unwrap();
        assert_eq!(eval_res, ScalarValue::Int64(Some(6)));

        let merge_input = vec![
            Arc::new(Int64Array::from(vec![Some(6), Some(42), None])) as arrow::array::ArrayRef
        ];
        let merge_func = wrapper.merge_function();
        let merge_accum_args = AccumulatorArgs {
            return_type: &sum.return_type(&[DataType::Int64]).unwrap(),
            schema: &arrow_schema::Schema::new(vec![Field::new(
                "original_input[0]",
                DataType::Int64,
                true,
            )]),
            ignore_nulls: false,
            ordering_req: LexOrdering::empty(),
            is_reversed: false,
            name: state_func.name(),
            is_distinct: false,
            exprs: &[Arc::new(
                datafusion::physical_expr::expressions::Column::new("original_input[0]", 0),
            )],
        };
        let mut merge_accum = merge_func.accumulator(merge_accum_args).unwrap();
        merge_accum.update_batch(&merge_input).unwrap();
        let merge_state = merge_accum.state().unwrap();
        assert_eq!(merge_state.len(), 1);
        assert_eq!(merge_state[0], ScalarValue::Int64(Some(48)));

        let merge_eval_res = merge_accum.evaluate().unwrap();
        assert_eq!(merge_eval_res, ScalarValue::Int64(Some(48)));
    }

    #[test]
    fn test_avg_udaf() {
        let avg = datafusion::functions_aggregate::average::avg_udaf();
        let avg = (*avg).clone();

        let args = WrapperArgs {
            name: avg.name(),
            input_types: &[DataType::Float64],
            return_type: &avg.return_type(&[DataType::Float64]).unwrap(),
            ordering_fields: &[],
            is_distinct: false,
        };

        let wrapper = StateMergeWrapper::new(avg.clone(), args).unwrap();

        let expected = StateMergeWrapper {
            original: avg.clone(),
            state_functions: vec![
                StateWrapper {
                    inner: avg.clone(),
                    name: "__avg_state_col_0_udaf".to_string(),
                },
                StateWrapper {
                    inner: avg.clone(),
                    name: "__avg_state_col_1_udaf".to_string(),
                },
            ],
            merge_function: MergeWrapper {
                inner: avg.clone(),
                name: "__avg_merge_udaf".to_string(),
                merge_signature: Signature::exact(
                    vec![DataType::UInt64, DataType::Float64],
                    datafusion_expr::Volatility::Immutable,
                ),
            },
        };
        assert_eq!(wrapper, expected);

        // evaluate the state function
        let input = Float64Array::from(vec![Some(1.), Some(2.), None, Some(3.)]);
        let values = vec![Arc::new(input) as arrow::array::ArrayRef];

        let state_func = wrapper.state_functions()[0].clone();
        let accum_args = AccumulatorArgs {
            return_type: &DataType::UInt64,
            schema: &arrow_schema::Schema::new(vec![Field::new("col", DataType::Float64, true)]),
            ignore_nulls: false,
            ordering_req: LexOrdering::empty(),
            is_reversed: false,
            name: state_func.name(),
            is_distinct: false,
            exprs: &[Arc::new(
                datafusion::physical_expr::expressions::Column::new("col", 0),
            )],
        };
        let mut state_accum = state_func.accumulator(accum_args).unwrap();

        state_accum.update_batch(&values).unwrap();
        let state = state_accum.state().unwrap();
        assert_eq!(state.len(), 2);
        assert_eq!(state[0], ScalarValue::UInt64(Some(3)));
        assert_eq!(state[1], ScalarValue::Float64(Some(6.)));

        let eval_res = state_accum.evaluate().unwrap();
        // state_index=0, hence we only return the count
        assert_eq!(eval_res, ScalarValue::UInt64(Some(3)));

        let state_func = wrapper.state_functions()[1].clone();
        let accum_args = AccumulatorArgs {
            return_type: &DataType::Float64,
            schema: &arrow_schema::Schema::new(vec![Field::new("col", DataType::Float64, true)]),
            ignore_nulls: false,
            ordering_req: LexOrdering::empty(),
            is_reversed: false,
            name: state_func.name(),
            is_distinct: false,
            exprs: &[Arc::new(
                datafusion::physical_expr::expressions::Column::new("col", 0),
            )],
        };
        let mut state_accum = state_func.accumulator(accum_args).unwrap();

        state_accum.update_batch(&values).unwrap();
        let state = state_accum.state().unwrap();
        assert_eq!(state.len(), 2);
        assert_eq!(state[0], ScalarValue::UInt64(Some(3)));
        assert_eq!(state[1], ScalarValue::Float64(Some(6.)));

        let eval_res = state_accum.evaluate().unwrap();
        // state_index=0, hence we only return the count
        assert_eq!(eval_res, ScalarValue::Float64(Some(6.)));

        let merge_input = vec![
            Arc::new(UInt64Array::from(vec![Some(3), Some(42), None])) as arrow::array::ArrayRef,
            Arc::new(Float64Array::from(vec![Some(48.), Some(84.), None])),
        ];
        let merge_func = wrapper.merge_function();
        // this accum args is still the original aggregate function's args
        let merge_accum_args = AccumulatorArgs {
            return_type: &avg.return_type(&[DataType::Float64]).unwrap(),
            schema: &arrow_schema::Schema::new(vec![
                Field::new("col0", DataType::UInt64, true),
                Field::new("col1", DataType::Float64, true),
            ]),
            ignore_nulls: false,
            ordering_req: LexOrdering::empty(),
            is_reversed: false,
            name: state_func.name(),
            is_distinct: false,
            exprs: &[
                Arc::new(datafusion::physical_expr::expressions::Column::new(
                    "col0", 0,
                )),
                Arc::new(datafusion::physical_expr::expressions::Column::new(
                    "col1", 1,
                )),
            ],
        };
        let mut merge_accum = merge_func.accumulator(merge_accum_args).unwrap();
        merge_accum.update_batch(&merge_input).unwrap();
        let merge_state = merge_accum.state().unwrap();
        assert_eq!(merge_state.len(), 2);
        assert_eq!(merge_state[0], ScalarValue::UInt64(Some(45)));
        assert_eq!(merge_state[1], ScalarValue::Float64(Some(132.)));

        let merge_eval_res = merge_accum.evaluate().unwrap();
        // the merge function returns the average, which is 132 / 45
        assert_eq!(merge_eval_res, ScalarValue::Float64(Some(132. / 45_f64)));
    }
}
