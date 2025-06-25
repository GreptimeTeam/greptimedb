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

use datafusion_common::ScalarValue;
use datafusion_expr::function::StateFieldsArgs;
use datafusion_expr::{Accumulator, AggregateUDF, AggregateUDFImpl};
use datatypes::arrow::datatypes::{DataType, Field};

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
            .map(|i| StateWrapper::new(original.clone(), args, i))
            .collect::<Result<Vec<_>, _>>()?;
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

/// Wrapper to make an aggregate function out of a state function.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StateWrapper {
    inner: AggregateUDF,
    name: String,
    /// The index of the state in the output of the state function.
    state_index: usize,
    /// The ordering fields of the aggregate function.
    pub ordering_fields: Vec<Field>,

    /// Whether the aggregate function is distinct.
    pub is_distinct: bool,
    /// The final return type of the `inner` function.
    pub final_return_type: DataType,
}

impl StateWrapper {
    /// `state_index`: The index of the state in the output of the state function.
    pub fn new<'a>(
        inner: AggregateUDF,
        args: WrapperArgs<'a>,
        state_index: usize,
    ) -> datafusion_common::Result<Self> {
        let name = format!("__{}_state_col_{}_udaf", args.name, state_index);
        let final_return_type = inner.return_type(args.input_types)?;
        Ok(Self {
            inner,
            name,
            state_index,
            ordering_fields: args.ordering_fields.to_vec(),
            is_distinct: args.is_distinct,
            final_return_type,
        })
    }

    pub fn inner(&self) -> &AggregateUDF {
        &self.inner
    }
}

impl AggregateUDFImpl for StateWrapper {
    fn accumulator<'a>(
        &'a self,
        mut acc_args: datafusion_expr::function::AccumulatorArgs<'a>,
    ) -> datafusion_common::Result<Box<dyn Accumulator>> {
        // TODO: fix and recover proper acc args for the original aggregate function.
        acc_args.return_type = &self.final_return_type;
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
            name: &self.inner().name(),
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

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MergeWrapper {
    inner: AggregateUDF,
    name: String,
    /// The signature of the merge function.
    /// It is the `state_fields` of the original aggregate function.
    merge_signature: datafusion_expr::Signature,
    state_fields: Vec<Field>,
    original_input_types: Vec<DataType>,
    original_return_type: DataType,
    original_schema: arrow_schema::Schema,
    original_exprs: Vec<Arc<dyn datafusion_physical_expr::PhysicalExpr>>,
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

        let schema = arrow_schema::Schema::new(
            args.input_types
                .to_vec()
                .iter()
                .enumerate()
                .map(|(i, dt)| Field::new(format!("original_input[{}]", i), dt.clone(), true))
                .collect::<Vec<_>>(),
        );
        let exprs = args
            .input_types
            .iter()
            .enumerate()
            .map(|(i, _dt)| {
                Arc::new(datafusion::physical_expr::expressions::Column::new(
                    &format!("original_input[{}]", i),
                    i,
                )) as Arc<dyn datafusion_physical_expr::PhysicalExpr>
            })
            .collect::<Vec<_>>();

        Ok(Self {
            inner,
            name,
            merge_signature: signature,
            state_fields,
            original_input_types: args.input_types.to_vec(),
            original_return_type: args.return_type.clone(),
            original_schema: schema,
            original_exprs: exprs,
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
        // rewrite the accumulator args to match the original aggregate function's input types.
        let mut acc_args = acc_args;

        acc_args.schema = &self.original_schema;
        acc_args.exprs = &self.original_exprs;

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
        Ok(self.original_return_type.clone())
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
                name: "__sum_state_col_0_udaf".to_string(),
                state_index: 0,
                ordering_fields: vec![],
                is_distinct: false,
                final_return_type: DataType::Int64,
            }],
            merge_function: MergeWrapper {
                inner: sum.clone(),
                name: "__sum_merge_udaf".to_string(),
                merge_signature: Signature::exact(
                    vec![DataType::Int64],
                    datafusion_expr::Volatility::Immutable,
                ),
                state_fields: vec![Field::new("sum[sum]", DataType::Int64, true)],
                original_input_types: vec![DataType::Int64],
                original_return_type: DataType::Int64,
                original_schema: arrow_schema::Schema::new(vec![Field::new(
                    "original_input[0]",
                    DataType::Int64,
                    true,
                )]),
                original_exprs: vec![Arc::new(
                    datafusion::physical_expr::expressions::Column::new("original_input[0]", 0),
                )],
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
            is_distinct: state_func.is_distinct,
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
            is_distinct: state_func.is_distinct,
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
                    state_index: 0,
                    ordering_fields: vec![],
                    is_distinct: false,
                    final_return_type: DataType::Float64,
                },
                StateWrapper {
                    inner: avg.clone(),
                    name: "__avg_state_col_1_udaf".to_string(),
                    state_index: 1,
                    ordering_fields: vec![],
                    is_distinct: false,
                    final_return_type: DataType::Float64,
                },
            ],
            merge_function: MergeWrapper {
                inner: avg.clone(),
                name: "__avg_merge_udaf".to_string(),
                merge_signature: Signature::exact(
                    vec![DataType::UInt64, DataType::Float64],
                    datafusion_expr::Volatility::Immutable,
                ),
                state_fields: vec![
                    Field::new("avg[count]", DataType::UInt64, true),
                    Field::new("avg[sum]", DataType::Float64, true),
                ],
                original_input_types: vec![DataType::Float64],
                original_return_type: DataType::Float64,

                original_schema: arrow_schema::Schema::new(vec![Field::new(
                    "original_input[0]",
                    DataType::Float64,
                    true,
                )]),
                original_exprs: vec![Arc::new(
                    datafusion::physical_expr::expressions::Column::new("original_input[0]", 0),
                )],
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
            is_distinct: state_func.is_distinct,
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
            is_distinct: state_func.is_distinct,
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
            is_distinct: state_func.is_distinct,
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
