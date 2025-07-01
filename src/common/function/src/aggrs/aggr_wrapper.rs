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

use std::collections::BTreeMap;
use std::sync::Arc;

use arrow::array::StructArray;
use datafusion_common::ScalarValue;
use datafusion_expr::function::StateFieldsArgs;
use datafusion_expr::{Accumulator, AggregateUDF, AggregateUDFImpl, Signature, TypeSignature};
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
    state_function: StateWrapper,
    /// The merge function of the aggregate function.
    /// It is used to merge the states of the state functions.
    merge_function: MergeWrapper,
}

impl StateMergeWrapper {
    pub fn new(original: AggregateUDF) -> datafusion_common::Result<Self> {
        let state_function = StateWrapper::new(original.clone())?;
        let merge_function = MergeWrapper::new(original.clone())?;
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
            name: self.inner().name(),
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

#[derive(Debug, Default, Clone, PartialEq, Eq)]
struct StateToInputType {
    /// A mapping from state types to input types.
    mapping: BTreeMap<Vec<DataType>, Vec<DataType>>,
}

impl StateToInputType {
    /// Returns the input types for the given state types.
    ///
    /// Also handle certain data types that have extra args, like `Decimal128/256`, assuming only one of
    /// the `Decimal128/256` in the state types, and replace it with the same type in the returned input types.
    pub fn get_input_types(
        &self,
        state_types: &[DataType],
    ) -> datafusion_common::Result<Option<Vec<DataType>>> {
        let is_decimal =
            |ty: &DataType| matches!(ty, DataType::Decimal128(_, _) | DataType::Decimal256(_, _));
        let need_change_ty = state_types.iter().any(is_decimal);
        if !need_change_ty {
            return Ok(self.mapping.get(state_types).cloned());
        }

        for (k, v) in self.mapping.iter() {
            if k.len() != state_types.len() {
                continue;
            }
            let mut is_equal_types = true;
            for (k, s) in k.iter().zip(state_types.iter()) {
                let is_equal = match (k, s) {
                    (DataType::Decimal128(_, _), DataType::Decimal128(_, _)) => true,
                    (DataType::Decimal256(_, _), DataType::Decimal256(_, _)) => true,
                    (a, b) => a == b,
                };
                if !is_equal {
                    is_equal_types = false;
                    break;
                }
            }

            if is_equal_types {
                // replace `Decimal` in input_types with same `Decimal` type in `state_types`
                // expect only one of the `Decimal128/256` in the state_types
                let mut input_types = v.clone();
                let new_type = state_types
                    .iter()
                    .find(|ty| is_decimal(ty))
                    .ok_or_else(|| {
                        datafusion_common::DataFusionError::Internal(format!(
                            "No Decimal type found in state types: {:?}",
                            state_types
                        ))
                    })?;
                let replace_type = |ty: &DataType| {
                    if is_decimal(ty) {
                        // replace with the first `Decimal256` in state_types
                        new_type.clone()
                    } else {
                        ty.clone()
                    }
                };
                input_types = input_types.iter().map(replace_type).collect::<Vec<_>>();
                return Ok(Some(input_types));
            }
        }
        Ok(None)
    }
}

fn get_possible_state_to_input_types(
    inner: &AggregateUDF,
) -> datafusion_common::Result<StateToInputType> {
    let mut mapping = BTreeMap::new();

    fn update_input_types(
        inner: &AggregateUDF,
        mapping: &mut BTreeMap<Vec<DataType>, Vec<DataType>>,
        input_types: &[DataType],
    ) -> datafusion_common::Result<()> {
        let ret_type = inner.return_type(input_types)?;
        let state_fields_args = StateFieldsArgs {
            name: inner.name(),
            input_types,
            return_type: &ret_type,
            ordering_fields: &[],
            is_distinct: false,
        };
        let state_fields = inner.state_fields(state_fields_args)?;
        mapping.insert(
            state_fields.iter().map(|f| f.data_type().clone()).collect(),
            input_types.to_vec(),
        );
        Ok(())
    }

    fn parse_ty_sig(
        inner: &AggregateUDF,
        sig: &TypeSignature,
        mapping: &mut BTreeMap<Vec<DataType>, Vec<DataType>>,
    ) -> datafusion_common::Result<()> {
        match &sig {
            TypeSignature::Exact(input_types) => {
                let _ = update_input_types(inner, mapping, input_types);
            }
            TypeSignature::OneOf(type_sig) => {
                for sig in type_sig {
                    parse_ty_sig(inner, sig, mapping)?;
                }
            }
            TypeSignature::Numeric(len) => {
                for ty in DataTypeIterator::all_flat().filter(|t| t.is_numeric()) {
                    let input_types = vec![ty.clone(); *len];
                    // ignore error as we are just collecting all possible input types
                    let _ = update_input_types(inner, mapping, &input_types);
                }
            }
            TypeSignature::Variadic(var) => {
                for ty in var {
                    // assuming one or more arguments produce the same state
                    let input_types = vec![ty.clone()];
                    // ignore error as we are just collecting all possible input types
                    let _ = update_input_types(inner, mapping, &input_types);
                }
            }
            TypeSignature::VariadicAny => {
                for ty in DataTypeIterator::all_flat() {
                    // assuming one or more arguments produce the same state
                    let input_types = vec![ty.clone()];
                    // ignore error as we are just collecting all possible input types
                    let _ = update_input_types(inner, mapping, &input_types);
                }
            }
            TypeSignature::Uniform(cnt, tys) => {
                for ty in tys {
                    let input_types = vec![ty.clone(); *cnt];
                    // ignore error as we are just collecting all possible input types
                    let _ = update_input_types(inner, mapping, &input_types);
                }
            }
            TypeSignature::UserDefined => {
                // first determine input length
                let input_len = match inner.name() {
                    "sum" | "count" | "avg" | "min" | "max" => 1,
                    _ => {
                        return Err(datafusion_common::DataFusionError::Internal(format!(
                            "Unsupported aggregate function for step aggr pushdown: {}",
                            inner.name()
                        )));
                    }
                };
                match input_len {
                    1 => {
                        for ty in DataTypeIterator::all_flat() {
                            let input_types = vec![ty.clone(); input_len];
                            // ignore error as we are just collecting all possible input types
                            let _ = update_input_types(inner, mapping, &input_types);
                        }
                    }
                    _ => {
                        return Err(datafusion_common::DataFusionError::Internal(format!(
                            "Unsupported input length for step aggr pushdown: {}",
                            input_len
                        )));
                    }
                }
            }
            TypeSignature::String(cnt) => {
                for ty in DataTypeIterator::all_flat() {
                    let input_types = vec![ty.clone(); *cnt];
                    // ignore error as we are just collecting all possible input types
                    let _ = update_input_types(inner, mapping, &input_types);
                }
            }
            TypeSignature::Nullary => {
                let input_types = vec![];
                // ignore error as we are just collecting all possible input types
                let _ = update_input_types(inner, mapping, &input_types);
            }
            _ => {
                return Err(datafusion_common::DataFusionError::Internal(format!(
                    "Unsupported type signature for step aggr pushdown: {:?}",
                    sig
                )))
            }
        }
        Ok(())
    }
    parse_ty_sig(inner, &inner.signature().type_signature, &mut mapping)?;

    Ok(StateToInputType { mapping })
}

pub struct DataTypeIterator {
    inner: Vec<DataType>,
}

impl DataTypeIterator {
    pub fn new(inner: Vec<DataType>) -> Self {
        Self { inner }
    }

    pub fn all_flat() -> Self {
        use arrow::datatypes::{IntervalUnit, TimeUnit};
        use DataType::*;
        let inner = vec![
            Null,
            Boolean,
            Int8,
            Int16,
            Int32,
            Int64,
            UInt8,
            UInt16,
            UInt32,
            UInt64,
            Float32,
            Float64,
            Decimal128(38, 18),
            Decimal256(76, 38),
            Binary,
            Date32,
            Date64,
            Timestamp(TimeUnit::Second, None),
            Timestamp(TimeUnit::Millisecond, None),
            Timestamp(TimeUnit::Microsecond, None),
            Timestamp(TimeUnit::Nanosecond, None),
            Time32(TimeUnit::Second),
            Time32(TimeUnit::Millisecond),
            Time64(TimeUnit::Microsecond),
            Time64(TimeUnit::Nanosecond),
            Duration(TimeUnit::Second),
            Duration(TimeUnit::Millisecond),
            Duration(TimeUnit::Microsecond),
            Duration(TimeUnit::Nanosecond),
            Interval(IntervalUnit::DayTime),
            Interval(IntervalUnit::YearMonth),
            Interval(IntervalUnit::MonthDayNano),
            LargeBinary,
            BinaryView,
            Utf8,
            LargeUtf8,
            Utf8View,
        ];
        Self { inner }
    }
}

impl Iterator for DataTypeIterator {
    type Item = DataType;

    fn next(&mut self) -> Option<Self::Item> {
        self.inner.pop()
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MergeWrapper {
    inner: AggregateUDF,
    name: String,
    merge_signature: Signature,
    state_to_input_types: StateToInputType,
}
impl MergeWrapper {
    pub fn new(inner: AggregateUDF) -> datafusion_common::Result<Self> {
        let name = aggr_merge_func_name(inner.name());
        // the input type is actually struct type, which is the state fields of the original aggregate function.
        let merge_signature = Signature::user_defined(datafusion_expr::Volatility::Immutable);
        // TODO: a mapping of acceptable input types to state fields' data types for the original function
        let state_to_input_types = get_possible_state_to_input_types(&inner)?;

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
        let input_types = self
            .state_to_input_types
            .get_input_types(&state_types)?
            .ok_or_else(|| {
                datafusion_common::DataFusionError::Internal(format!(
                    "No input types found for state fields: {:?}",
                    state_types
                ))
            })?;
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
        self.inner.inner().as_any()
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
        Ok(arg_types.to_vec())
    }

    /// Just return the original aggregate function's state fields.
    fn state_fields(
        &self,
        args: datafusion_expr::function::StateFieldsArgs,
    ) -> datafusion_common::Result<Vec<Field>> {
        let old_return_type = self.inner.return_type(args.input_types)?;
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

        let wrapper = StateMergeWrapper::new(sum.clone()).unwrap();
        let expected = StateMergeWrapper {
            original: sum.clone(),
            state_function: StateWrapper {
                inner: sum.clone(),
                name: "__sum_state".to_string(),
            },
            merge_function: MergeWrapper {
                inner: sum.clone(),
                name: "__sum_merge".to_string(),
                merge_signature: Signature::user_defined(datafusion_expr::Volatility::Immutable),
                state_to_input_types: StateToInputType {
                    mapping: BTreeMap::from([
                        (vec![DataType::Float64], vec![DataType::Float64]),
                        (vec![DataType::Int64], vec![DataType::Int64]),
                        (vec![DataType::UInt64], vec![DataType::UInt64]),
                        (
                            vec![DataType::Decimal128(38, 18)],
                            vec![DataType::Decimal128(38, 18)],
                        ),
                        (
                            vec![DataType::Decimal256(76, 38)],
                            vec![DataType::Decimal256(76, 38)],
                        ),
                    ]),
                },
            },
        };
        assert_eq!(wrapper, expected);

        // evaluate the state function
        let input = Int64Array::from(vec![Some(1), Some(2), None, Some(3)]);
        let values = vec![Arc::new(input) as arrow::array::ArrayRef];

        let state_func = wrapper.state_function();
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
        assert_eq!(
            eval_res,
            ScalarValue::Struct(Arc::new(
                StructArray::try_new(
                    vec![Field::new("col_0", DataType::Int64, true)].into(),
                    vec![Arc::new(Int64Array::from(vec![Some(6)]))],
                    None,
                )
                .unwrap(),
            ))
        );

        let merge_input = vec![
            Arc::new(Int64Array::from(vec![Some(6), Some(42), None])) as arrow::array::ArrayRef
        ];
        let merge_input_struct_arr = StructArray::try_new(
            vec![Field::new("state[0]", DataType::Int64, true)].into(),
            merge_input,
            None,
        )
        .unwrap();

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
        merge_accum
            .update_batch(&[Arc::new(merge_input_struct_arr)])
            .unwrap();
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

        let wrapper = StateMergeWrapper::new(avg.clone()).unwrap();

        let expected = StateMergeWrapper {
            original: avg.clone(),
            state_function: StateWrapper {
                inner: avg.clone(),
                name: "__avg_state".to_string(),
            },
            merge_function: MergeWrapper {
                inner: avg.clone(),
                name: "__avg_merge".to_string(),
                merge_signature: Signature::user_defined(datafusion_expr::Volatility::Immutable),
                state_to_input_types: StateToInputType {
                    mapping: {
                        use DataType::*;
                        BTreeMap::from([
                            (vec![UInt64, Int8], vec![Int8]),
                            (vec![UInt64, Int16], vec![Int16]),
                            (vec![UInt64, Int32], vec![Int32]),
                            (vec![UInt64, Int64], vec![Int64]),
                            (vec![UInt64, UInt8], vec![UInt8]),
                            (vec![UInt64, UInt16], vec![UInt16]),
                            (vec![UInt64, UInt32], vec![UInt32]),
                            (vec![UInt64, UInt64], vec![UInt64]),
                            (vec![UInt64, Float32], vec![Float32]),
                            (vec![UInt64, Float64], vec![Float64]),
                            (vec![UInt64, Decimal128(38, 18)], vec![Decimal128(38, 18)]),
                            (vec![UInt64, Decimal256(76, 38)], vec![Decimal256(76, 38)]),
                        ])
                    },
                },
            },
        };
        assert_eq!(wrapper, expected);

        // evaluate the state function
        let input = Float64Array::from(vec![Some(1.), Some(2.), None, Some(3.)]);
        let values = vec![Arc::new(input) as arrow::array::ArrayRef];

        let state_func = wrapper.state_function();
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
        let expected = Arc::new(
            StructArray::try_new(
                vec![
                    Field::new("col_0", DataType::UInt64, true),
                    Field::new("col_1", DataType::Float64, true),
                ]
                .into(),
                vec![
                    Arc::new(UInt64Array::from(vec![Some(3)])),
                    Arc::new(Float64Array::from(vec![Some(6.)])),
                ],
                None,
            )
            .unwrap(),
        );
        assert_eq!(eval_res, ScalarValue::Struct(expected));

        let merge_input = vec![
            Arc::new(UInt64Array::from(vec![Some(3), Some(42), None])) as arrow::array::ArrayRef,
            Arc::new(Float64Array::from(vec![Some(48.), Some(84.), None])),
        ];
        let merge_input_struct_arr = StructArray::try_new(
            vec![
                Field::new("state[0]", DataType::UInt64, true),
                Field::new("state[1]", DataType::Float64, true),
            ]
            .into(),
            merge_input,
            None,
        )
        .unwrap();
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
        merge_accum
            .update_batch(&[Arc::new(merge_input_struct_arr)])
            .unwrap();
        let merge_state = merge_accum.state().unwrap();
        assert_eq!(merge_state.len(), 2);
        assert_eq!(merge_state[0], ScalarValue::UInt64(Some(45)));
        assert_eq!(merge_state[1], ScalarValue::Float64(Some(132.)));

        let merge_eval_res = merge_accum.evaluate().unwrap();
        // the merge function returns the average, which is 132 / 45
        assert_eq!(merge_eval_res, ScalarValue::Float64(Some(132. / 45_f64)));
    }
}
