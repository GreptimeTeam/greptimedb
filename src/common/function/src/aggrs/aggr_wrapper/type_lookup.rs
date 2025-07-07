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

use std::collections::BTreeMap;

use datafusion_expr::function::StateFieldsArgs;
use datafusion_expr::{AggregateUDF, TypeSignature};
use datatypes::arrow::datatypes::DataType;

/// Find a valid input types from given state types.
///
/// Useful for merge functions that need to merge to use correct accumulator to merge states.
#[derive(Debug, Default, Clone, PartialEq, Eq)]
pub struct StateTypeLookup {
    /// A mapping from state types to input types.
    pub mapping: BTreeMap<Vec<DataType>, Vec<DataType>>,
}

impl StateTypeLookup {
    /// Returns the input types for the given state types.
    ///
    /// Also handle certain data types that have extra args, like `Decimal128/256`, assuming only one of
    /// the `Decimal128/256` in the state types, and replace it with the same type in the returned input types.
    pub fn get_input_types(
        &self,
        state_types: &[DataType],
    ) -> datafusion_common::Result<Option<Vec<DataType>>> {
        let state_types = if state_types.len() == 1 && matches!(state_types[0], DataType::Struct(_))
        {
            // unpack single struct type
            let struct_type = state_types[0].clone();
            if let DataType::Struct(struct_ty) = struct_type {
                let state_types = struct_ty
                    .iter()
                    .map(|f| f.data_type().clone())
                    .collect::<Vec<_>>();
                state_types
            } else {
                state_types.to_vec()
            }
        } else {
            state_types.to_vec()
        };

        let is_decimal =
            |ty: &DataType| matches!(ty, DataType::Decimal128(_, _) | DataType::Decimal256(_, _));
        let need_change_ty = state_types.iter().any(is_decimal);
        if !need_change_ty {
            return Ok(self.mapping.get(&state_types).cloned());
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
                // Expect only one of the `Decimal128/256` in the state_types.
                // If there are multiple, this logic might need to be revisited.
                let mut input_types = v.clone();
                let decimal_types_in_state: Vec<&DataType> =
                    state_types.iter().filter(|ty| is_decimal(ty)).collect();

                if decimal_types_in_state.len() > 1 {
                    return Err(datafusion_common::DataFusionError::Internal(format!(
                        "Multiple Decimal types found in state types, expected at most one: {:?}",
                        state_types
                    )));
                }

                if let Some(new_decimal_type) = decimal_types_in_state.first() {
                    input_types = input_types
                        .into_iter()
                        .map(|ty| {
                            if is_decimal(&ty) {
                                (*new_decimal_type).clone()
                            } else {
                                ty
                            }
                        })
                        .collect::<Vec<_>>();
                }
                return Ok(Some(input_types));
            }
        }
        Ok(None)
    }
}

#[allow(unused)]
pub(crate) fn get_possible_state_to_input_types(
    inner: &AggregateUDF,
) -> datafusion_common::Result<StateTypeLookup> {
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

    Ok(StateTypeLookup { mapping })
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
        Self::new(inner)
    }
}

impl Iterator for DataTypeIterator {
    type Item = DataType;

    fn next(&mut self) -> Option<Self::Item> {
        self.inner.pop()
    }
}
