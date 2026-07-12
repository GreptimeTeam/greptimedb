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

//! MySQL-compatible SPACE function implementation.
//!
//! SPACE(N) - Returns a string consisting of N space characters.

use std::fmt;
use std::sync::Arc;

use datafusion_common::DataFusionError;
use datafusion_common::arrow::array::{Array, AsArray, LargeStringBuilder};
use datafusion_common::arrow::datatypes::DataType;
use datafusion_expr::{ColumnarValue, ScalarFunctionArgs, Signature, TypeSignature, Volatility};

use crate::function::Function;
use crate::function_registry::FunctionRegistry;

const NAME: &str = "space";

// Safety limit for maximum number of spaces
const MAX_SPACE_COUNT: i64 = 1024 * 1024; // 1MB of spaces

/// MySQL-compatible SPACE function.
///
/// Syntax: SPACE(N)
/// Returns a string consisting of N space characters.
/// Returns NULL if N is NULL.
/// Returns empty string if N < 0.
#[derive(Debug)]
pub struct SpaceFunction {
    signature: Signature,
}

impl SpaceFunction {
    pub fn register(registry: &FunctionRegistry) {
        registry.register_scalar(SpaceFunction::default());
    }
}

impl Default for SpaceFunction {
    fn default() -> Self {
        Self {
            signature: Signature::one_of(
                vec![
                    TypeSignature::Exact(vec![DataType::Int64]),
                    TypeSignature::Exact(vec![DataType::Int32]),
                    TypeSignature::Exact(vec![DataType::Int16]),
                    TypeSignature::Exact(vec![DataType::Int8]),
                    TypeSignature::Exact(vec![DataType::UInt64]),
                    TypeSignature::Exact(vec![DataType::UInt32]),
                    TypeSignature::Exact(vec![DataType::UInt16]),
                    TypeSignature::Exact(vec![DataType::UInt8]),
                ],
                Volatility::Immutable,
            ),
        }
    }
}

impl fmt::Display for SpaceFunction {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "{}", NAME.to_ascii_uppercase())
    }
}

impl Function for SpaceFunction {
    fn name(&self) -> &str {
        NAME
    }

    fn return_type(&self, _: &[DataType]) -> datafusion_common::Result<DataType> {
        Ok(DataType::LargeUtf8)
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn invoke_with_args(
        &self,
        args: ScalarFunctionArgs,
    ) -> datafusion_common::Result<ColumnarValue> {
        if args.args.len() != 1 {
            return Err(DataFusionError::Execution(
                "SPACE requires exactly 1 argument: SPACE(N)".to_string(),
            ));
        }

        let arrays = ColumnarValue::values_to_arrays(&args.args)?;
        let len = arrays[0].len();
        let n_array = &arrays[0];

        let mut builder = LargeStringBuilder::with_capacity(len, len * 10);

        for i in 0..len {
            if n_array.is_null(i) {
                builder.append_null();
                continue;
            }

            let n = get_int_value(n_array, i)?;

            if n < 0 {
                // MySQL returns empty string for negative values
                builder.append_value("");
            } else if n > MAX_SPACE_COUNT {
                return Err(DataFusionError::Execution(format!(
                    "SPACE: requested {} spaces exceeds maximum allowed ({})",
                    n, MAX_SPACE_COUNT
                )));
            } else {
                let spaces = " ".repeat(n as usize);
                builder.append_value(&spaces);
            }
        }

        Ok(ColumnarValue::Array(Arc::new(builder.finish())))
    }
}

/// Extract integer value from various integer types.
fn get_int_value(
    array: &datafusion_common::arrow::array::ArrayRef,
    index: usize,
) -> datafusion_common::Result<i64> {
    use datafusion_common::arrow::datatypes as arrow_types;

    match array.data_type() {
        DataType::Int64 => Ok(array.as_primitive::<arrow_types::Int64Type>().value(index)),
        DataType::Int32 => Ok(array.as_primitive::<arrow_types::Int32Type>().value(index) as i64),
        DataType::Int16 => Ok(array.as_primitive::<arrow_types::Int16Type>().value(index) as i64),
        DataType::Int8 => Ok(array.as_primitive::<arrow_types::Int8Type>().value(index) as i64),
        DataType::UInt64 => {
            let v = array.as_primitive::<arrow_types::UInt64Type>().value(index);
            if v > i64::MAX as u64 {
                Err(DataFusionError::Execution(format!(
                    "SPACE: value {} exceeds maximum",
                    v
                )))
            } else {
                Ok(v as i64)
            }
        }
        DataType::UInt32 => Ok(array.as_primitive::<arrow_types::UInt32Type>().value(index) as i64),
        DataType::UInt16 => Ok(array.as_primitive::<arrow_types::UInt16Type>().value(index) as i64),
        DataType::UInt8 => Ok(array.as_primitive::<arrow_types::UInt8Type>().value(index) as i64),
        _ => Err(DataFusionError::Execution(format!(
            "SPACE: unsupported type {:?}",
            array.data_type()
        ))),
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use datafusion_common::arrow::array::Int64Array;
    use datafusion_common::arrow::datatypes::Field;
    use datafusion_expr::ScalarFunctionArgs;

    use super::*;

    fn create_args(arrays: Vec<datafusion_common::arrow::array::ArrayRef>) -> ScalarFunctionArgs {
        let arg_fields: Vec<_> = arrays
            .iter()
            .enumerate()
            .map(|(i, arr)| {
                Arc::new(Field::new(
                    format!("arg_{}", i),
                    arr.data_type().clone(),
                    true,
                ))
            })
            .collect();

        ScalarFunctionArgs {
            args: arrays.iter().cloned().map(ColumnarValue::Array).collect(),
            arg_fields,
            return_field: Arc::new(Field::new("result", DataType::LargeUtf8, true)),
            number_rows: arrays[0].len(),
            config_options: Arc::new(datafusion_common::config::ConfigOptions::default()),
        }
    }

    #[test]
    fn test_space_basic() {
        let function = SpaceFunction::default();

        let n = Arc::new(Int64Array::from(vec![0, 1, 5]));

        let args = create_args(vec![n]);
        let result = function.invoke_with_args(args).unwrap();

        if let ColumnarValue::Array(array) = result {
            let str_array = array.as_string::<i64>();
            assert_eq!(str_array.value(0), "");
            assert_eq!(str_array.value(1), " ");
            assert_eq!(str_array.value(2), "     ");
        } else {
            panic!("Expected array result");
        }
    }

    #[test]
    fn test_space_negative() {
        let function = SpaceFunction::default();

        let n = Arc::new(Int64Array::from(vec![-1, -100]));

        let args = create_args(vec![n]);
        let result = function.invoke_with_args(args).unwrap();

        if let ColumnarValue::Array(array) = result {
            let str_array = array.as_string::<i64>();
            assert_eq!(str_array.value(0), "");
            assert_eq!(str_array.value(1), "");
        } else {
            panic!("Expected array result");
        }
    }

    #[test]
    fn test_space_with_nulls() {
        let function = SpaceFunction::default();

        let n = Arc::new(Int64Array::from(vec![Some(3), None]));

        let args = create_args(vec![n]);
        let result = function.invoke_with_args(args).unwrap();

        if let ColumnarValue::Array(array) = result {
            let str_array = array.as_string::<i64>();
            assert_eq!(str_array.value(0), "   ");
            assert!(str_array.is_null(1));
        } else {
            panic!("Expected array result");
        }
    }
}
