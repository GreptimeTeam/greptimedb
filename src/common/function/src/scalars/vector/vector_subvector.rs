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

use std::fmt::Display;
use std::sync::Arc;

use common_query::error::{InvalidFuncArgsSnafu, Result};
use datafusion::arrow::array::{Array, AsArray, BinaryViewBuilder};
use datafusion::arrow::datatypes::Int64Type;
use datafusion::logical_expr::ColumnarValue;
use datafusion_common::ScalarValue;
use datafusion_expr::{ScalarFunctionArgs, Signature, TypeSignature, Volatility};
use datatypes::arrow::datatypes::DataType;
use snafu::ensure;

use crate::function::{Function, extract_args};
use crate::scalars::vector::impl_conv::{as_veclit, veclit_to_binlit};

const NAME: &str = "vec_subvector";

/// Returns a subvector from start(included) to end(excluded) index.
///
/// # Example
///
/// ```sql
/// SELECT vec_to_string(vec_subvector("[1, 2, 3, 4, 5]", 1, 3)) as result;
///
/// +---------+
/// | result  |
/// +---------+
/// | [2, 3]  |
/// +---------+
///
/// ```
///

#[derive(Debug, Clone, Default)]
pub struct VectorSubvectorFunction;

impl Function for VectorSubvectorFunction {
    fn name(&self) -> &str {
        NAME
    }

    fn return_type(&self, _: &[DataType]) -> Result<DataType> {
        Ok(DataType::BinaryView)
    }

    fn signature(&self) -> Signature {
        Signature::one_of(
            vec![
                TypeSignature::Exact(vec![DataType::Utf8, DataType::Int64, DataType::Int64]),
                TypeSignature::Exact(vec![DataType::Binary, DataType::Int64, DataType::Int64]),
            ],
            Volatility::Immutable,
        )
    }

    fn invoke_with_args(
        &self,
        args: ScalarFunctionArgs,
    ) -> datafusion_common::Result<ColumnarValue> {
        let [arg0, arg1, arg2] = extract_args(self.name(), &args)?;
        let arg1 = arg1.as_primitive::<Int64Type>();
        let arg2 = arg2.as_primitive::<Int64Type>();

        let len = arg0.len();
        let mut builder = BinaryViewBuilder::with_capacity(len);
        if len == 0 {
            return Ok(ColumnarValue::Array(Arc::new(builder.finish())));
        }

        for i in 0..len {
            let v = ScalarValue::try_from_array(&arg0, i)?;
            let arg0 = as_veclit(&v)?;
            let arg1 = arg1.is_valid(i).then(|| arg1.value(i));
            let arg2 = arg2.is_valid(i).then(|| arg2.value(i));
            let (Some(arg0), Some(arg1), Some(arg2)) = (arg0, arg1, arg2) else {
                builder.append_null();
                continue;
            };

            ensure!(
                0 <= arg1 && arg1 <= arg2 && arg2 as usize <= arg0.len(),
                InvalidFuncArgsSnafu {
                    err_msg: format!(
                        "Invalid start and end indices: start={}, end={}, vec_len={}",
                        arg1,
                        arg2,
                        arg0.len()
                    )
                }
            );

            let subvector = &arg0[arg1 as usize..arg2 as usize];
            let binlit = veclit_to_binlit(subvector);
            builder.append_value(&binlit);
        }

        Ok(ColumnarValue::Array(Arc::new(builder.finish())))
    }
}

impl Display for VectorSubvectorFunction {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", NAME.to_ascii_uppercase())
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use arrow_schema::Field;
    use datafusion::arrow::array::{ArrayRef, Int64Array, StringViewArray};
    use datafusion_common::config::ConfigOptions;

    use super::*;

    #[test]
    fn test_subvector() {
        let func = VectorSubvectorFunction;

        let input0: ArrayRef = Arc::new(StringViewArray::from(vec![
            Some("[1.0, 2.0, 3.0, 4.0, 5.0]".to_string()),
            Some("[6.0, 7.0, 8.0, 9.0, 10.0]".to_string()),
            None,
            Some("[11.0, 12.0, 13.0]".to_string()),
        ]));
        let input1: ArrayRef = Arc::new(Int64Array::from(vec![Some(1), Some(0), Some(0), Some(1)]));
        let input2: ArrayRef = Arc::new(Int64Array::from(vec![Some(3), Some(5), Some(2), Some(3)]));

        let args = ScalarFunctionArgs {
            args: vec![
                ColumnarValue::Array(input0),
                ColumnarValue::Array(input1),
                ColumnarValue::Array(input2),
            ],
            arg_fields: vec![],
            number_rows: 5,
            return_field: Arc::new(Field::new("x", DataType::BinaryView, false)),
            config_options: Arc::new(ConfigOptions::new()),
        };
        let result = func
            .invoke_with_args(args)
            .and_then(|x| x.to_array(5))
            .unwrap();

        let result = result.as_binary_view();
        assert_eq!(result.len(), 4);
        assert_eq!(result.value(0), veclit_to_binlit(&[2.0, 3.0]).as_slice());
        assert_eq!(
            result.value(1),
            veclit_to_binlit(&[6.0, 7.0, 8.0, 9.0, 10.0]).as_slice()
        );
        assert!(result.is_null(2));
        assert_eq!(result.value(3), veclit_to_binlit(&[12.0, 13.0]).as_slice());
    }
    #[test]
    fn test_subvector_error() {
        let func = VectorSubvectorFunction;

        let input0: ArrayRef = Arc::new(StringViewArray::from(vec![
            Some("[1.0, 2.0, 3.0]".to_string()),
            Some("[4.0, 5.0, 6.0]".to_string()),
        ]));
        let input1: ArrayRef = Arc::new(Int64Array::from(vec![Some(1), Some(2)]));
        let input2: ArrayRef = Arc::new(Int64Array::from(vec![Some(3)]));

        let args = ScalarFunctionArgs {
            args: vec![
                ColumnarValue::Array(input0),
                ColumnarValue::Array(input1),
                ColumnarValue::Array(input2),
            ],
            arg_fields: vec![],
            number_rows: 3,
            return_field: Arc::new(Field::new("x", DataType::BinaryView, false)),
            config_options: Arc::new(ConfigOptions::new()),
        };
        let e = func.invoke_with_args(args).unwrap_err();
        assert!(e.to_string().starts_with(
            "Internal error: Arguments has mixed length. Expected length: 2, found length: 1."
        ));
    }

    #[test]
    fn test_subvector_invalid_indices() {
        let func = VectorSubvectorFunction;

        let input0 = Arc::new(StringViewArray::from(vec![
            Some("[1.0, 2.0, 3.0]".to_string()),
            Some("[4.0, 5.0, 6.0]".to_string()),
        ]));
        let input1 = Arc::new(Int64Array::from(vec![Some(1), Some(3)]));
        let input2 = Arc::new(Int64Array::from(vec![Some(3), Some(4)]));

        let args = ScalarFunctionArgs {
            args: vec![
                ColumnarValue::Array(input0),
                ColumnarValue::Array(input1),
                ColumnarValue::Array(input2),
            ],
            arg_fields: vec![],
            number_rows: 3,
            return_field: Arc::new(Field::new("x", DataType::BinaryView, false)),
            config_options: Arc::new(ConfigOptions::new()),
        };
        let e = func.invoke_with_args(args).unwrap_err();
        assert!(e.to_string().starts_with("External error: Invalid function args: Invalid start and end indices: start=3, end=4, vec_len=3"));
    }
}
