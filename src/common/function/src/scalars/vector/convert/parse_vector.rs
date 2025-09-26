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

use common_query::error::{InvalidVectorStringSnafu, Result};
use datafusion_common::arrow::array::{Array, AsArray, BinaryViewBuilder};
use datafusion_common::arrow::compute;
use datafusion_common::arrow::datatypes::DataType;
use datafusion_expr::{ColumnarValue, ScalarFunctionArgs, Signature, Volatility};
use datatypes::types::parse_string_to_vector_type_value;
use snafu::ResultExt;

use crate::function::{Function, extract_args};

const NAME: &str = "parse_vec";

#[derive(Debug, Clone, Default)]
pub struct ParseVectorFunction;

impl Function for ParseVectorFunction {
    fn name(&self) -> &str {
        NAME
    }

    fn return_type(&self, _: &[DataType]) -> Result<DataType> {
        Ok(DataType::BinaryView)
    }

    fn signature(&self) -> Signature {
        Signature::string(1, Volatility::Immutable)
    }

    fn invoke_with_args(
        &self,
        args: ScalarFunctionArgs,
    ) -> datafusion_common::Result<ColumnarValue> {
        let [arg0] = extract_args(self.name(), &args)?;
        let arg0 = compute::cast(&arg0, &DataType::Utf8View)?;
        let column = arg0.as_string_view();

        let size = column.len();

        let mut builder = BinaryViewBuilder::with_capacity(size);
        for i in 0..size {
            let value = column.is_valid(i).then(|| column.value(i));
            if let Some(value) = value {
                let result = parse_string_to_vector_type_value(value, None)
                    .context(InvalidVectorStringSnafu { vec_str: value })?;
                builder.append_value(result);
            } else {
                builder.append_null();
            }
        }

        Ok(ColumnarValue::Array(Arc::new(builder.finish())))
    }
}

impl Display for ParseVectorFunction {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", NAME.to_ascii_uppercase())
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use arrow_schema::Field;
    use common_base::bytes::Bytes;
    use datafusion_common::arrow::array::StringViewArray;

    use super::*;

    #[test]
    fn test_parse_vector() {
        let func = ParseVectorFunction;

        let arg0 = Arc::new(StringViewArray::from_iter([
            Some("[1.0,2.0,3.0]".to_string()),
            Some("[4.0,5.0,6.0]".to_string()),
            None,
        ]));
        let args = ScalarFunctionArgs {
            args: vec![ColumnarValue::Array(arg0)],
            arg_fields: vec![],
            number_rows: 3,
            return_field: Arc::new(Field::new("", DataType::BinaryView, false)),
            config_options: Arc::new(Default::default()),
        };

        let result = func
            .invoke_with_args(args)
            .and_then(|x| x.to_array(3))
            .unwrap();
        let result = result.as_binary_view();

        assert_eq!(result.len(), 3);
        assert_eq!(
            result.value(0),
            &Bytes::from(
                [1.0f32, 2.0, 3.0]
                    .iter()
                    .flat_map(|e| e.to_le_bytes())
                    .collect::<Vec<u8>>()
            )
        );
        assert_eq!(
            result.value(1),
            &Bytes::from(
                [4.0f32, 5.0, 6.0]
                    .iter()
                    .flat_map(|e| e.to_le_bytes())
                    .collect::<Vec<u8>>()
            )
        );
        assert!(result.is_null(2));
    }

    #[test]
    fn test_parse_vector_error() {
        let func = ParseVectorFunction;

        let inputs = [
            StringViewArray::from_iter([
                Some("[1.0,2.0,3.0]".to_string()),
                Some("[4.0,5.0,6.0]".to_string()),
                Some("[7.0,8.0,9.0".to_string()),
            ]),
            StringViewArray::from_iter([
                Some("[1.0,2.0,3.0]".to_string()),
                Some("[4.0,5.0,6.0]".to_string()),
                Some("7.0,8.0,9.0]".to_string()),
            ]),
            StringViewArray::from_iter([
                Some("[1.0,2.0,3.0]".to_string()),
                Some("[4.0,5.0,6.0]".to_string()),
                Some("[7.0,hello,9.0]".to_string()),
            ]),
        ];
        let expected = [
            "External error: Invalid vector string: [7.0,8.0,9.0",
            "External error: Invalid vector string: 7.0,8.0,9.0]",
            "External error: Invalid vector string: [7.0,hello,9.0]",
        ];

        for (input, expected) in inputs.into_iter().zip(expected.into_iter()) {
            let args = ScalarFunctionArgs {
                args: vec![ColumnarValue::Array(Arc::new(input))],
                arg_fields: vec![],
                number_rows: 3,
                return_field: Arc::new(Field::new("", DataType::BinaryView, false)),
                config_options: Arc::new(Default::default()),
            };
            let result = func.invoke_with_args(args);
            assert_eq!(result.unwrap_err().to_string(), expected);
        }
    }
}
