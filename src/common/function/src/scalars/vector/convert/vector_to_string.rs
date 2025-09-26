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

use common_query::error::Result;
use datafusion_common::DataFusionError;
use datafusion_common::arrow::array::{Array, AsArray, StringViewBuilder};
use datafusion_common::arrow::compute;
use datafusion_common::arrow::datatypes::DataType;
use datafusion_expr::type_coercion::aggregates::BINARYS;
use datafusion_expr::{ColumnarValue, ScalarFunctionArgs, Signature, TypeSignature, Volatility};
use datatypes::types::vector_type_value_to_string;

use crate::function::{Function, extract_args};

const NAME: &str = "vec_to_string";

#[derive(Debug, Clone, Default)]
pub struct VectorToStringFunction;

impl Function for VectorToStringFunction {
    fn name(&self) -> &str {
        NAME
    }

    fn return_type(&self, _: &[DataType]) -> Result<DataType> {
        Ok(DataType::Utf8View)
    }

    fn signature(&self) -> Signature {
        Signature::one_of(
            vec![
                TypeSignature::Uniform(1, vec![DataType::BinaryView]),
                TypeSignature::Uniform(1, BINARYS.to_vec()),
            ],
            Volatility::Immutable,
        )
    }

    fn invoke_with_args(
        &self,
        args: ScalarFunctionArgs,
    ) -> datafusion_common::Result<ColumnarValue> {
        let [arg0] = extract_args(self.name(), &args)?;
        let arg0 = compute::cast(&arg0, &DataType::BinaryView)?;
        let column = arg0.as_binary_view();

        let size = column.len();

        let mut builder = StringViewBuilder::with_capacity(size);
        for i in 0..size {
            let value = column.is_valid(i).then(|| column.value(i));
            match value {
                Some(bytes) => {
                    let len = bytes.len();
                    if len % std::mem::size_of::<f32>() != 0 {
                        return Err(DataFusionError::Execution(format!(
                            "Invalid binary length of vector: {len}"
                        )));
                    }

                    let dim = len / std::mem::size_of::<f32>();
                    // Safety: `dim` is calculated from the length of `bytes` and is guaranteed to be valid
                    let result = vector_type_value_to_string(bytes, dim as _).unwrap();
                    builder.append_value(result);
                }
                None => {
                    builder.append_null();
                }
            }
        }

        Ok(ColumnarValue::Array(Arc::new(builder.finish())))
    }
}

impl Display for VectorToStringFunction {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", NAME.to_ascii_uppercase())
    }
}

#[cfg(test)]
mod tests {
    use arrow_schema::Field;
    use datafusion_common::arrow::array::BinaryViewBuilder;

    use super::*;

    #[test]
    fn test_vector_to_string() {
        let func = VectorToStringFunction;

        let mut builder = BinaryViewBuilder::with_capacity(3);
        builder.append_option(Some(
            [1.0f32, 2.0, 3.0]
                .iter()
                .flat_map(|e| e.to_le_bytes())
                .collect::<Vec<_>>()
                .as_slice(),
        ));
        builder.append_option(Some(
            [4.0f32, 5.0, 6.0]
                .iter()
                .flat_map(|e| e.to_le_bytes())
                .collect::<Vec<_>>()
                .as_slice(),
        ));
        builder.append_null();
        let args = ScalarFunctionArgs {
            args: vec![ColumnarValue::Array(Arc::new(builder.finish()))],
            arg_fields: vec![],
            number_rows: 3,
            return_field: Arc::new(Field::new("", DataType::Utf8View, false)),
            config_options: Arc::new(Default::default()),
        };

        let result = func
            .invoke_with_args(args)
            .and_then(|x| x.to_array(3))
            .unwrap();
        let result = result.as_string_view();

        assert_eq!(result.len(), 3);
        assert_eq!(result.value(0), "[1,2,3]");
        assert_eq!(result.value(1), "[4,5,6]");
        assert!(result.is_null(2));
    }
}
