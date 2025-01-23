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

use common_query::error::{InvalidFuncArgsSnafu, Result};
use common_query::prelude::{Signature, Volatility};
use datatypes::prelude::ConcreteDataType;
use datatypes::scalars::ScalarVectorBuilder;
use datatypes::types::vector_type_value_to_string;
use datatypes::value::Value;
use datatypes::vectors::{MutableVector, StringVectorBuilder, VectorRef};
use snafu::ensure;

use crate::function::{Function, FunctionContext};

const NAME: &str = "vec_to_string";

#[derive(Debug, Clone, Default)]
pub struct VectorToStringFunction;

impl Function for VectorToStringFunction {
    fn name(&self) -> &str {
        NAME
    }

    fn return_type(&self, _input_types: &[ConcreteDataType]) -> Result<ConcreteDataType> {
        Ok(ConcreteDataType::string_datatype())
    }

    fn signature(&self) -> Signature {
        Signature::exact(
            vec![ConcreteDataType::binary_datatype()],
            Volatility::Immutable,
        )
    }

    fn eval(&self, _func_ctx: FunctionContext, columns: &[VectorRef]) -> Result<VectorRef> {
        ensure!(
            columns.len() == 1,
            InvalidFuncArgsSnafu {
                err_msg: format!(
                    "The length of the args is not correct, expect exactly one, have: {}",
                    columns.len()
                ),
            }
        );

        let column = &columns[0];
        let size = column.len();

        let mut result = StringVectorBuilder::with_capacity(size);
        for i in 0..size {
            let value = column.get(i);
            match value {
                Value::Binary(bytes) => {
                    let len = bytes.len();
                    if len % std::mem::size_of::<f32>() != 0 {
                        return InvalidFuncArgsSnafu {
                            err_msg: format!("Invalid binary length of vector: {}", len),
                        }
                        .fail();
                    }

                    let dim = len / std::mem::size_of::<f32>();
                    // Safety: `dim` is calculated from the length of `bytes` and is guaranteed to be valid
                    let res = vector_type_value_to_string(&bytes, dim as _).unwrap();
                    result.push(Some(&res));
                }
                Value::Null => {
                    result.push_null();
                }
                _ => {
                    return InvalidFuncArgsSnafu {
                        err_msg: format!("Invalid value type: {:?}", value.data_type()),
                    }
                    .fail();
                }
            }
        }

        Ok(result.to_vector())
    }
}

impl Display for VectorToStringFunction {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", NAME.to_ascii_uppercase())
    }
}

#[cfg(test)]
mod tests {
    use datatypes::value::Value;
    use datatypes::vectors::BinaryVectorBuilder;

    use super::*;

    #[test]
    fn test_vector_to_string() {
        let func = VectorToStringFunction;

        let mut builder = BinaryVectorBuilder::with_capacity(3);
        builder.push(Some(
            [1.0f32, 2.0, 3.0]
                .iter()
                .flat_map(|e| e.to_le_bytes())
                .collect::<Vec<_>>()
                .as_slice(),
        ));
        builder.push(Some(
            [4.0f32, 5.0, 6.0]
                .iter()
                .flat_map(|e| e.to_le_bytes())
                .collect::<Vec<_>>()
                .as_slice(),
        ));
        builder.push_null();
        let vector = builder.to_vector();

        let result = func.eval(FunctionContext::default(), &[vector]).unwrap();

        assert_eq!(result.len(), 3);
        assert_eq!(result.get(0), Value::String("[1,2,3]".to_string().into()));
        assert_eq!(result.get(1), Value::String("[4,5,6]".to_string().into()));
        assert_eq!(result.get(2), Value::Null);
    }
}
