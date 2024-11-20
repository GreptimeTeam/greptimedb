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

use common_query::error::{InvalidFuncArgsSnafu, InvalidVectorStringSnafu, Result};
use common_query::prelude::{Signature, Volatility};
use datatypes::prelude::ConcreteDataType;
use datatypes::scalars::ScalarVectorBuilder;
use datatypes::types::parse_string_to_vector_type_value;
use datatypes::vectors::{BinaryVectorBuilder, MutableVector, VectorRef};
use snafu::{ensure, ResultExt};

use crate::function::{Function, FunctionContext};

const NAME: &str = "parse_vec";

#[derive(Debug, Clone, Default)]
pub struct ParseVectorFunction;

impl Function for ParseVectorFunction {
    fn name(&self) -> &str {
        NAME
    }

    fn return_type(&self, _input_types: &[ConcreteDataType]) -> Result<ConcreteDataType> {
        Ok(ConcreteDataType::binary_datatype())
    }

    fn signature(&self) -> Signature {
        Signature::exact(
            vec![ConcreteDataType::string_datatype()],
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

        let mut result = BinaryVectorBuilder::with_capacity(size);
        for i in 0..size {
            let value = column.get(i).as_string();
            if let Some(value) = value {
                let res = parse_string_to_vector_type_value(&value, None)
                    .context(InvalidVectorStringSnafu { vec_str: &value })?;
                result.push(Some(&res));
            } else {
                result.push_null();
            }
        }

        Ok(result.to_vector())
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

    use common_base::bytes::Bytes;
    use datatypes::value::Value;
    use datatypes::vectors::StringVector;

    use super::*;

    #[test]
    fn test_parse_vector() {
        let func = ParseVectorFunction;

        let input = Arc::new(StringVector::from(vec![
            Some("[1.0,2.0,3.0]".to_string()),
            Some("[4.0,5.0,6.0]".to_string()),
            None,
        ]));

        let result = func.eval(FunctionContext::default(), &[input]).unwrap();

        let result = result.as_ref();
        assert_eq!(result.len(), 3);
        assert_eq!(
            result.get(0),
            Value::Binary(Bytes::from(
                [1.0f32, 2.0, 3.0]
                    .iter()
                    .flat_map(|e| e.to_le_bytes())
                    .collect::<Vec<u8>>()
            ))
        );
        assert_eq!(
            result.get(1),
            Value::Binary(Bytes::from(
                [4.0f32, 5.0, 6.0]
                    .iter()
                    .flat_map(|e| e.to_le_bytes())
                    .collect::<Vec<u8>>()
            ))
        );
        assert!(result.get(2).is_null());
    }

    #[test]
    fn test_parse_vector_error() {
        let func = ParseVectorFunction;

        let input = Arc::new(StringVector::from(vec![
            Some("[1.0,2.0,3.0]".to_string()),
            Some("[4.0,5.0,6.0]".to_string()),
            Some("[7.0,8.0,9.0".to_string()),
        ]));

        let result = func.eval(FunctionContext::default(), &[input]);
        assert!(result.is_err());

        let input = Arc::new(StringVector::from(vec![
            Some("[1.0,2.0,3.0]".to_string()),
            Some("[4.0,5.0,6.0]".to_string()),
            Some("7.0,8.0,9.0]".to_string()),
        ]));

        let result = func.eval(FunctionContext::default(), &[input]);
        assert!(result.is_err());

        let input = Arc::new(StringVector::from(vec![
            Some("[1.0,2.0,3.0]".to_string()),
            Some("[4.0,5.0,6.0]".to_string()),
            Some("[7.0,hello,9.0]".to_string()),
        ]));

        let result = func.eval(FunctionContext::default(), &[input]);
        assert!(result.is_err());
    }
}
