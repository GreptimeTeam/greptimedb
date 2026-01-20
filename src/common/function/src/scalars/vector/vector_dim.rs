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

use datafusion::arrow::datatypes::DataType;
use datafusion::logical_expr::{Coercion, ColumnarValue, TypeSignatureClass};
use datafusion_common::ScalarValue;
use datafusion_common::types::{logical_binary, logical_string};
use datafusion_expr::{ScalarFunctionArgs, Signature, TypeSignature, Volatility};

use crate::function::Function;
use crate::scalars::vector::VectorCalculator;
use crate::scalars::vector::impl_conv::as_veclit;

const NAME: &str = "vec_dim";

/// Returns the dimension of the vector.
///
/// # Example
///
/// ```sql
/// SELECT vec_dim('[7.0, 8.0, 9.0, 10.0]');
///
/// +---------------------------------------------------------------+
/// | vec_dim(Utf8("[7.0, 8.0, 9.0, 10.0]"))                        |
/// +---------------------------------------------------------------+
/// | 4                                                             |
/// +---------------------------------------------------------------+
///
#[derive(Debug, Clone)]
pub(crate) struct VectorDimFunction {
    signature: Signature,
}

impl Default for VectorDimFunction {
    fn default() -> Self {
        Self {
            signature: Signature::one_of(
                vec![
                    TypeSignature::Coercible(vec![Coercion::new_exact(
                        TypeSignatureClass::Native(logical_binary()),
                    )]),
                    TypeSignature::Coercible(vec![Coercion::new_exact(
                        TypeSignatureClass::Native(logical_string()),
                    )]),
                ],
                Volatility::Immutable,
            ),
        }
    }
}

impl Function for VectorDimFunction {
    fn name(&self) -> &str {
        NAME
    }

    fn return_type(&self, _: &[DataType]) -> datafusion_common::Result<DataType> {
        Ok(DataType::UInt64)
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn invoke_with_args(
        &self,
        args: ScalarFunctionArgs,
    ) -> datafusion_common::Result<ColumnarValue> {
        let body = |v0: &ScalarValue| -> datafusion_common::Result<ScalarValue> {
            let v = as_veclit(v0)?.map(|v0| v0.len() as u64);
            Ok(ScalarValue::UInt64(v))
        };

        let calculator = VectorCalculator {
            name: self.name(),
            func: body,
        };
        calculator.invoke_with_single_argument(args)
    }
}

impl Display for VectorDimFunction {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", NAME.to_ascii_uppercase())
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use arrow_schema::Field;
    use datafusion::arrow::array::{Array, AsArray, StringViewArray};
    use datafusion::arrow::datatypes::UInt64Type;
    use datafusion_common::config::ConfigOptions;

    use super::*;

    #[test]
    fn test_vec_dim() {
        let func = VectorDimFunction::default();

        let input0 = Arc::new(StringViewArray::from(vec![
            Some("[0.0,2.0,3.0]".to_string()),
            Some("[1.0,2.0,3.0,4.0]".to_string()),
            None,
            Some("[5.0]".to_string()),
        ]));

        let args = ScalarFunctionArgs {
            args: vec![ColumnarValue::Array(input0)],
            arg_fields: vec![],
            number_rows: 4,
            return_field: Arc::new(Field::new("x", DataType::UInt64, false)),
            config_options: Arc::new(ConfigOptions::new()),
        };
        let result = func
            .invoke_with_args(args)
            .and_then(|x| x.to_array(4))
            .unwrap();

        let result = result.as_primitive::<UInt64Type>();
        assert_eq!(result.len(), 4);
        assert_eq!(result.value(0), 3);
        assert_eq!(result.value(1), 4);
        assert!(result.is_null(2));
        assert_eq!(result.value(3), 1);
    }

    #[test]
    fn test_dim_error() {
        let func = VectorDimFunction::default();

        let input0 = Arc::new(StringViewArray::from(vec![
            Some("[1.0,2.0,3.0]".to_string()),
            Some("[4.0,5.0,6.0]".to_string()),
            None,
            Some("[2.0,3.0,3.0]".to_string()),
        ]));
        let input1 = Arc::new(StringViewArray::from(vec![
            Some("[1.0,1.0,1.0]".to_string()),
            Some("[6.0,5.0,4.0]".to_string()),
            Some("[3.0,2.0,2.0]".to_string()),
        ]));

        let args = ScalarFunctionArgs {
            args: vec![ColumnarValue::Array(input0), ColumnarValue::Array(input1)],
            arg_fields: vec![],
            number_rows: 4,
            return_field: Arc::new(Field::new("x", DataType::UInt64, false)),
            config_options: Arc::new(ConfigOptions::new()),
        };
        let e = func.invoke_with_args(args).unwrap_err();
        assert!(
            e.to_string()
                .starts_with("Execution error: vec_dim function requires 1 argument, got 2")
        )
    }
}
