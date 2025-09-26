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

use common_query::error::Result;
use datafusion::arrow::datatypes::DataType;
use datafusion::logical_expr::ColumnarValue;
use datafusion::logical_expr_common::type_coercion::aggregates::{BINARYS, STRINGS};
use datafusion_common::ScalarValue;
use datafusion_expr::{ScalarFunctionArgs, Signature, TypeSignature, Volatility};
use nalgebra::DVectorView;

use crate::function::Function;
use crate::scalars::vector::VectorCalculator;
use crate::scalars::vector::impl_conv::{as_veclit, veclit_to_binlit};

const NAME: &str = "vec_norm";

/// Normalizes the vector to length 1, returns a vector.
/// This's equivalent to `VECTOR_SCALAR_MUL(1/SQRT(VECTOR_ELEM_SUM(VECTOR_MUL(v, v))), v)`.
///
/// # Example
///
/// ```sql
/// SELECT vec_to_string(vec_norm('[7.0, 8.0, 9.0]'));
///
/// +--------------------------------------------------+
/// | vec_to_string(vec_norm(Utf8("[7.0, 8.0, 9.0]"))) |
/// +--------------------------------------------------+
/// | [0.013888889,0.015873017,0.017857144]            |
/// +--------------------------------------------------+
///
/// ```
#[derive(Debug, Clone, Default)]
pub struct VectorNormFunction;

impl Function for VectorNormFunction {
    fn name(&self) -> &str {
        NAME
    }

    fn return_type(&self, _: &[DataType]) -> Result<DataType> {
        Ok(DataType::BinaryView)
    }

    fn signature(&self) -> Signature {
        Signature::one_of(
            vec![
                TypeSignature::Uniform(1, STRINGS.to_vec()),
                TypeSignature::Uniform(1, BINARYS.to_vec()),
                TypeSignature::Uniform(1, vec![DataType::BinaryView]),
            ],
            Volatility::Immutable,
        )
    }

    fn invoke_with_args(
        &self,
        args: ScalarFunctionArgs,
    ) -> datafusion_common::Result<ColumnarValue> {
        let body = |v0: &ScalarValue| -> datafusion_common::Result<ScalarValue> {
            let v0 = as_veclit(v0)?;
            let Some(v0) = v0 else {
                return Ok(ScalarValue::BinaryView(None));
            };

            let v0 = DVectorView::from_slice(&v0, v0.len());
            let result =
                veclit_to_binlit(v0.unscale(v0.component_mul(&v0).sum().sqrt()).as_slice());
            Ok(ScalarValue::BinaryView(Some(result)))
        };

        let calculator = VectorCalculator {
            name: self.name(),
            func: body,
        };
        calculator.invoke_with_single_argument(args)
    }
}

impl Display for VectorNormFunction {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", NAME.to_ascii_uppercase())
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use arrow_schema::Field;
    use datafusion::arrow::array::{Array, AsArray, StringViewArray};
    use datafusion_common::config::ConfigOptions;

    use super::*;

    #[test]
    fn test_vec_norm() {
        let func = VectorNormFunction;

        let input0 = Arc::new(StringViewArray::from(vec![
            Some("[0.0,2.0,3.0]".to_string()),
            Some("[1.0,2.0,3.0]".to_string()),
            Some("[7.0,8.0,9.0]".to_string()),
            Some("[7.0,-8.0,9.0]".to_string()),
            None,
        ]));

        let args = ScalarFunctionArgs {
            args: vec![ColumnarValue::Array(input0)],
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
        assert_eq!(result.len(), 5);
        assert_eq!(
            result.value(0),
            veclit_to_binlit(&[0.0, 0.5547002, 0.8320503]).as_slice()
        );
        assert_eq!(
            result.value(1),
            veclit_to_binlit(&[0.26726124, 0.5345225, 0.8017837]).as_slice()
        );
        assert_eq!(
            result.value(2),
            veclit_to_binlit(&[0.5025707, 0.5743665, 0.64616233]).as_slice()
        );
        assert_eq!(
            result.value(3),
            veclit_to_binlit(&[0.5025707, -0.5743665, 0.64616233]).as_slice()
        );
        assert!(result.is_null(4));
    }
}
