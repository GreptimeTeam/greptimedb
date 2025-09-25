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
use datafusion_common::ScalarValue;
use datafusion_expr::{ScalarFunctionArgs, Signature};
use nalgebra::DVectorView;

use crate::function::Function;
use crate::helper;
use crate::scalars::vector::VectorCalculator;
use crate::scalars::vector::impl_conv::{as_veclit, veclit_to_binlit};

const NAME: &str = "vec_scalar_add";

/// Adds a scalar to each element of a vector.
///
/// # Example
///
/// ```sql
/// SELECT vec_to_string(vec_scalar_add(1, "[1, 2, 3]")) as result;
///
/// +---------+
/// | result  |
/// +---------+
/// | [2,3,4] |
/// +---------+
///
/// -- Negative scalar to simulate subtraction
/// SELECT vec_to_string(vec_scalar_add(-1, "[1, 2, 3]")) as result;
///
/// +---------+
/// | result  |
/// +---------+
/// | [0,1,2] |
/// +---------+
/// ```
#[derive(Debug, Clone, Default)]
pub struct ScalarAddFunction;

impl Function for ScalarAddFunction {
    fn name(&self) -> &str {
        NAME
    }

    fn return_type(&self, _: &[DataType]) -> Result<DataType> {
        Ok(DataType::BinaryView)
    }

    fn signature(&self) -> Signature {
        helper::one_of_sigs2(
            vec![DataType::Float64],
            vec![DataType::Utf8, DataType::Binary, DataType::BinaryView],
        )
    }

    fn invoke_with_args(
        &self,
        args: ScalarFunctionArgs,
    ) -> datafusion_common::Result<ColumnarValue> {
        let body = |v0: &ScalarValue, v1: &ScalarValue| -> datafusion_common::Result<ScalarValue> {
            let ScalarValue::Float64(Some(v0)) = v0 else {
                return Ok(ScalarValue::BinaryView(None));
            };

            let v1 = as_veclit(v1)?
                .map(|v1| DVectorView::from_slice(&v1, v1.len()).add_scalar(*v0 as f32));
            let result = v1.map(|v1| veclit_to_binlit(v1.as_slice()));
            Ok(ScalarValue::BinaryView(result))
        };

        let calculator = VectorCalculator {
            name: self.name(),
            func: body,
        };
        calculator.invoke_with_args(args)
    }
}

impl Display for ScalarAddFunction {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", NAME.to_ascii_uppercase())
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use arrow_schema::Field;
    use datafusion::arrow::array::{Array, AsArray, Float64Array, StringViewArray};
    use datafusion_common::config::ConfigOptions;

    use super::*;

    #[test]
    fn test_scalar_add() {
        let func = ScalarAddFunction;

        let input0 = Arc::new(Float64Array::from(vec![
            Some(1.0),
            Some(-1.0),
            None,
            Some(3.0),
        ]));
        let input1 = Arc::new(StringViewArray::from(vec![
            Some("[1.0,2.0,3.0]".to_string()),
            Some("[4.0,5.0,6.0]".to_string()),
            Some("[7.0,8.0,9.0]".to_string()),
            None,
        ]));

        let args = ScalarFunctionArgs {
            args: vec![ColumnarValue::Array(input0), ColumnarValue::Array(input1)],
            arg_fields: vec![],
            number_rows: 4,
            return_field: Arc::new(Field::new("x", DataType::BinaryView, false)),
            config_options: Arc::new(ConfigOptions::new()),
        };
        let result = func
            .invoke_with_args(args)
            .and_then(|x| x.to_array(4))
            .unwrap();

        let result = result.as_binary_view();
        assert_eq!(result.len(), 4);
        assert_eq!(
            result.value(0),
            veclit_to_binlit(&[2.0, 3.0, 4.0]).as_slice()
        );
        assert_eq!(
            result.value(1),
            veclit_to_binlit(&[3.0, 4.0, 5.0]).as_slice()
        );
        assert!(result.is_null(2));
        assert!(result.is_null(3));
    }
}
