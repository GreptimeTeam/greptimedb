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

use std::borrow::Cow;
use std::fmt::Display;

use common_query::error::Result;
use datafusion::arrow::datatypes::DataType;
use datafusion::logical_expr::ColumnarValue;
use datafusion_common::{DataFusionError, ScalarValue};
use datafusion_expr::{ScalarFunctionArgs, Signature};
use nalgebra::DVectorView;

use crate::function::Function;
use crate::helper;
use crate::scalars::vector::VectorCalculator;
use crate::scalars::vector::impl_conv::veclit_to_binlit;

const NAME: &str = "vec_div";

/// Divides corresponding elements of two vectors.
///
/// # Example
///
/// ```sql
/// SELECT vec_to_string(vec_div("[2, 4, 6]", "[2, 2, 2]")) as result;
///
/// +---------+
/// | result  |
/// +---------+
/// | [1,2,3] |
/// +---------+
///
/// ```
#[derive(Debug, Clone, Default)]
pub struct VectorDivFunction;

impl Function for VectorDivFunction {
    fn name(&self) -> &str {
        NAME
    }

    fn return_type(&self, _: &[DataType]) -> Result<DataType> {
        Ok(DataType::BinaryView)
    }

    fn signature(&self) -> Signature {
        helper::one_of_sigs2(
            vec![DataType::Utf8, DataType::Binary, DataType::BinaryView],
            vec![DataType::Utf8, DataType::Binary, DataType::BinaryView],
        )
    }

    fn invoke_with_args(
        &self,
        args: ScalarFunctionArgs,
    ) -> datafusion_common::Result<ColumnarValue> {
        let body = |v0: &Option<Cow<[f32]>>,
                    v1: &Option<Cow<[f32]>>|
         -> datafusion_common::Result<ScalarValue> {
            let result = if let (Some(v0), Some(v1)) = (v0, v1) {
                let v0 = DVectorView::from_slice(v0, v0.len());
                let v1 = DVectorView::from_slice(v1, v1.len());
                if v0.len() != v1.len() {
                    return Err(DataFusionError::Execution(format!(
                        "vectors length not match: {}",
                        self.name()
                    )));
                }

                let result = veclit_to_binlit((v0.component_div(&v1)).as_slice());
                Some(result)
            } else {
                None
            };
            Ok(ScalarValue::BinaryView(result))
        };

        let calculator = VectorCalculator {
            name: self.name(),
            func: body,
        };
        calculator.invoke_with_vectors(args)
    }
}

impl Display for VectorDivFunction {
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
    fn test_vector_mul() {
        let func = VectorDivFunction;

        let vec0 = vec![1.0, 2.0, 3.0];
        let vec1 = vec![1.0, 1.0];
        let input0 = Arc::new(StringViewArray::from(vec![Some(format!("{vec0:?}"))]));
        let input1 = Arc::new(StringViewArray::from(vec![Some(format!("{vec1:?}"))]));

        let args = ScalarFunctionArgs {
            args: vec![ColumnarValue::Array(input0), ColumnarValue::Array(input1)],
            arg_fields: vec![],
            number_rows: 3,
            return_field: Arc::new(Field::new("x", DataType::BinaryView, false)),
            config_options: Arc::new(ConfigOptions::new()),
        };
        let e = func.invoke_with_args(args).unwrap_err();
        assert_eq!(
            e.to_string(),
            "Execution error: vectors length not match: vec_div"
        );

        let input0 = Arc::new(StringViewArray::from(vec![
            Some("[1.0,2.0,3.0]".to_string()),
            Some("[8.0,10.0,12.0]".to_string()),
            Some("[7.0,8.0,9.0]".to_string()),
            None,
        ]));

        let input1 = Arc::new(StringViewArray::from(vec![
            Some("[1.0,1.0,1.0]".to_string()),
            Some("[2.0,2.0,2.0]".to_string()),
            None,
            Some("[3.0,3.0,3.0]".to_string()),
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
            veclit_to_binlit(&[1.0, 2.0, 3.0]).as_slice()
        );
        assert_eq!(
            result.value(1),
            veclit_to_binlit(&[4.0, 5.0, 6.0]).as_slice()
        );
        assert!(result.is_null(2));
        assert!(result.is_null(3));

        let input0 = Arc::new(StringViewArray::from(vec![Some("[1.0,-2.0]".to_string())]));
        let input1 = Arc::new(StringViewArray::from(vec![Some("[0.0,0.0]".to_string())]));

        let args = ScalarFunctionArgs {
            args: vec![ColumnarValue::Array(input0), ColumnarValue::Array(input1)],
            arg_fields: vec![],
            number_rows: 2,
            return_field: Arc::new(Field::new("x", DataType::BinaryView, false)),
            config_options: Arc::new(ConfigOptions::new()),
        };
        let result = func
            .invoke_with_args(args)
            .and_then(|x| x.to_array(2))
            .unwrap();

        let result = result.as_binary_view();
        assert_eq!(
            result.value(0),
            veclit_to_binlit(&[f64::INFINITY as f32, f64::NEG_INFINITY as f32]).as_slice()
        );
    }
}
