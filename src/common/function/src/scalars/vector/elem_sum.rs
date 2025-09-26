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
use datafusion_expr::type_coercion::aggregates::{BINARYS, STRINGS};
use datafusion_expr::{ScalarFunctionArgs, Signature, TypeSignature, Volatility};
use nalgebra::DVectorView;

use crate::function::Function;
use crate::scalars::vector::{VectorCalculator, impl_conv};

const NAME: &str = "vec_elem_sum";

#[derive(Debug, Clone, Default)]
pub struct ElemSumFunction;

impl Function for ElemSumFunction {
    fn name(&self) -> &str {
        NAME
    }

    fn return_type(&self, _: &[DataType]) -> Result<DataType> {
        Ok(DataType::Float32)
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
            let v0 =
                impl_conv::as_veclit(v0)?.map(|v0| DVectorView::from_slice(&v0, v0.len()).sum());
            Ok(ScalarValue::Float32(v0))
        };

        let calculator = VectorCalculator {
            name: self.name(),
            func: body,
        };
        calculator.invoke_with_single_argument(args)
    }
}

impl Display for ElemSumFunction {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", NAME.to_ascii_uppercase())
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use arrow::array::StringViewArray;
    use arrow_schema::Field;
    use datafusion::arrow::array::{Array, AsArray};
    use datafusion::arrow::datatypes::Float32Type;
    use datafusion_common::config::ConfigOptions;

    use super::*;

    #[test]
    fn test_elem_sum() {
        let func = ElemSumFunction;

        let input = Arc::new(StringViewArray::from(vec![
            Some("[1.0,2.0,3.0]".to_string()),
            Some("[4.0,5.0,6.0]".to_string()),
            None,
        ]));

        let result = func
            .invoke_with_args(ScalarFunctionArgs {
                args: vec![ColumnarValue::Array(input.clone())],
                arg_fields: vec![],
                number_rows: input.len(),
                return_field: Arc::new(Field::new("x", DataType::Float32, true)),
                config_options: Arc::new(ConfigOptions::new()),
            })
            .and_then(|v| ColumnarValue::values_to_arrays(&[v]))
            .map(|mut a| a.remove(0))
            .unwrap();
        let result = result.as_primitive::<Float32Type>();

        assert_eq!(result.len(), 3);
        assert_eq!(result.value(0), 6.0);
        assert_eq!(result.value(1), 15.0);
        assert!(result.is_null(2));
    }
}
