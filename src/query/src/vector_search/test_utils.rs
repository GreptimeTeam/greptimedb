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

use std::sync::Arc;

use common_function::function::Function;
use common_function::scalars::udf::create_udf;
use datafusion_common::{DataFusionError, Result, ScalarValue};
use datafusion_expr::expr::ScalarFunction;
use datafusion_expr::{Expr, Signature, Volatility, col, lit};

pub(crate) struct TestVectorFunction {
    name: &'static str,
    signature: Signature,
}

impl TestVectorFunction {
    fn new(name: &'static str) -> Self {
        Self {
            name,
            signature: Signature::any(2, Volatility::Immutable),
        }
    }
}

impl std::fmt::Display for TestVectorFunction {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.name)
    }
}

impl Function for TestVectorFunction {
    fn name(&self) -> &str {
        self.name
    }

    fn return_type(
        &self,
        _input_types: &[datatypes::arrow::datatypes::DataType],
    ) -> Result<datatypes::arrow::datatypes::DataType> {
        Ok(datatypes::arrow::datatypes::DataType::Float32)
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn invoke_with_args(
        &self,
        _args: datafusion_expr::ScalarFunctionArgs,
    ) -> Result<datafusion_expr::ColumnarValue> {
        Err(DataFusionError::Execution(
            "test udf should not be invoked".to_string(),
        ))
    }
}

pub(crate) fn vec_distance_expr(function_name: &'static str) -> Expr {
    let udf = create_udf(Arc::new(TestVectorFunction::new(function_name)));
    Expr::ScalarFunction(ScalarFunction::new_udf(
        Arc::new(udf),
        vec![
            col("v"),
            lit(ScalarValue::Utf8(Some("[1.0, 2.0]".to_string()))),
        ],
    ))
}

pub(crate) fn vec_distance_expr_qualified(
    function_name: &'static str,
    table_name: &str,
    column_name: &str,
) -> Expr {
    use datafusion_common::Column;

    let udf = create_udf(Arc::new(TestVectorFunction::new(function_name)));
    let qualified_col = Expr::Column(Column::new(Some(table_name.to_string()), column_name));
    Expr::ScalarFunction(ScalarFunction::new_udf(
        Arc::new(udf),
        vec![
            qualified_col,
            lit(ScalarValue::Utf8(Some("[1.0, 2.0]".to_string()))),
        ],
    ))
}
