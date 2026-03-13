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

use common_macro::admin_fn;
use common_query::error::{
    InvalidFuncArgsSnafu, MissingTableMutationHandlerSnafu, Result, UnsupportedInputDataTypeSnafu,
};
use datafusion_expr::{Signature, Volatility};
use datatypes::data_type::DataType;
use datatypes::prelude::*;
use session::context::QueryContextRef;
use snafu::ensure;
use store_api::storage::RegionId;

use crate::handlers::TableMutationHandlerRef;
use crate::helper::cast_u64;

macro_rules! define_region_function {
    ($name: expr, $display_name_str: expr, $display_name: ident) => {
        /// A function to $display_name
        #[admin_fn(name = $name, display_name = $display_name_str, sig_fn = signature, ret = uint64)]
        pub(crate) async fn $display_name(
            table_mutation_handler: &TableMutationHandlerRef,
            query_ctx: &QueryContextRef,
            params: &[ValueRef<'_>],
        ) -> Result<Value> {
            ensure!(
                params.len() == 1,
                InvalidFuncArgsSnafu {
                    err_msg: format!(
                        "The length of the args is not correct, expect 1, have: {}",
                        params.len()
                    ),
                }
            );

            let Some(region_id) = cast_u64(&params[0])? else {
                return UnsupportedInputDataTypeSnafu {
                    function: stringify!($display_name_str),
                    datatypes: params.iter().map(|v| v.data_type()).collect::<Vec<_>>(),
                }
                .fail();
            };

            let affected_rows = table_mutation_handler
                .$display_name(RegionId::from_u64(region_id), query_ctx.clone())
                .await?;

            Ok(Value::from(affected_rows as u64))
        }
    };
}

define_region_function!(FlushRegionFunction, flush_region, flush_region);

define_region_function!(CompactRegionFunction, compact_region, compact_region);

fn signature() -> Signature {
    Signature::uniform(
        1,
        ConcreteDataType::numerics()
            .into_iter()
            .map(|dt| dt.as_arrow_type())
            .collect(),
        Volatility::Immutable,
    )
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use arrow::array::UInt64Array;
    use arrow::datatypes::{DataType, Field};
    use datafusion_expr::ColumnarValue;

    use super::*;
    use crate::function::FunctionContext;
    use crate::function_factory::ScalarFunctionFactory;

    macro_rules! define_region_function_test {
        ($name: ident, $func: ident) => {
            paste::paste! {
                #[test]
                fn [<test_ $name _misc>]() {
                    let factory: ScalarFunctionFactory = $func::factory().into();
                    let f = factory.provide(FunctionContext::mock());
                    assert_eq!(stringify!($name), f.name());
                    assert_eq!(
                        DataType::UInt64,
                        f.return_type(&[]).unwrap()
                    );
                    assert!(matches!(f.signature(),
                                     datafusion_expr::Signature {
                                         type_signature: datafusion_expr::TypeSignature::Uniform(1, valid_types),
                                         volatility: datafusion_expr::Volatility::Immutable,
                                         ..
                                     } if valid_types == &ConcreteDataType::numerics().into_iter().map(|dt| { use datatypes::data_type::DataType; dt.as_arrow_type() }).collect::<Vec<_>>()));
                }

                #[tokio::test]
                async fn [<test_ $name _missing_table_mutation>]() {
                    let factory: ScalarFunctionFactory = $func::factory().into();
                    let provider = factory.provide(FunctionContext::default());
                    let f = provider.as_async().unwrap();

                    let func_args = datafusion::logical_expr::ScalarFunctionArgs {
                        args: vec![
                            ColumnarValue::Array(Arc::new(UInt64Array::from(vec![99]))),
                        ],
                        arg_fields: vec![
                            Arc::new(Field::new("arg_0", DataType::UInt64, false)),
                        ],
                        return_field: Arc::new(Field::new("result", DataType::UInt64, true)),
                        number_rows: 1,
                        config_options: Arc::new(datafusion_common::config::ConfigOptions::default()),
                    };
                    let result = f.invoke_async_with_args(func_args).await.unwrap_err();
                    assert_eq!(
                        "Execution error: Handler error: Missing TableMutationHandler, not expected",
                        result.to_string()
                    );
                }

                #[tokio::test]
                async fn [<test_ $name>]() {
                    let factory: ScalarFunctionFactory = $func::factory().into();
                    let provider = factory.provide(FunctionContext::mock());
                    let f = provider.as_async().unwrap();

                    let func_args = datafusion::logical_expr::ScalarFunctionArgs {
                        args: vec![
                            ColumnarValue::Array(Arc::new(UInt64Array::from(vec![99]))),
                        ],
                        arg_fields: vec![
                            Arc::new(Field::new("arg_0", DataType::UInt64, false)),
                        ],
                        return_field: Arc::new(Field::new("result", DataType::UInt64, true)),
                        number_rows: 1,
                        config_options: Arc::new(datafusion_common::config::ConfigOptions::default()),
                    };
                    let result = f.invoke_async_with_args(func_args).await.unwrap();

                    match result {
                        ColumnarValue::Array(array) => {
                            let result_array = array.as_any().downcast_ref::<UInt64Array>().unwrap();
                            assert_eq!(result_array.value(0), 42u64);
                        }
                        ColumnarValue::Scalar(scalar) => {
                            assert_eq!(scalar, datafusion_common::ScalarValue::UInt64(Some(42)));
                        }
                    }
                }
            }
        };
    }

    define_region_function_test!(flush_region, FlushRegionFunction);

    define_region_function_test!(compact_region, CompactRegionFunction);
}
