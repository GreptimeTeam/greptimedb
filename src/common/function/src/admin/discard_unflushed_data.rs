// Copyright 2026 Greptime Team
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

/// Discards all unflushed data from a region.
#[admin_fn(
    name = DiscardUnflushedDataFunction,
    display_name = discard_unflushed_data,
    sig_fn = signature,
    ret = uint64,
    single_row
)]
pub(crate) async fn discard_unflushed_data(
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
            function: "discard_unflushed_data",
            datatypes: params
                .iter()
                .map(|value| value.data_type())
                .collect::<Vec<_>>(),
        }
        .fail();
    };

    let affected_rows = table_mutation_handler
        .discard_unflushed_data(RegionId::from_u64(region_id), query_ctx.clone())
        .await?;

    Ok(Value::from(affected_rows as u64))
}

fn signature() -> Signature {
    Signature::uniform(
        1,
        ConcreteDataType::numerics()
            .into_iter()
            .map(|data_type| data_type.as_arrow_type())
            .collect(),
        Volatility::Immutable,
    )
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use arrow::array::UInt64Array;
    use arrow::datatypes::{DataType, Field};
    use datafusion_expr::{ColumnarValue, TypeSignature};

    use super::*;
    use crate::function::FunctionContext;
    use crate::function_factory::ScalarFunctionFactory;
    use crate::function_registry::{FUNCTION_REGISTRY, get_admin_function};

    #[test]
    fn test_discard_unflushed_data_is_admin_only() {
        assert!(get_admin_function("discard_unflushed_data").is_some());
        assert!(
            FUNCTION_REGISTRY
                .get_function("discard_unflushed_data")
                .is_none()
        );
    }

    #[test]
    fn test_discard_unflushed_data_signature() {
        let factory: ScalarFunctionFactory = DiscardUnflushedDataFunction::factory().into();
        let function = factory.provide(FunctionContext::mock());

        assert_eq!("discard_unflushed_data", function.name());
        assert_eq!(DataType::UInt64, function.return_type(&[]).unwrap());
        assert!(matches!(
            function.signature(),
            Signature {
                type_signature: TypeSignature::Uniform(1, valid_types),
                volatility: Volatility::Immutable,
                ..
            } if valid_types == &ConcreteDataType::numerics()
                .into_iter()
                .map(|data_type| {
                    use datatypes::data_type::DataType;
                    data_type.as_arrow_type()
                })
                .collect::<Vec<_>>()
        ));
    }

    #[tokio::test]
    async fn test_discard_unflushed_data() {
        let factory: ScalarFunctionFactory = DiscardUnflushedDataFunction::factory().into();
        let function = factory.provide(FunctionContext::mock());
        let args = datafusion::logical_expr::ScalarFunctionArgs {
            args: vec![ColumnarValue::Array(Arc::new(UInt64Array::from(vec![99])))],
            arg_fields: vec![Arc::new(Field::new("arg_0", DataType::UInt64, false))],
            return_field: Arc::new(Field::new("result", DataType::UInt64, false)),
            number_rows: 1,
            config_options: Arc::new(datafusion_common::config::ConfigOptions::default()),
        };

        let result = function
            .as_async()
            .unwrap()
            .invoke_async_with_args(args)
            .await
            .unwrap();
        let ColumnarValue::Array(array) = result else {
            panic!("expected array output");
        };
        let array = array.as_any().downcast_ref::<UInt64Array>().unwrap();
        assert_eq!(42, array.value(0));
    }

    #[tokio::test]
    async fn test_discard_unflushed_data_rejects_multiple_rows() {
        let factory: ScalarFunctionFactory = DiscardUnflushedDataFunction::factory().into();
        let function = factory.provide(FunctionContext::mock());
        let args = datafusion::logical_expr::ScalarFunctionArgs {
            args: vec![ColumnarValue::Array(Arc::new(UInt64Array::from(vec![
                1, 2,
            ])))],
            arg_fields: vec![Arc::new(Field::new("arg_0", DataType::UInt64, false))],
            return_field: Arc::new(Field::new("result", DataType::UInt64, false)),
            number_rows: 2,
            config_options: Arc::new(datafusion_common::config::ConfigOptions::default()),
        };

        let error = function
            .as_async()
            .unwrap()
            .invoke_async_with_args(args)
            .await
            .unwrap_err();
        assert!(error.to_string().contains("received 2"));
    }
}
