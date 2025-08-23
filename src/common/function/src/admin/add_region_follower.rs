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
use common_meta::rpc::procedure::AddRegionFollowerRequest;
use common_query::error::{
    InvalidFuncArgsSnafu, MissingProcedureServiceHandlerSnafu, Result,
    UnsupportedInputDataTypeSnafu,
};
use datafusion_expr::{Signature, TypeSignature, Volatility};
use datatypes::data_type::DataType;
use datatypes::prelude::ConcreteDataType;
use datatypes::value::{Value, ValueRef};
use session::context::QueryContextRef;
use snafu::ensure;

use crate::handlers::ProcedureServiceHandlerRef;
use crate::helper::cast_u64;

/// A function to add a follower to a region.
/// Only available in cluster mode.
///
/// - `add_region_follower(region_id, peer_id)`.
///
/// The parameters:
/// - `region_id`:  the region id
/// - `peer_id`:  the peer id
#[admin_fn(
    name = AddRegionFollowerFunction,
    display_name = add_region_follower,
    sig_fn = signature,
    ret = uint64
)]
pub(crate) async fn add_region_follower(
    procedure_service_handler: &ProcedureServiceHandlerRef,
    _ctx: &QueryContextRef,
    params: &[ValueRef<'_>],
) -> Result<Value> {
    ensure!(
        params.len() == 2,
        InvalidFuncArgsSnafu {
            err_msg: format!(
                "The length of the args is not correct, expect exactly 2, have: {}",
                params.len()
            ),
        }
    );

    let Some(region_id) = cast_u64(&params[0])? else {
        return UnsupportedInputDataTypeSnafu {
            function: "add_region_follower",
            datatypes: params.iter().map(|v| v.data_type()).collect::<Vec<_>>(),
        }
        .fail();
    };
    let Some(peer_id) = cast_u64(&params[1])? else {
        return UnsupportedInputDataTypeSnafu {
            function: "add_region_follower",
            datatypes: params.iter().map(|v| v.data_type()).collect::<Vec<_>>(),
        }
        .fail();
    };

    procedure_service_handler
        .add_region_follower(AddRegionFollowerRequest { region_id, peer_id })
        .await?;

    Ok(Value::from(0u64))
}

fn signature() -> Signature {
    Signature::one_of(
        vec![
            // add_region_follower(region_id, peer)
            TypeSignature::Uniform(
                2,
                ConcreteDataType::numerics()
                    .into_iter()
                    .map(|dt| dt.as_arrow_type())
                    .collect(),
            ),
        ],
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

    #[test]
    fn test_add_region_follower_misc() {
        let factory: ScalarFunctionFactory = AddRegionFollowerFunction::factory().into();
        let f = factory.provide(FunctionContext::mock());
        assert_eq!("add_region_follower", f.name());
        assert_eq!(DataType::UInt64, f.return_type(&[]).unwrap());
        assert!(matches!(f.signature(),
                         datafusion_expr::Signature {
                             type_signature: datafusion_expr::TypeSignature::OneOf(sigs),
                             volatility: datafusion_expr::Volatility::Immutable
                         } if sigs.len() == 1));
    }

    #[tokio::test]
    async fn test_add_region_follower() {
        let factory: ScalarFunctionFactory = AddRegionFollowerFunction::factory().into();
        let provider = factory.provide(FunctionContext::mock());
        let f = provider.as_async().unwrap();

        let func_args = datafusion::logical_expr::ScalarFunctionArgs {
            args: vec![
                ColumnarValue::Array(Arc::new(UInt64Array::from(vec![1]))),
                ColumnarValue::Array(Arc::new(UInt64Array::from(vec![2]))),
            ],
            arg_fields: vec![
                Arc::new(Field::new("arg_0", DataType::UInt64, false)),
                Arc::new(Field::new("arg_1", DataType::UInt64, false)),
            ],
            return_field: Arc::new(Field::new("result", DataType::UInt64, true)),
            number_rows: 1,
            config_options: Arc::new(datafusion_common::config::ConfigOptions::default()),
        };

        let result = f.invoke_async_with_args(func_args).await.unwrap();

        match result {
            ColumnarValue::Array(array) => {
                let result_array = array.as_any().downcast_ref::<UInt64Array>().unwrap();
                assert_eq!(result_array.value(0), 0u64);
            }
            ColumnarValue::Scalar(scalar) => {
                assert_eq!(scalar, datafusion_common::ScalarValue::UInt64(Some(0)));
            }
        }
    }
}
