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

use std::time::Duration;

use common_macro::admin_fn;
use common_meta::rpc::procedure::MigrateRegionRequest;
use common_query::error::{InvalidFuncArgsSnafu, MissingProcedureServiceHandlerSnafu, Result};
use datafusion_expr::{Signature, TypeSignature, Volatility};
use datatypes::data_type::DataType;
use datatypes::prelude::ConcreteDataType;
use datatypes::value::{Value, ValueRef};
use session::context::QueryContextRef;

use crate::handlers::ProcedureServiceHandlerRef;
use crate::helper::cast_u64;

/// The default timeout for migrate region procedure.
const DEFAULT_TIMEOUT_SECS: u64 = 300;

/// A function to migrate a region from source peer to target peer.
/// Returns the submitted procedure id if success. Only available in cluster mode.
///
/// - `migrate_region(region_id, from_peer, to_peer)`, with timeout(300 seconds).
/// - `migrate_region(region_id, from_peer, to_peer, timeout(secs))`.
///
/// The parameters:
/// - `region_id`:  the region id
/// - `from_peer`:  the source peer id
/// - `to_peer`:  the target peer id
#[admin_fn(
    name = MigrateRegionFunction,
    display_name = migrate_region,
    sig_fn = signature,
    ret = string
)]
pub(crate) async fn migrate_region(
    procedure_service_handler: &ProcedureServiceHandlerRef,
    _ctx: &QueryContextRef,
    params: &[ValueRef<'_>],
) -> Result<Value> {
    let (region_id, from_peer, to_peer, timeout) = match params.len() {
        3 => {
            let region_id = cast_u64(&params[0])?;
            let from_peer = cast_u64(&params[1])?;
            let to_peer = cast_u64(&params[2])?;

            (region_id, from_peer, to_peer, Some(DEFAULT_TIMEOUT_SECS))
        }

        4 => {
            let region_id = cast_u64(&params[0])?;
            let from_peer = cast_u64(&params[1])?;
            let to_peer = cast_u64(&params[2])?;
            let replay_timeout = cast_u64(&params[3])?;

            (region_id, from_peer, to_peer, replay_timeout)
        }

        size => {
            return InvalidFuncArgsSnafu {
                err_msg: format!(
                    "The length of the args is not correct, expect exactly 3 or 4, have: {}",
                    size
                ),
            }
            .fail();
        }
    };

    match (region_id, from_peer, to_peer, timeout) {
        (Some(region_id), Some(from_peer), Some(to_peer), Some(timeout)) => {
            let pid = procedure_service_handler
                .migrate_region(MigrateRegionRequest {
                    region_id,
                    from_peer,
                    to_peer,
                    timeout: Duration::from_secs(timeout),
                })
                .await?;

            match pid {
                Some(pid) => Ok(Value::from(pid)),
                None => Ok(Value::Null),
            }
        }

        _ => Ok(Value::Null),
    }
}

fn signature() -> Signature {
    Signature::one_of(
        vec![
            // migrate_region(region_id, from_peer, to_peer)
            TypeSignature::Uniform(
                3,
                ConcreteDataType::numerics()
                    .into_iter()
                    .map(|dt| dt.as_arrow_type())
                    .collect(),
            ),
            // migrate_region(region_id, from_peer, to_peer, timeout(secs))
            TypeSignature::Uniform(
                4,
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

    use arrow::array::{StringArray, UInt64Array};
    use arrow::datatypes::{DataType, Field};
    use datafusion_expr::ColumnarValue;

    use super::*;
    use crate::function::FunctionContext;
    use crate::function_factory::ScalarFunctionFactory;

    #[test]
    fn test_migrate_region_misc() {
        let factory: ScalarFunctionFactory = MigrateRegionFunction::factory().into();
        let f = factory.provide(FunctionContext::mock());
        assert_eq!("migrate_region", f.name());
        assert_eq!(DataType::Utf8, f.return_type(&[]).unwrap());
        assert!(matches!(f.signature(),
                         datafusion_expr::Signature {
                             type_signature: datafusion_expr::TypeSignature::OneOf(sigs),
                             volatility: datafusion_expr::Volatility::Immutable,
                             ..
                         } if sigs.len() == 2));
    }

    #[tokio::test]
    async fn test_missing_procedure_service() {
        let factory: ScalarFunctionFactory = MigrateRegionFunction::factory().into();
        let provider = factory.provide(FunctionContext::default());
        let f = provider.as_async().unwrap();

        let func_args = datafusion::logical_expr::ScalarFunctionArgs {
            args: vec![
                ColumnarValue::Array(Arc::new(UInt64Array::from(vec![1]))),
                ColumnarValue::Array(Arc::new(UInt64Array::from(vec![1]))),
                ColumnarValue::Array(Arc::new(UInt64Array::from(vec![1]))),
            ],
            arg_fields: vec![
                Arc::new(Field::new("arg_0", DataType::UInt64, false)),
                Arc::new(Field::new("arg_1", DataType::UInt64, false)),
                Arc::new(Field::new("arg_2", DataType::UInt64, false)),
            ],
            return_field: Arc::new(Field::new("result", DataType::Utf8, true)),
            number_rows: 1,
            config_options: Arc::new(datafusion_common::config::ConfigOptions::default()),
        };
        let result = f.invoke_async_with_args(func_args).await.unwrap_err();
        assert_eq!(
            "Execution error: Handler error: Missing ProcedureServiceHandler, not expected",
            result.to_string()
        );
    }

    #[tokio::test]
    async fn test_migrate_region() {
        let factory: ScalarFunctionFactory = MigrateRegionFunction::factory().into();
        let provider = factory.provide(FunctionContext::mock());
        let f = provider.as_async().unwrap();

        let func_args = datafusion::logical_expr::ScalarFunctionArgs {
            args: vec![
                ColumnarValue::Array(Arc::new(UInt64Array::from(vec![1]))),
                ColumnarValue::Array(Arc::new(UInt64Array::from(vec![1]))),
                ColumnarValue::Array(Arc::new(UInt64Array::from(vec![1]))),
            ],
            arg_fields: vec![
                Arc::new(Field::new("arg_0", DataType::UInt64, false)),
                Arc::new(Field::new("arg_1", DataType::UInt64, false)),
                Arc::new(Field::new("arg_2", DataType::UInt64, false)),
            ],
            return_field: Arc::new(Field::new("result", DataType::Utf8, true)),
            number_rows: 1,
            config_options: Arc::new(datafusion_common::config::ConfigOptions::default()),
        };
        let result = f.invoke_async_with_args(func_args).await.unwrap();

        match result {
            ColumnarValue::Array(array) => {
                let result_array = array.as_any().downcast_ref::<StringArray>().unwrap();
                assert_eq!(result_array.value(0), "test_pid");
            }
            ColumnarValue::Scalar(scalar) => {
                assert_eq!(
                    scalar,
                    datafusion_common::ScalarValue::Utf8(Some("test_pid".to_string()))
                );
            }
        }
    }
}
