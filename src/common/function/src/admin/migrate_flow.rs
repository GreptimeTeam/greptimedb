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
use common_meta::rpc::procedure::MigrateFlowRequest;
use common_query::error::{InvalidFuncArgsSnafu, MissingProcedureServiceHandlerSnafu, Result};
use datafusion_expr::{Signature, TypeSignature, Volatility};
use datatypes::arrow::datatypes::DataType as ArrowDataType;
use datatypes::data_type::DataType;
use datatypes::prelude::ConcreteDataType;
use datatypes::value::{Value, ValueRef};
use session::context::QueryContextRef;
use snafu::ensure;

use crate::handlers::ProcedureServiceHandlerRef;
use crate::helper::cast_u64;

/// The default timeout for migrate flow procedure.
const DEFAULT_TIMEOUT_SECS: u64 = 300;

/// A function to migrate a flow partition from source flownode to target flownode.
/// Returns the submitted procedure id if success.
///
/// - `migrate_flow(catalog, flow_id, partition_id, from_flownode, to_flownode)`,
///   with timeout(300 seconds).
/// - `migrate_flow(catalog, flow_id, partition_id, from_flownode, to_flownode, timeout(secs))`.
///
/// The parameters:
/// - `catalog`: the catalog name of the flow
/// - `flow_id`: the flow id
/// - `partition_id`: the flow partition id
/// - `from_flownode`: the source flownode id
/// - `to_flownode`: the target flownode id
#[admin_fn(
    name = MigrateFlowFunction,
    display_name = migrate_flow,
    sig_fn = signature,
    ret = string
)]
pub(crate) async fn migrate_flow(
    procedure_service_handler: &ProcedureServiceHandlerRef,
    _ctx: &QueryContextRef,
    params: &[ValueRef<'_>],
) -> Result<Value> {
    let Some(request) = parse_migrate_flow_request(params)? else {
        return Ok(Value::Null);
    };

    let pid = procedure_service_handler.migrate_flow(request).await?;

    match pid {
        Some(pid) => Ok(Value::from(pid)),
        None => Ok(Value::Null),
    }
}

fn parse_migrate_flow_request(params: &[ValueRef<'_>]) -> Result<Option<MigrateFlowRequest>> {
    ensure!(
        matches!(params.len(), 5 | 6),
        InvalidFuncArgsSnafu {
            err_msg: format!(
                "The length of the args is not correct, expect exactly 5 or 6, have: {}",
                params.len()
            ),
        }
    );

    let ValueRef::String(catalog) = params[0] else {
        return common_query::error::UnsupportedInputDataTypeSnafu {
            function: "migrate_flow",
            datatypes: params.iter().map(|v| v.data_type()).collect::<Vec<_>>(),
        }
        .fail();
    };

    let flow_id = cast_u32_arg(params, 1, "flow_id")?;
    let partition_id = cast_u32_arg(params, 2, "partition_id")?;
    let from_flownode = cast_u64(&params[3])?;
    let to_flownode = cast_u64(&params[4])?;
    let timeout = match params.len() {
        5 => Some(DEFAULT_TIMEOUT_SECS),
        6 => cast_u64(&params[5])?,
        _ => unreachable!(),
    };

    match (flow_id, partition_id, from_flownode, to_flownode, timeout) {
        (
            Some(flow_id),
            Some(partition_id),
            Some(from_flownode),
            Some(to_flownode),
            Some(timeout),
        ) => Ok(Some(MigrateFlowRequest {
            catalog: catalog.to_string(),
            flow_id,
            partition_id,
            from_flownode,
            to_flownode,
            timeout: Duration::from_secs(timeout),
        })),
        _ => Ok(None),
    }
}

fn cast_u32_arg(params: &[ValueRef<'_>], index: usize, name: &str) -> Result<Option<u32>> {
    let Some(value) = cast_u64(&params[index])? else {
        return Ok(None);
    };

    ensure!(
        u32::try_from(value).is_ok(),
        InvalidFuncArgsSnafu {
            err_msg: format!("The {} is out of range for uint32: {}", name, value),
        }
    );

    Ok(Some(value as u32))
}

fn signature() -> Signature {
    let numerics = ConcreteDataType::numerics()
        .into_iter()
        .map(|dt| dt.as_arrow_type())
        .collect::<Vec<_>>();
    let mut signatures = Vec::with_capacity(numerics.len() * 2);

    for data_type in numerics {
        signatures.push(TypeSignature::Exact(vec![
            ArrowDataType::Utf8,
            data_type.clone(),
            data_type.clone(),
            data_type.clone(),
            data_type.clone(),
        ]));
        signatures.push(TypeSignature::Exact(vec![
            ArrowDataType::Utf8,
            data_type.clone(),
            data_type.clone(),
            data_type.clone(),
            data_type.clone(),
            data_type,
        ]));
    }

    Signature::one_of(signatures, Volatility::Immutable)
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
    fn test_migrate_flow_misc() {
        let factory: ScalarFunctionFactory = MigrateFlowFunction::factory().into();
        let f = factory.provide(FunctionContext::mock());

        assert_eq!("migrate_flow", f.name());
        assert_eq!(DataType::Utf8, f.return_type(&[]).unwrap());
        assert!(matches!(f.signature(),
                         datafusion_expr::Signature {
                             type_signature: datafusion_expr::TypeSignature::OneOf(sigs),
                             volatility: datafusion_expr::Volatility::Immutable,
                             ..
                         } if !sigs.is_empty()));
    }

    #[test]
    fn test_parse_migrate_flow_request() {
        let params = vec![
            ValueRef::String("greptime"),
            ValueRef::UInt32(1024),
            ValueRef::UInt32(0),
            ValueRef::UInt64(1),
            ValueRef::UInt64(2),
        ];

        let request = parse_migrate_flow_request(&params).unwrap().unwrap();

        assert_eq!("greptime", request.catalog);
        assert_eq!(1024, request.flow_id);
        assert_eq!(0, request.partition_id);
        assert_eq!(1, request.from_flownode);
        assert_eq!(2, request.to_flownode);
        assert_eq!(Duration::from_secs(DEFAULT_TIMEOUT_SECS), request.timeout);
    }

    #[test]
    fn test_parse_migrate_flow_request_with_timeout() {
        let params = vec![
            ValueRef::String("greptime"),
            ValueRef::UInt32(1024),
            ValueRef::UInt32(0),
            ValueRef::UInt64(1),
            ValueRef::UInt64(2),
            ValueRef::UInt64(60),
        ];

        let request = parse_migrate_flow_request(&params).unwrap().unwrap();

        assert_eq!(Duration::from_secs(60), request.timeout);
    }

    #[test]
    fn test_parse_migrate_flow_request_with_nullable_arg() {
        let params = vec![
            ValueRef::String("greptime"),
            ValueRef::Null,
            ValueRef::UInt32(0),
            ValueRef::UInt64(1),
            ValueRef::UInt64(2),
        ];

        assert!(parse_migrate_flow_request(&params).unwrap().is_none());
    }

    #[test]
    fn test_parse_migrate_flow_request_rejects_bad_args() {
        let params = vec![ValueRef::String("greptime")];

        let err = parse_migrate_flow_request(&params).unwrap_err();
        assert!(err.to_string().contains("expect exactly 5 or 6"));

        let params = vec![
            ValueRef::String("greptime"),
            ValueRef::UInt64(u64::from(u32::MAX) + 1),
            ValueRef::UInt32(0),
            ValueRef::UInt64(1),
            ValueRef::UInt64(2),
        ];
        let err = parse_migrate_flow_request(&params).unwrap_err();
        assert!(err.to_string().contains("flow_id is out of range"));

        let params = vec![
            ValueRef::UInt64(1),
            ValueRef::UInt32(1024),
            ValueRef::UInt32(0),
            ValueRef::UInt64(1),
            ValueRef::UInt64(2),
        ];
        let err = parse_migrate_flow_request(&params).unwrap_err();
        assert!(err.to_string().contains("Unsupported input datatypes"));
    }

    #[tokio::test]
    async fn test_missing_procedure_service() {
        let factory: ScalarFunctionFactory = MigrateFlowFunction::factory().into();
        let provider = factory.provide(FunctionContext::default());
        let f = provider.as_async().unwrap();

        let result = f
            .invoke_async_with_args(build_func_args())
            .await
            .unwrap_err();

        assert_eq!(
            "Execution error: Missing ProcedureServiceHandler, not expected",
            result.to_string()
        );
    }

    #[tokio::test]
    async fn test_migrate_flow() {
        let factory: ScalarFunctionFactory = MigrateFlowFunction::factory().into();
        let provider = factory.provide(FunctionContext::mock());
        let f = provider.as_async().unwrap();

        let result = f.invoke_async_with_args(build_func_args()).await.unwrap();

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

    fn build_func_args() -> datafusion::logical_expr::ScalarFunctionArgs {
        datafusion::logical_expr::ScalarFunctionArgs {
            args: vec![
                ColumnarValue::Array(Arc::new(StringArray::from(vec!["greptime"]))),
                ColumnarValue::Array(Arc::new(UInt64Array::from(vec![1024]))),
                ColumnarValue::Array(Arc::new(UInt64Array::from(vec![0]))),
                ColumnarValue::Array(Arc::new(UInt64Array::from(vec![1]))),
                ColumnarValue::Array(Arc::new(UInt64Array::from(vec![2]))),
            ],
            arg_fields: vec![
                Arc::new(Field::new("arg_0", DataType::Utf8, false)),
                Arc::new(Field::new("arg_1", DataType::UInt64, false)),
                Arc::new(Field::new("arg_2", DataType::UInt64, false)),
                Arc::new(Field::new("arg_3", DataType::UInt64, false)),
                Arc::new(Field::new("arg_4", DataType::UInt64, false)),
            ],
            return_field: Arc::new(Field::new("result", DataType::Utf8, true)),
            number_rows: 1,
            config_options: Arc::new(datafusion_common::config::ConfigOptions::default()),
        }
    }
}
