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

use api::v1::meta::ProcedureStatus;
use arrow::datatypes::DataType as ArrowDataType;
use common_macro::admin_fn;
use common_meta::rpc::procedure::ProcedureStateResponse;
use common_query::error::{
    InvalidFuncArgsSnafu, MissingProcedureServiceHandlerSnafu, Result,
    UnsupportedInputDataTypeSnafu,
};
use datafusion_expr::{Signature, Volatility};
use datatypes::prelude::*;
use serde::Serialize;
use session::context::QueryContextRef;
use snafu::ensure;

use crate::handlers::ProcedureServiceHandlerRef;

#[derive(Serialize)]
struct ProcedureStateJson {
    status: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    error: Option<String>,
}

/// A function to query procedure state by its id.
/// Such as `procedure_state(pid)`.
#[admin_fn(
    name = ProcedureStateFunction,
    display_name = procedure_state,
    sig_fn = signature,
    ret = string
)]
pub(crate) async fn procedure_state(
    procedure_service_handler: &ProcedureServiceHandlerRef,
    _ctx: &QueryContextRef,
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

    let ValueRef::String(pid) = params[0] else {
        return UnsupportedInputDataTypeSnafu {
            function: "procedure_state",
            datatypes: params.iter().map(|v| v.data_type()).collect::<Vec<_>>(),
        }
        .fail();
    };

    let ProcedureStateResponse { status, error, .. } =
        procedure_service_handler.query_procedure_state(pid).await?;
    let status = ProcedureStatus::try_from(status)
        .map(|v| v.as_str_name())
        .unwrap_or("Unknown");

    let state = ProcedureStateJson {
        status: status.to_string(),
        error: if error.is_empty() { None } else { Some(error) },
    };
    let json = serde_json::to_string(&state).unwrap_or_default();

    Ok(Value::from(json))
}

fn signature() -> Signature {
    Signature::uniform(1, vec![ArrowDataType::Utf8], Volatility::Immutable)
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use arrow::array::StringArray;
    use arrow::datatypes::{DataType, Field};
    use datafusion_expr::ColumnarValue;

    use super::*;
    use crate::function::FunctionContext;
    use crate::function_factory::ScalarFunctionFactory;

    #[test]
    fn test_procedure_state_misc() {
        let factory: ScalarFunctionFactory = ProcedureStateFunction::factory().into();
        let f = factory.provide(FunctionContext::mock());
        assert_eq!("procedure_state", f.name());
        assert_eq!(DataType::Utf8, f.return_type(&[]).unwrap());
        assert!(matches!(f.signature(),
                         datafusion_expr::Signature {
                             type_signature: datafusion_expr::TypeSignature::Uniform(1, valid_types),
                             volatility: datafusion_expr::Volatility::Immutable,
                             ..
                         } if valid_types == &vec![ArrowDataType::Utf8]));
    }

    #[tokio::test]
    async fn test_missing_procedure_service() {
        let factory: ScalarFunctionFactory = ProcedureStateFunction::factory().into();
        let binding = factory.provide(FunctionContext::default());
        let f = binding.as_async().unwrap();

        let func_args = datafusion::logical_expr::ScalarFunctionArgs {
            args: vec![ColumnarValue::Array(Arc::new(StringArray::from(vec![
                "pid",
            ])))],
            arg_fields: vec![Arc::new(Field::new("arg_0", DataType::Utf8, false))],
            return_field: Arc::new(Field::new("result", DataType::Utf8, true)),
            number_rows: 1,
            config_options: Arc::new(datafusion_common::config::ConfigOptions::default()),
        };
        let result = f.invoke_async_with_args(func_args).await;
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn test_procedure_state() {
        let factory: ScalarFunctionFactory = ProcedureStateFunction::factory().into();
        let provider = factory.provide(FunctionContext::mock());
        let f = provider.as_async().unwrap();

        let func_args = datafusion::logical_expr::ScalarFunctionArgs {
            args: vec![ColumnarValue::Array(Arc::new(StringArray::from(vec![
                "pid",
            ])))],
            arg_fields: vec![Arc::new(Field::new("arg_0", DataType::Utf8, false))],
            return_field: Arc::new(Field::new("result", DataType::Utf8, true)),
            number_rows: 1,
            config_options: Arc::new(datafusion_common::config::ConfigOptions::default()),
        };
        let result = f.invoke_async_with_args(func_args).await.unwrap();

        match result {
            ColumnarValue::Array(array) => {
                let result_array = array.as_any().downcast_ref::<StringArray>().unwrap();
                assert_eq!(
                    result_array.value(0),
                    "{\"status\":\"Done\",\"error\":\"OK\"}"
                );
            }
            ColumnarValue::Scalar(scalar) => {
                assert_eq!(
                    scalar,
                    datafusion_common::ScalarValue::Utf8(Some(
                        "{\"status\":\"Done\",\"error\":\"OK\"}".to_string()
                    ))
                );
            }
        }
    }
}
