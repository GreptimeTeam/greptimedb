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
use common_macro::admin_fn;
use common_meta::rpc::procedure::ProcedureStateResponse;
use common_query::error::{
    InvalidFuncArgsSnafu, MissingProcedureServiceHandlerSnafu, Result,
    UnsupportedInputDataTypeSnafu,
};
use common_query::prelude::{Signature, Volatility};
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
    Signature::uniform(
        1,
        vec![ConcreteDataType::string_datatype()],
        Volatility::Immutable,
    )
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use common_query::prelude::TypeSignature;
    use datatypes::vectors::StringVector;

    use super::*;
    use crate::function::{Function, FunctionContext};

    #[test]
    fn test_procedure_state_misc() {
        let f = ProcedureStateFunction;
        assert_eq!("procedure_state", f.name());
        assert_eq!(
            ConcreteDataType::string_datatype(),
            f.return_type(&[]).unwrap()
        );
        assert!(matches!(f.signature(),
                         Signature {
                             type_signature: TypeSignature::Uniform(1, valid_types),
                             volatility: Volatility::Immutable
                         } if valid_types == vec![ConcreteDataType::string_datatype()]
        ));
    }

    #[test]
    fn test_missing_procedure_service() {
        let f = ProcedureStateFunction;

        let args = vec!["pid"];

        let args = args
            .into_iter()
            .map(|arg| Arc::new(StringVector::from_slice(&[arg])) as _)
            .collect::<Vec<_>>();

        let result = f.eval(FunctionContext::default(), &args).unwrap_err();
        assert_eq!(
            "Missing ProcedureServiceHandler, not expected",
            result.to_string()
        );
    }

    #[test]
    fn test_procedure_state() {
        let f = ProcedureStateFunction;

        let args = vec!["pid"];

        let args = args
            .into_iter()
            .map(|arg| Arc::new(StringVector::from_slice(&[arg])) as _)
            .collect::<Vec<_>>();

        let result = f.eval(FunctionContext::mock(), &args).unwrap();

        let expect: VectorRef = Arc::new(StringVector::from(vec![
            "{\"status\":\"Done\",\"error\":\"OK\"}",
        ]));
        assert_eq!(expect, result);
    }
}
