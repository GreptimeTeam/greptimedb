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
use common_query::error::{InvalidFuncArgsSnafu, MissingProcedureServiceHandlerSnafu, Result};
use datafusion_expr::{Signature, Volatility};
use datatypes::data_type::DataType;
use datatypes::prelude::{ConcreteDataType, Value, ValueRef};
use session::context::QueryContextRef;
use snafu::ensure;

use crate::handlers::ProcedureServiceHandlerRef;
use crate::helper::cast_u64;

/// Starts draining a datanode and returns the submitted procedure id.
/// The progress can be queried with `SELECT procedure_state(pid)`.
#[admin_fn(
    name = DrainNodeFunction,
    display_name = drain_node,
    sig_fn = signature,
    ret = string,
    single_row
)]
pub(crate) async fn drain_node(
    procedure_service_handler: &ProcedureServiceHandlerRef,
    query_ctx: &QueryContextRef,
    params: &[ValueRef<'_>],
) -> Result<Value> {
    ensure!(
        params.len() == 1,
        InvalidFuncArgsSnafu {
            err_msg: format!(
                "drain_node expects exactly 1 datanode id, got {}",
                params.len()
            ),
        }
    );
    let Some(datanode_id) = cast_u64(&params[0])? else {
        return Ok(Value::Null);
    };
    let pid = procedure_service_handler
        .drain_node(query_ctx.clone(), datanode_id)
        .await?;
    Ok(Value::from(pid))
}

fn signature() -> Signature {
    Signature::uniform(
        1,
        ConcreteDataType::numerics()
            .into_iter()
            .map(|dt| dt.as_arrow_type())
            .collect(),
        Volatility::Volatile,
    )
}

#[cfg(test)]
mod tests {
    use datatypes::prelude::{Value, ValueRef};

    use super::drain_node;
    use crate::function::FunctionContext;
    use crate::function_registry::{FUNCTION_REGISTRY, get_admin_function};

    #[tokio::test]
    async fn test_drain_node_admin_function() {
        assert!(get_admin_function("drain_node").is_some());
        assert!(FUNCTION_REGISTRY.get_function("drain_node").is_none());

        let context = FunctionContext::mock();
        let handler = context.state.procedure_service_handler.as_ref().unwrap();
        assert_eq!(
            drain_node(handler, &context.query_ctx, &[ValueRef::UInt64(7)])
                .await
                .unwrap(),
            Value::from("test_pid")
        );
        assert_eq!(
            drain_node(handler, &context.query_ctx, &[ValueRef::Null])
                .await
                .unwrap(),
            Value::Null
        );
        assert!(drain_node(handler, &context.query_ctx, &[]).await.is_err());
    }
}
