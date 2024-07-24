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
    InvalidFuncArgsSnafu, MissingFlowServiceHandlerSnafu, Result, UnsupportedInputDataTypeSnafu,
};
use common_query::prelude::Signature;
use datafusion::logical_expr::Volatility;
use datatypes::value::{Value, ValueRef};
use session::context::QueryContextRef;
use snafu::ensure;
use store_api::storage::ConcreteDataType;

use crate::handlers::FlowServiceHandlerRef;

fn flush_signature() -> Signature {
    Signature::uniform(
        1,
        vec![ConcreteDataType::string_datatype()],
        Volatility::Immutable,
    )
}

#[admin_fn(
    name = FlushFlowFunction,
    display_name = flush_flow,
    sig_fn = flush_signature,
    ret = uint64
)]
pub(crate) async fn flush_flow(
    flow_service_handler: &FlowServiceHandlerRef,
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

    let ValueRef::String(flow_name) = params[0] else {
        return UnsupportedInputDataTypeSnafu {
            function: "flush_flow",
            datatypes: params.iter().map(|v| v.data_type()).collect::<Vec<_>>(),
        }
        .fail();
    };
    let catalog_name = query_ctx.current_catalog();
    let res = flow_service_handler
        .flush(catalog_name, flow_name, query_ctx.clone())
        .await?;
    let affected_rows = res.affected_rows;

    Ok(Value::from(affected_rows))
}
