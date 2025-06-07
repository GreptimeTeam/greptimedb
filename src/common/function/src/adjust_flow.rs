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
use crate::helper::parse_catalog_flow;

fn adjust_signature() -> Signature {
    Signature::exact(
        vec![
            ConcreteDataType::string_datatype(), // flow name
            ConcreteDataType::uint64_datatype(), // min_run_interval in seconds
            ConcreteDataType::uint64_datatype(), // max filter number per query
        ],
        Volatility::Immutable,
    )
}

#[admin_fn(
    name = AdjustFlowFunction,
    display_name = adjust_flow,
    sig_fn = adjust_signature,
    ret = uint64
)]
pub(crate) async fn adjust_flow(
    flow_service_handler: &FlowServiceHandlerRef,
    query_ctx: &QueryContextRef,
    params: &[ValueRef<'_>],
) -> Result<Value> {
    ensure!(
        params.len() == 3,
        InvalidFuncArgsSnafu {
            err_msg: format!(
                "The length of the args is not correct, expect 3, have: {}",
                params.len()
            ),
        }
    );

    let (flow_name, min_run_interval, max_filter_num) = match (params[0], params[1], params[2]) {
        (
            ValueRef::String(flow_name),
            ValueRef::UInt64(min_run_interval),
            ValueRef::UInt64(max_filter_num),
        ) => (flow_name, min_run_interval, max_filter_num),
        _ => {
            return UnsupportedInputDataTypeSnafu {
                function: "adjust_flow",
                datatypes: params.iter().map(|v| v.data_type()).collect::<Vec<_>>(),
            }
            .fail();
        }
    };

    let (catalog_name, flow_name) = parse_catalog_flow(flow_name, query_ctx)?;

    let res = flow_service_handler
        .adjust(
            &catalog_name,
            &flow_name,
            min_run_interval,
            max_filter_num as usize,
            query_ctx.clone(),
        )
        .await?;
    let affected_rows = res.affected_rows;

    Ok(Value::from(affected_rows))
}
