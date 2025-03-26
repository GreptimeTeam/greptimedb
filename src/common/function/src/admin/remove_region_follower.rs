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
use common_meta::rpc::procedure::RemoveRegionFollowerRequest;
use common_query::error::{InvalidFuncArgsSnafu, MissingProcedureServiceHandlerSnafu, Result};
use common_query::prelude::{Signature, TypeSignature, Volatility};
use datatypes::prelude::ConcreteDataType;
use datatypes::value::{Value, ValueRef};
use session::context::QueryContextRef;

use crate::handlers::ProcedureServiceHandlerRef;
use crate::helper::cast_u64;

#[admin_fn(
    name = RemoveRegionFollowerFunction,
    display_name = remove_region_follower,
    sig_fn = signature,
    ret = string
)]
pub(crate) async fn remove_region_follower(
    procedure_service_handler: &ProcedureServiceHandlerRef,
    _ctx: &QueryContextRef,
    params: &[ValueRef<'_>],
) -> Result<Value> {
    let (region_id, peer_id) = match params.len() {
        2 => {
            let region_id = cast_u64(&params[0])?;
            let peer_id = cast_u64(&params[1])?;

            (region_id, peer_id)
        }

        size => {
            return InvalidFuncArgsSnafu {
                err_msg: format!(
                    "The length of the args is not correct, expect exactly 2, have: {}",
                    size
                ),
            }
            .fail();
        }
    };

    match (region_id, peer_id) {
        (Some(region_id), Some(peer_id)) => {
            procedure_service_handler
                .remove_region_follower(RemoveRegionFollowerRequest { region_id, peer_id })
                .await?;

            Ok(Value::Null)
        }

        _ => Ok(Value::Null),
    }
}

fn signature() -> Signature {
    Signature::one_of(
        vec![
            // add_region_follower(region_id, peer)
            TypeSignature::Uniform(2, ConcreteDataType::numerics()),
        ],
        Volatility::Immutable,
    )
}
