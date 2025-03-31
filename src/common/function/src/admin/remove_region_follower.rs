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
use common_query::error::{
    InvalidFuncArgsSnafu, MissingProcedureServiceHandlerSnafu, Result,
    UnsupportedInputDataTypeSnafu,
};
use common_query::prelude::{Signature, TypeSignature, Volatility};
use datatypes::prelude::ConcreteDataType;
use datatypes::value::{Value, ValueRef};
use session::context::QueryContextRef;
use snafu::ensure;

use crate::handlers::ProcedureServiceHandlerRef;
use crate::helper::cast_u64;

/// A function to remove a follower from a region.
//// Only available in cluster mode.
///
/// - `remove_region_follower(region_id, peer_id)`.
///
/// The parameters:
/// - `region_id`:  the region id
/// - `peer_id`:  the peer id
#[admin_fn(
    name = RemoveRegionFollowerFunction,
    display_name = remove_region_follower,
    sig_fn = signature,
    ret = uint64
)]
pub(crate) async fn remove_region_follower(
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
        .remove_region_follower(RemoveRegionFollowerRequest { region_id, peer_id })
        .await?;

    Ok(Value::from(0u64))
}

fn signature() -> Signature {
    Signature::one_of(
        vec![
            // remove_region_follower(region_id, peer_id)
            TypeSignature::Uniform(2, ConcreteDataType::numerics()),
        ],
        Volatility::Immutable,
    )
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use common_query::prelude::TypeSignature;
    use datatypes::vectors::{UInt64Vector, VectorRef};

    use super::*;
    use crate::function::{AsyncFunction, FunctionContext};

    #[test]
    fn test_remove_region_follower_misc() {
        let f = RemoveRegionFollowerFunction;
        assert_eq!("remove_region_follower", f.name());
        assert_eq!(
            ConcreteDataType::uint64_datatype(),
            f.return_type(&[]).unwrap()
        );
        assert!(matches!(f.signature(),
                         Signature {
                             type_signature: TypeSignature::OneOf(sigs),
                             volatility: Volatility::Immutable
                         } if sigs.len() == 1));
    }

    #[tokio::test]
    async fn test_remove_region_follower() {
        let f = RemoveRegionFollowerFunction;
        let args = vec![1, 1];
        let args = args
            .into_iter()
            .map(|arg| Arc::new(UInt64Vector::from_slice([arg])) as _)
            .collect::<Vec<_>>();

        let result = f.eval(FunctionContext::mock(), &args).await.unwrap();
        let expect: VectorRef = Arc::new(UInt64Vector::from_slice([0u64]));
        assert_eq!(result, expect);
    }
}
