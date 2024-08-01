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
use common_query::prelude::{Signature, TypeSignature, Volatility};
use datatypes::prelude::ConcreteDataType;
use datatypes::value::{Value, ValueRef};
use session::context::QueryContextRef;

use crate::handlers::ProcedureServiceHandlerRef;
use crate::helper::cast_u64;

const DEFAULT_REPLAY_TIMEOUT_SECS: u64 = 10;

/// A function to migrate a region from source peer to target peer.
/// Returns the submitted procedure id if success. Only available in cluster mode.
///
/// - `migrate_region(region_id, from_peer, to_peer)`, with default replay WAL timeout(10 seconds).
/// - `migrate_region(region_id, from_peer, to_peer, timeout(secs))`
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
    let (region_id, from_peer, to_peer, replay_timeout) = match params.len() {
        3 => {
            let region_id = cast_u64(&params[0])?;
            let from_peer = cast_u64(&params[1])?;
            let to_peer = cast_u64(&params[2])?;

            (
                region_id,
                from_peer,
                to_peer,
                Some(DEFAULT_REPLAY_TIMEOUT_SECS),
            )
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

    match (region_id, from_peer, to_peer, replay_timeout) {
        (Some(region_id), Some(from_peer), Some(to_peer), Some(replay_timeout)) => {
            let pid = procedure_service_handler
                .migrate_region(MigrateRegionRequest {
                    region_id,
                    from_peer,
                    to_peer,
                    replay_timeout: Duration::from_secs(replay_timeout),
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
            TypeSignature::Uniform(3, ConcreteDataType::numerics()),
            // migrate_region(region_id, from_peer, to_peer, timeout(secs))
            TypeSignature::Uniform(4, ConcreteDataType::numerics()),
        ],
        Volatility::Immutable,
    )
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use common_query::prelude::TypeSignature;
    use datatypes::vectors::{StringVector, UInt64Vector, VectorRef};

    use super::*;
    use crate::function::{Function, FunctionContext};

    #[test]
    fn test_migrate_region_misc() {
        let f = MigrateRegionFunction;
        assert_eq!("migrate_region", f.name());
        assert_eq!(
            ConcreteDataType::string_datatype(),
            f.return_type(&[]).unwrap()
        );
        assert!(matches!(f.signature(),
                         Signature {
                             type_signature: TypeSignature::OneOf(sigs),
                             volatility: Volatility::Immutable
                         } if sigs.len() == 2));
    }

    #[test]
    fn test_missing_procedure_service() {
        let f = MigrateRegionFunction;

        let args = vec![1, 1, 1];

        let args = args
            .into_iter()
            .map(|arg| Arc::new(UInt64Vector::from_slice([arg])) as _)
            .collect::<Vec<_>>();

        let result = f.eval(FunctionContext::default(), &args).unwrap_err();
        assert_eq!(
            "Missing ProcedureServiceHandler, not expected",
            result.to_string()
        );
    }

    #[test]
    fn test_migrate_region() {
        let f = MigrateRegionFunction;

        let args = vec![1, 1, 1];

        let args = args
            .into_iter()
            .map(|arg| Arc::new(UInt64Vector::from_slice([arg])) as _)
            .collect::<Vec<_>>();

        let result = f.eval(FunctionContext::mock(), &args).unwrap();

        let expect: VectorRef = Arc::new(StringVector::from(vec!["test_pid"]));
        assert_eq!(expect, result);
    }
}
