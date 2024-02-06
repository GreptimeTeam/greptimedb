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

use std::fmt::{self};
use std::time::Duration;

use common_query::error::Error::ThreadJoin;
use common_query::error::{
    InvalidFuncArgsSnafu, InvalidInputTypeSnafu, MissingTableMutationHandlerSnafu, Result,
};
use common_query::prelude::{Signature, TypeSignature, Volatility};
use common_telemetry::logging::error;
use datatypes::prelude::{ConcreteDataType, MutableVector, ScalarVectorBuilder};
use datatypes::value::Value;
use datatypes::vectors::{StringVectorBuilder, VectorRef};
use snafu::{Location, OptionExt, ResultExt};

use crate::function::{Function, FunctionContext};

#[derive(Clone, Debug, Default)]
pub struct MigrateRegionFunction;

const NAME: &str = "migrate_region";
const DEFAULT_REPLAY_TIMEOUT_SECS: u64 = 10;

fn cast_u64_vector(vector: &VectorRef) -> Result<VectorRef> {
    vector
        .cast(&ConcreteDataType::uint64_datatype())
        .context(InvalidInputTypeSnafu {
            err_msg: format!(
                "Failed to cast input into uint64, actual type: {:#?}",
                vector.data_type(),
            ),
        })
}

impl Function for MigrateRegionFunction {
    fn name(&self) -> &str {
        NAME
    }

    fn return_type(&self, _input_types: &[ConcreteDataType]) -> Result<ConcreteDataType> {
        Ok(ConcreteDataType::string_datatype())
    }

    fn signature(&self) -> Signature {
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

    fn eval(&self, func_ctx: FunctionContext, columns: &[VectorRef]) -> Result<VectorRef> {
        let (region_ids, from_peers, to_peers, replay_timeouts) = match columns.len() {
            3 => {
                let region_ids = cast_u64_vector(&columns[0])?;
                let from_peers = cast_u64_vector(&columns[1])?;
                let to_peers = cast_u64_vector(&columns[2])?;

                (region_ids, from_peers, to_peers, None)
            }

            4 => {
                let region_ids = cast_u64_vector(&columns[0])?;
                let from_peers = cast_u64_vector(&columns[1])?;
                let to_peers = cast_u64_vector(&columns[2])?;
                let replay_timeouts = cast_u64_vector(&columns[3])?;

                (region_ids, from_peers, to_peers, Some(replay_timeouts))
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

        std::thread::spawn(move || {
            let len = region_ids.len();
            let mut results = StringVectorBuilder::with_capacity(len);

            for index in 0..len {
                let region_id = region_ids.get(index);
                let from_peer = from_peers.get(index);
                let to_peer = to_peers.get(index);
                let replay_timeout = match &replay_timeouts {
                    Some(replay_timeouts) => replay_timeouts.get(index),
                    None => Value::UInt64(DEFAULT_REPLAY_TIMEOUT_SECS),
                };

                match (region_id, from_peer, to_peer, replay_timeout) {
                    (
                        Value::UInt64(region_id),
                        Value::UInt64(from_peer),
                        Value::UInt64(to_peer),
                        Value::UInt64(replay_timeout),
                    ) => {
                        let func_ctx = func_ctx.clone();

                        let pid = common_runtime::block_on_read(async move {
                            func_ctx
                                .state
                                .table_mutation_handler
                                .as_ref()
                                .context(MissingTableMutationHandlerSnafu)?
                                .migrate_region(
                                    region_id,
                                    from_peer,
                                    to_peer,
                                    Duration::from_secs(replay_timeout),
                                )
                                .await
                        })?;

                        results.push(Some(&pid));
                    }
                    _ => {
                        results.push(None);
                    }
                }
            }

            Ok(results.to_vector())
        })
        .join()
        .map_err(|e| {
            error!(e ;"Join thread error");
            ThreadJoin {
                location: Location::default(),
            }
        })?
    }
}

impl fmt::Display for MigrateRegionFunction {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "MIGRATE_REGION")
    }
}

#[cfg(test)]
mod tests {
    // FIXME(dennis): test in the following PR.
}
