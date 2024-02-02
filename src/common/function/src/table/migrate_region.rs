use std::fmt::{self};
use std::sync::Arc;
use std::time::Duration;

use common_query::error::{
    InvalidFuncArgsSnafu, InvalidInputTypeSnafu, MissingTableMutationHandlerSnafu, Result,
};
use common_query::prelude::{Signature, TypeSignature, Volatility};
use datatypes::prelude::{
    ConcreteDataType, MutableVector, ScalarVector, ScalarVectorBuilder, Vector,
};
use datatypes::value::Value;
use datatypes::vectors::{
    ConstantVector, StringVector, StringVectorBuilder, UInt64Vector, VectorRef,
};
use snafu::{OptionExt, ResultExt};

use crate::function::{Function, FunctionContext};

#[derive(Clone, Debug, Default)]
pub struct MigrateRegionFunction;

const NAME: &str = "migrate_region";

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
                // migrate_region(region_id, from_peer, to_peer, timeout)
                TypeSignature::Uniform(4, ConcreteDataType::numerics()),
            ],
            Volatility::Immutable,
        )
    }

    fn eval(&self, func_ctx: FunctionContext, columns: &[VectorRef]) -> Result<VectorRef> {
        let u64_type = ConcreteDataType::uint64_datatype();

        let (region_id, from_peer, to_peer, replay_timeout, len) = match columns.len() {
            3 => {
                let len = columns[0].len();
                let region_id = columns[0].cast(&u64_type).context(InvalidInputTypeSnafu {
                    err_msg: "Failed to cast input into uint64",
                })?;
                let from_peer = columns[1].cast(&u64_type).context(InvalidInputTypeSnafu {
                    err_msg: "Failed to cast input into uint64",
                })?;
                let to_peer = columns[2].cast(&u64_type).context(InvalidInputTypeSnafu {
                    err_msg: "Failed to cast input into uint64",
                })?;

                (region_id, from_peer, to_peer, None, len)
            }

            4 => {
                let region_id = columns[0].cast(&u64_type).context(InvalidInputTypeSnafu {
                    err_msg: "Failed to cast input into uint64",
                })?;
                let from_peer = columns[1].cast(&u64_type).context(InvalidInputTypeSnafu {
                    err_msg: "Failed to cast input into uint64",
                })?;
                let to_peer = columns[2].cast(&u64_type).context(InvalidInputTypeSnafu {
                    err_msg: "Failed to cast input into uint64",
                })?;
                let replay_timeout = columns[3].cast(&u64_type).context(InvalidInputTypeSnafu {
                    err_msg: "Failed to cast input into uint64",
                })?;

                (
                    region_id,
                    from_peer,
                    to_peer,
                    Some(replay_timeout),
                    columns[0].len(),
                )
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

        let results = std::thread::spawn(move || {
            let mut results = StringVectorBuilder::with_capacity(len);

            for index in 0..len {
                let region_id = region_id.get(index);
                let from_peer = from_peer.get(index);
                let to_peer = to_peer.get(index);
                let replay_timeout = match &replay_timeout {
                    Some(replay_timeout) => replay_timeout.get(index),
                    None => Value::UInt64(10000),
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
        .unwrap();

        results
    }
}

impl fmt::Display for MigrateRegionFunction {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "MIGRATE_REGION")
    }
}
