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

use api::v1::meta::ResolveStrategy;
use common_query::error::{
    InvalidFuncArgsSnafu, InvalidInputTypeSnafu, Result, UnsupportedInputDataTypeSnafu,
};
use common_query::prelude::{Signature, TypeSignature, Volatility};
use datatypes::prelude::ConcreteDataType;
use datatypes::types::cast::cast;
use datatypes::value::ValueRef;
use snafu::{OptionExt, ResultExt};

/// Create a function signature with oneof signatures of interleaving two arguments.
pub fn one_of_sigs2(args1: Vec<ConcreteDataType>, args2: Vec<ConcreteDataType>) -> Signature {
    let mut sigs = Vec::with_capacity(args1.len() * args2.len());

    for arg1 in &args1 {
        for arg2 in &args2 {
            sigs.push(TypeSignature::Exact(vec![arg1.clone(), arg2.clone()]));
        }
    }

    Signature::one_of(sigs, Volatility::Immutable)
}

/// Cast a [`ValueRef`] to u64, returns `None` if fails
pub fn cast_u64(value: &ValueRef) -> Result<Option<u64>> {
    cast((*value).into(), &ConcreteDataType::uint64_datatype())
        .context(InvalidInputTypeSnafu {
            err_msg: format!(
                "Failed to cast input into uint64, actual type: {:#?}",
                value.data_type(),
            ),
        })
        .map(|v| v.as_u64())
}

/// Cast a [`ValueRef`] to u32, returns `None` if fails
pub fn cast_u32(value: &ValueRef) -> Result<Option<u32>> {
    cast((*value).into(), &ConcreteDataType::uint32_datatype())
        .context(InvalidInputTypeSnafu {
            err_msg: format!(
                "Failed to cast input into uint32, actual type: {:#?}",
                value.data_type(),
            ),
        })
        .map(|v| v.as_u64().map(|v| v as u32))
}

/// Parse a resolve strategy from a string.
pub fn parse_resolve_strategy(strategy: &str) -> Result<ResolveStrategy> {
    ResolveStrategy::from_str_name(strategy).context(InvalidFuncArgsSnafu {
        err_msg: format!("Invalid resolve strategy: {}", strategy),
    })
}

/// Default parallelism for reconcile operations.
pub fn default_parallelism() -> u32 {
    64
}

/// Default resolve strategy for reconcile operations.
pub fn default_resolve_strategy() -> ResolveStrategy {
    ResolveStrategy::UseLatest
}

/// Get the string value from the params.
///
/// # Errors
/// Returns an error if the input type is not a string.
pub fn get_string_from_params<'a>(
    params: &'a [ValueRef<'a>],
    index: usize,
    fn_name: &'a str,
) -> Result<&'a str> {
    let ValueRef::String(s) = &params[index] else {
        return UnsupportedInputDataTypeSnafu {
            function: fn_name,
            datatypes: params.iter().map(|v| v.data_type()).collect::<Vec<_>>(),
        }
        .fail();
    };
    Ok(s)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_resolve_strategy() {
        assert_eq!(
            parse_resolve_strategy("UseLatest").unwrap(),
            ResolveStrategy::UseLatest
        );
    }
}
