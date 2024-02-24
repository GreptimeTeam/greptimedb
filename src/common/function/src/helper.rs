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

use common_query::error::{InvalidInputTypeSnafu, Result};
use common_query::prelude::{Signature, TypeSignature, Volatility};
use datatypes::prelude::ConcreteDataType;
use datatypes::types::cast::cast;
use datatypes::value::ValueRef;
use snafu::ResultExt;

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
